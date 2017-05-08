/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"unicode/utf8"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"bytes"
	"encoding/binary"
)

type typeAttributeInfo struct {
	Key bool `json:"key"`
}

type typeInfoStruct struct {
	Keys    map[string]typeAttributeInfo `json:"keys"`
	Name    string                       `json:"name"`
	NextSeq uint64                       `json:"next_seq"`
}

type KVObject interface {
	GetID() uint64
	SetID(id uint64)
}

func id2String(id uint64) (string, error) {
	idBuf := new(bytes.Buffer)
	if err := binary.Write(idBuf, binary.BigEndian, id); err != nil {
		return "", err
	}
	return string(idBuf.Bytes()), nil
}

var typeKeyPrefix = byte(0x10)
var errNotFound = errors.New("Not Found.")

func getTypeName(obj interface{}) string {
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Ptr {
		return typ.Elem().Name()
	} else if typ.Kind() == reflect.Struct {
		return typ.Name()
	}
	return ""
}

func getTypeKey(typename string) string {
	return string(typeKeyPrefix) + typename
}
func getObjectSubkey(stub shim.ChaincodeStubInterface, typename string, subkeys []string) (string, error) {
	return stub.CreateCompositeKey(getTypeKey(typename), subkeys)
}

func getTypeInfo(stub shim.ChaincodeStubInterface, typename string) (*typeInfoStruct, error) {
	v, err := stub.GetState(getTypeKey(typename))
	if err != nil {
		return nil, err
	}
	typeInfo := &typeInfoStruct{
		Keys: make(map[string]typeAttributeInfo),
	}
	err = json.Unmarshal(v, &typeInfo)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse json %s", string(v))
	}
	if typeInfo.Name != typename {
		return nil, errors.New("invalid type name")
	}

	return typeInfo, nil
}

func putTypeInfo(stub shim.ChaincodeStubInterface, typename string, typeInfo *typeInfoStruct) error {
	v, err := json.Marshal(typeInfo)
	if err != nil {
		return err
	}
	return stub.PutState(getTypeKey(typename), v)
}

func getObjectKVs(stub shim.ChaincodeStubInterface, obj KVObject) (map[string][]byte, error) {
	kvs := make(map[string][]byte)
	typename := getTypeName(obj)
	typeInfo, err := getTypeInfo(stub, typename)
	if err != nil {
		return kvs, fmt.Errorf("获取类型 %s 信息失败: %s", typename, err)
	}

	objID := obj.GetID()
	if objID == 0 {
		objID = typeInfo.NextSeq
		obj.SetID(objID)
	}

	objBytes, err := json.Marshal(obj)
	if err != nil {
		return kvs, fmt.Errorf("解析成json失败: %s", err)
	}

	jsonObj := make(map[string]interface{})
	if err := json.Unmarshal(objBytes, &jsonObj); err != nil {
		return kvs, err
	}

	objKeyID, err := id2String(objID)
	if err != nil {
		return kvs, fmt.Errorf("Failed to convert ID: %s", err)
	}

	primKey, err := stub.CreateCompositeKey(typename, []string{objKeyID})
	if err != nil {
		return kvs, err
	}
	kvs[primKey] = objBytes

	for key, keyInfo := range typeInfo.Keys {
		if keyInfo.Key {
			attr := fmt.Sprintf("%v", jsonObj[key])
			subKey, _ := getObjectSubkey(stub, typename, []string{key, attr, objKeyID})
			kvs[subKey] = []byte{0x00}
		}
	}

	return kvs, nil
}

func incrObjectTypeNextSeq(stub shim.ChaincodeStubInterface, typename string) error {
	typeInfo, err := getTypeInfo(stub, typename)
	if err != nil {
		return err
	}

	typeInfo.NextSeq += 1
	return putTypeInfo(stub, typename, typeInfo)
}

func CreateObjectType(stub shim.ChaincodeStubInterface, obj KVObject, keys []string) error {
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	} else if typ.Kind() != reflect.Struct {
		return errors.New("对象类型不正确.")
	}

	typename := typ.Name()
	typeInfo := &typeInfoStruct{
		Keys:    make(map[string]typeAttributeInfo),
		Name:    typename,
		NextSeq: 1,
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Tag.Get("json")
		typeInfo.Keys[field] = typeAttributeInfo{Key: false}
	}

	// check all keys are in type attributes
	for _, key := range keys {
		if _, ok := typeInfo.Keys[key]; !ok {
			return errors.New("Invalid Key: " + key)
		}
		attrInfo := typeInfo.Keys[key]
		attrInfo.Key = true
		typeInfo.Keys[key] = attrInfo
	}

	return putTypeInfo(stub, typename, typeInfo)
}

func GetObject(stub shim.ChaincodeStubInterface, obj KVObject, keys map[string]string) error {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return errors.New("对象类型不正确. 需要ptr类型.")
	}

	if len(keys) == 0 {
		if obj.GetID() == 0 {
			return errors.New("查询不正确.  没有提供键.")
		}
		var k string
		var err error
		var valueBytes []byte
		idKey, err := id2String(obj.GetID())
		if err != nil {
			return err
		}
		if k, err = stub.CreateCompositeKey(getTypeName(obj), []string{idKey}); err != nil {
			return err
		}
		if valueBytes, err = stub.GetState(k); err != nil {
			return err
		}
		return json.Unmarshal(valueBytes, obj)
	}

	ite, err := QueryObjects(stub, obj, keys)
	if err != nil {
		return err
	}

	defer ite.Close()
	if ite.HasNext() {
		_, err := ite.Next()
		if err != nil {
			return err
		}
		return nil
	}
	return errNotFound
}

func GetObjectsIte(stub shim.ChaincodeStubInterface, obj KVObject, keys map[string]string) (shim.StateQueryIteratorInterface, error) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return nil, errors.New("对象类型不正确. 需要ptr类型.")
	}

	if len(keys) == 0 {
		k, err := stub.CreateCompositeKey(getTypeName(obj), []string{})
		if err != nil {
			return nil, errors.New("查询不正确.  创建组合键失败.")
		}
		return stub.GetStateByRange(k, k+string(utf8.MaxRune))
	}

	iter, err := QueryObjects(stub, obj, keys)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func GetObjectsIte_v2(stub shim.ChaincodeStubInterface, obj KVObject, keys map[string]string) (shim.StateQueryIteratorInterface, error) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return nil, errors.New("对象类型不正确. 需要ptr类型.")
	}

	typename := getTypeName(obj)

	ite := &ObjectIterator{
		typename:  typename,
		stub:      stub,
		iters:     make([]shim.StateQueryIteratorInterface, 0),
		currObjId: "\x00",
		obj:       obj,
	}

	if len(keys) == 0 { // get all objects of a type
		typename := getTypeName(obj)
		typeInfo, err := getTypeInfo(stub, typename)
		if err != nil {
			return nil, err
		}
		// primKey, err := stub.CreateCompositeKey(typename, []string{objID})
		subIte, err := stub.GetStateByRange(typename, typename+string(typeInfo.NextSeq))
		if err != nil {
			return nil, err
		}
		ite.iters = append(ite.iters, subIte)
	} else {
		ite, err := QueryObjects(stub, obj, keys)
		if err != nil {
			return nil, err
		}
		return ite, nil
	}

	return ite, nil
}

func PutObject(stub shim.ChaincodeStubInterface, obj KVObject) error {
	return putObjectInternal(stub, obj)
}

func putObjectInternal(stub shim.ChaincodeStubInterface, obj KVObject) error {
	typename := getTypeName(obj)
	kvs, err := getObjectKVs(stub, obj)
	if err != nil {
		return fmt.Errorf("获取ObjectKVs失败: %s", err)
	}

	for k, v := range kvs {
		err := stub.PutState(k, v)
		if err != nil {
			return fmt.Errorf("putState操作失败: %s", err)
		}
	}
	if err := incrObjectTypeNextSeq(stub, typename); err != nil {
		return fmt.Errorf("更新表 %s 流水号失败: %s", typename, err)
	}
	return nil
}

func UpdateObject(stub shim.ChaincodeStubInterface, obj KVObject) error {
	typename := getTypeName(obj)
	var k string
	var oldValueBytes, newValueBytes []byte
	var err error

	idKey, err := id2String(obj.GetID())
	if err != nil {
		return err
	}
	if k, err = stub.CreateCompositeKey(typename, []string{idKey}); err != nil {
		return err
	}
	if oldValueBytes, err = stub.GetState(k); err != nil {
		return err
	}
	if newValueBytes, err = json.Marshal(obj); err != nil {
		return err
	}

	// delete old obj
	if err = json.Unmarshal(oldValueBytes, obj); err != nil {
		return err
	}
	if err = DelObject(stub, obj); err != nil {
		return err
	}

	// put new obj
	if err = json.Unmarshal(newValueBytes, obj); err != nil {
		return err
	}
	if err := putObjectInternal(stub, obj); err != nil {
		return err
	}

	return nil
}

func DelObject(stub shim.ChaincodeStubInterface, obj KVObject) error {
	kvs, err := getObjectKVs(stub, obj)
	if err != nil {
		return err
	}

	for k := range kvs {
		err := stub.DelState(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func QueryObjects(stub shim.ChaincodeStubInterface, obj KVObject, keys map[string]string) (shim.StateQueryIteratorInterface, error) {
	typename := getTypeName(obj)
	typeInfo, err := getTypeInfo(stub, typename)
	if err != nil {
		return nil, err
	}

	ite := &ObjectIterator{
		typename:  typename,
		stub:      stub,
		iters:     make([]shim.StateQueryIteratorInterface, 0),
		currObjId: "\x00",
		obj:       obj,
	}
	for key, val := range keys {
		if keyInfo, ok := typeInfo.Keys[key]; ok && keyInfo.Key {
			subKey, _ := getObjectSubkey(stub, typename, []string{key, val})
			subIte, err := stub.GetStateByRange(subKey, subKey+string(utf8.MaxRune))
			if err != nil {
				return nil, err
			}
			ite.iters = append(ite.iters, subIte)
		}
	}

	return ite, nil
}

type ObjectIterator struct {
	typename  string
	stub      shim.ChaincodeStubInterface
	iters     []shim.StateQueryIteratorInterface
	currObjId string
	obj       interface{}
}

func (ite *ObjectIterator) HasNext() bool {
	currObjId := "\x00"
	iteMatched := make([]bool, len(ite.iters))

	for idx := 0; idx < len(ite.iters); idx++ {
		for !iteMatched[idx] {
			if !ite.iters[idx].HasNext() {
				return false
			}
			kv, err := ite.iters[idx].Next()
			if err != nil {
				return false
			}

			_, keys, err := ite.stub.SplitCompositeKey(kv.Key)
			objID := keys[2]
			if objID > currObjId {
				currObjId = objID
				if idx != 0 {
					// loop back
					iteMatched = make([]bool, len(ite.iters))
					iteMatched[idx] = true
					idx = 0
				} else {
					iteMatched[idx] = true
				}
			} else if objID == currObjId {
				iteMatched[idx] = true
			}
		}
	}

	if currObjId == "\x00" {
		return false
	}

	ite.currObjId = currObjId
	return true
}

func (ite *ObjectIterator) Next() (*queryresult.KV, error) {
	k, err := ite.stub.CreateCompositeKey(ite.typename, []string{ite.currObjId})
	if err != nil {
		return nil, err
	}
	v, err := ite.stub.GetState(k)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(v, ite.obj); err != nil {
		return nil, err
	}
	return &queryresult.KV{
		Key: ite.currObjId,
		Value: v,
	}, err
}

func (ite *ObjectIterator) Close() error {
	for _, it := range ite.iters {
		if err := it.Close(); err != nil {
			return err
		}
	}
	return nil
}