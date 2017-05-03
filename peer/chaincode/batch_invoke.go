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

package chaincode

import (
	"fmt"

	"encoding/json"
	"math/rand"
	"strconv"
	"strings"
	"time"

	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/cobra"
)

var chaincodeBatchInvokeCmd *cobra.Command

type MarbleCtor struct {
	Args     []string `json:"args"`
}

func getRandomMarbleName(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func getRandomInitMarbleCtor() []byte {
	initMarbleCtor := &MarbleCtor{
		Args:     []string{"initMarble", getRandomMarbleName(16), "blue", "100", "bob"},
	}
	bytes, _ := json.Marshal(initMarbleCtor)
	return bytes
}

type batchRequest struct {
	Spec     *pb.ChaincodeSpec
	ChainID  string
	invoke   bool
	reqTimes chan<- requestTime
}

type requestTime struct {
	startTime int64
	endTime   int64
}

// invokeCmd returns the cobra command for Chaincode Invoke
func batchInvokeCmd(cf *ChaincodeCmdFactory) *cobra.Command {
	chaincodeBatchInvokeCmd = &cobra.Command{
		Use:       "batchInvoke",
		Short:     fmt.Sprintf("Invoke the specified %s.", chainFuncName),
		Long:      fmt.Sprintf(`Invoke the specified %s. It will try to commit the endorsed transaction to the network.`, chainFuncName),
		ValidArgs: []string{"1"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return chaincodeBatchInvoke(cmd, args, cf)
		},
	}

	return chaincodeBatchInvokeCmd
}

func chaincodeBatchInvoke(cmd *cobra.Command, args []string, cf *ChaincodeCmdFactory) error {
	if err := checkChaincodeCmdParams(cmd); err != nil {
		return err
	}
	reqCount, err := strconv.Atoi(batchSize)
	if err != nil {
		return err
	}
	workers, err := strconv.Atoi(workerCount)
	if err != nil {
		return err
	}

	cfs, err := initCmdFactories(workers)
	if err != nil {
		return err
	}
	defer closeCmdFactories(cfs)
	rand.Seed(time.Now().UnixNano())

	requests := make(chan batchRequest, workers*4)
	reqTimes := make(chan requestTime, 1000)
	done := make(chan struct{}, workers)

	go buildInvokeRequests(requests, reqCount, reqTimes)
	batchStartTime := time.Now().UnixNano()
	for i := 0; i < workers; i++ {
		go doChaincodeInvoke(done, cfs[i], requests)
	}
	go awaitCompletion(done, workers, reqTimes)

	var count, sumDuration int64
	for reqTime := range reqTimes {
		count++
		sumDuration += reqTime.endTime - reqTime.startTime
	}
	batchEndTime := time.Now().UnixNano()

	logger.Warning("req count:", count, "batch Time(seconds):", time.Duration(batchEndTime-batchStartTime).String())
	logger.Warning("workers:", workers, "Seq Time(seconds)", time.Duration(sumDuration).String())

	return nil
}

func initCmdFactories(n int) ([]*ChaincodeCmdFactory, error) {
	cfs := make([]*ChaincodeCmdFactory, n)
	var err error
	for i := 0; i < n; i++ {
		if cfs[i], err = InitCmdFactory(true, true); err != nil {
			closeCmdFactories(cfs)
			return cfs, err
		}
	}

	return cfs, nil
}

func closeCmdFactories(cfs []*ChaincodeCmdFactory) {
	for i, cf := range cfs {
		if cf != nil {
			cf.BroadcastClient.Close()
			cfs[i] = nil
		}
	}
}

func buildInvokeRequests(requests chan<- batchRequest, reqCount int, reqTimes chan<- requestTime) {
	for i := 0; i < reqCount; i++ {
		spec := &pb.ChaincodeSpec{}
		// Build the spec
		input := &pb.ChaincodeInput{}
		if err := json.Unmarshal(getRandomInitMarbleCtor(), &input); err != nil {
			logger.Warning("Chaincode argument error: %s \n", err.Error())
			return
		}

		chaincodeLang = strings.ToUpper(chaincodeLang)
		spec = &pb.ChaincodeSpec{
			Type:        pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value[chaincodeLang]),
			ChaincodeId: &pb.ChaincodeID{Path: chaincodePath, Name: chaincodeName, Version: chaincodeVersion},
			Input:       input,
		}

		requests <- batchRequest{spec, chainID, true, reqTimes}
	}
	close(requests)
}

func doChaincodeInvoke(done chan<- struct{}, cf *ChaincodeCmdFactory, requests <-chan batchRequest) {
	for req := range requests {
		startTime := time.Now().UnixNano()
		ChaincodeInvokeOrQuery(req.Spec, req.ChainID, req.invoke, cf.Signer, cf.EndorserClient, cf.BroadcastClient)
		endTime := time.Now().UnixNano()
		req.reqTimes <- requestTime{startTime, endTime}
	}
	done <- struct{}{}
}

func awaitCompletion(done <-chan struct{}, n int, reqTimes chan requestTime) {
	for i := 0; i < n; i++ {
		<-done
	}
	close(reqTimes)
}
