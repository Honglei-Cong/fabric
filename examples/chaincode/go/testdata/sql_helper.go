package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
	"math"
	"strconv"
)

func isNumber(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func isColumnsDefined(table string, cols []string) bool {
	tableFound := true
	for _, t := range tables {
		tableName := getTypeName(t.Obj)
		if tableName != table {
			continue
		}

		elms := getTypeFields(t.Obj)
		for _, f := range cols {
			if f != "*" && !elms[f] && !isNumber(f) {
				return false
			}
		}
	}
	return tableFound
}

func sqlQuery(stub shim.ChaincodeStubInterface, args []string) pb.Response {

	sql := &shim.SqlQuery{}
	if err := json.Unmarshal([]byte(args[0]), sql); err != nil {
		return shim.Error(fmt.Sprintf("unmarshal sql failed: %s", err))
	}

	if strings.ToUpper(sql.Op) != "SELECT" {
		return shim.Error(fmt.Sprintf("Invalid sql op: %s", sql.Op))
	}

	selectQuery := &shim.SelectQuery{}
	if err := json.Unmarshal(sql.OpArg, selectQuery); err != nil {
		return shim.Error(fmt.Sprintf("unmarshal select query failed: %s", err))
	}

	for _, table := range selectQuery.Tables {
		if !isColumnsDefined(table, selectQuery.Fields) {
			return shim.Error("Some columns are not defined in table " + table)
		}
	}

	queryCols := make([]string, 0)
	queryRows := make([]shim.QueryRow, 0)

	for _, t := range selectQuery.Tables {
		cols, rows, err := sqlQueryTable(stub, t, selectQuery)
		if err != nil {
			return shim.Error(fmt.Sprintf("select query table %s failed: %s", t, err))
		}
		if len(queryCols) == 0 {
			queryCols = append(queryCols, cols...)
		}

		queryRows = append(queryRows, rows...)
	}

	queryResult := &shim.QueryResult{
		QueryOP: sql.Op,
		Cols:    queryCols,
		Rows:    queryRows,
	}
	res, err := json.Marshal(queryResult)
	if err != nil {
		return shim.Error(fmt.Sprintf("json marshal result failed: %s", err))
	}

	return shim.Success(res)
}

func sqlQueryTable(stub shim.ChaincodeStubInterface, table string, query *shim.SelectQuery) ([]string, []shim.QueryRow, error) {

	allColsIncluded := false
	for _, col := range query.Fields {
		if col == "*" || isNumber(col) {
			allColsIncluded = true
		}
	}

	cols := make([]string, 0)
	if allColsIncluded {
		for _, t := range tables {
			if table == getTypeName(t.Obj) {
				elms := getTypeFields(t.Obj)
				for e := range elms {
					cols = append(cols, e)
				}
			}
		}
	} else {
		for _, e := range query.Fields {
			cols = append(cols, e)
		}
	}

	if query.StartID < 0 {
		query.StartID = 0
	}
	startKey, err := stub.CreateCompositeKey(table, []string{id2String(uint64(query.StartID))})
	if err != nil {
		return cols, nil, fmt.Errorf("create start Key failed: %s", err)
	}
	endKey, err := stub.CreateCompositeKey(table, []string{id2String(math.MaxUint64)})
	if err != nil {
		return cols, nil, fmt.Errorf("create end Key failed: %s", err)
	}
	if query.EndID >= 0 {
		endKey, _ = stub.CreateCompositeKey(table, []string{id2String(uint64(query.EndID))})
		if err != nil {
			return cols, nil, fmt.Errorf("create end Key failed: %s", err)
		}
	}

	ite, err := stub.GetStateByRange(startKey, endKey)
	if err != nil {
		return cols, nil, fmt.Errorf("create ite failed: %s", err)
	}
	defer ite.Close()

	rows := make([]shim.QueryRow, 0)
	for {
		kv, _ := ite.Next()

		fmt.Printf(">> %v \n", kv)
		if kv == nil {
			break
		}

		m := make(map[string]interface{})
		err = json.Unmarshal(kv.Value, &m)
		if err != nil {
			return cols, nil, fmt.Errorf("Failed to unmarshal value %v, err: %s\n", string(kv.Value), err)
		}

		row := make(shim.QueryRow)
		for _, col := range cols {
			v, avail := m[col]
			if avail {
				switch v := v.(type) {
				case string:
					row[col] = v
				case int64, uint64, int, uint:
					row[col] = fmt.Sprintf("%d", v)
				case float64:
					row[col] = fmt.Sprintf("%d", uint64(v))
				default:
					row[col] = fmt.Sprintf("unsupported type: %s", reflect.TypeOf(v).Name())
				}
			} else {
				row[col] = "null"
			}
		}

		rows = append(rows, row)
	}

	return cols, rows, nil
}
