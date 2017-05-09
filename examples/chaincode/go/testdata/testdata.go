package main

import (
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

type TestDataCC struct {
}

type Person struct {
	ID      uint64 `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
	Tel     string `json:"tel"`
}

func (p *Person) GetID() uint64   { return p.ID }
func (p *Person) SetID(id uint64) { p.ID = id }

var tables = []*Table{
	{Obj: &Person{}, Keys: []string{"name"}},
}

func (t *TestDataCC) Init(stub shim.ChaincodeStubInterface) pb.Response {

	if err := InitTables(stub, tables); err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(nil)
}

func (t *TestDataCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("ex02 Invoke")
	function, args := stub.GetFunctionAndParameters()

	if function == "Create" {
		return t.create(stub, args)
	} else if function == "sqlQuery" {
		return sqlQuery(stub, args)
	}

	return shim.Error(fmt.Sprintf("invalid function name: %s", function))
}

func (t *TestDataCC) create(stub shim.ChaincodeStubInterface, args []string) pb.Response {

	if len(args) < 1 {
		return shim.Error("invalid args: no counter")
	}

	n, err := strconv.Atoi(args[0])
	if err != nil {
		return shim.Error(fmt.Sprintf("invalid counter arg: %s", err))
	}

	idx := GetObjectCount(stub, &Person{}) + 1

	for i := 0; i < n; i++ {
		p := &Person{
			ID:      idx + uint64(i),
			Name:    fmt.Sprintf("name-%d", idx+uint64(i)),
			Address: fmt.Sprintf("address-%d", idx+uint64(i)),
			Tel:     fmt.Sprintf("tel-%d", idx+uint64(i)),
		}
		if err := PutObject(stub, p); err != nil {
			return shim.Error(fmt.Sprintf("put new object failed: %s", err))
		}
	}

	return shim.Success(nil)
}


func main() {
	err := shim.Start(new(TestDataCC))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}
