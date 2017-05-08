package main

import (
	"fmt"
	"strconv"

	"time"

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

func (t *TestDataCC) Init(stub shim.ChaincodeStubInterface) pb.Response {

	if err := CreateObjectType(stub, &Person{}, []string{"name"}); err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(nil)
}

func (t *TestDataCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("ex02 Invoke")
	_, args := stub.GetFunctionAndParameters()

	if len(args) < 1 {
		return shim.Error("invalid args: no counter")
	}

	n, err := strconv.Atoi(args[0])
	if err != nil {
		return shim.Error(fmt.Sprintf("invalid counter arg: %s", err))
	}

	s := time.Now().UnixNano()

	for i := 0; i < n; i++ {
		p := &Person{
			Name:    fmt.Sprintf("name-%d", s+int64(i)),
			Address: fmt.Sprintf("address-%d", s+int64(i)),
			Tel:     fmt.Sprintf("tel-%d", s+int64(i)),
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
