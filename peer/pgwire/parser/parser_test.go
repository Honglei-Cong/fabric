package parser

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
)

func Test_ParseSelect(t *testing.T) {
	selStmt := "select a from b"
	s, err := Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select * from b"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select aa,bb,c from x,y"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select a from xx where id > 10"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select * from xx, yy where id > 10"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select a from xx where id > 10 and id < 100"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = `SELECT "id","name","address","tel" FROM Person`
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select a from xx where 1=1"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)

	selStmt = "select a from xx where 2 = 3"
	s, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")
	fmt.Printf("%v \n", s)
}
