package parser

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func Test_ParseSelect(t *testing.T) {
	selStmt := "select a from b"
	_, err := Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

	selStmt = "select * from b"
	_, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

	selStmt = "select aa,bb,c from x,y"
	_, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

	selStmt = "select a from xx where id > 10"
	_, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

	selStmt = "select * from xx, yy where id > 10"
	_, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

	selStmt = "select a from xx where id > 10 and id < 100"
	_, err = Parse(selStmt)
	assert.NoError(t, err, "select parse failed")

}
