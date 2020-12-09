package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomBool(t *testing.T) {
	atomBool := new(TAtomBool)
	atomBool.Set(true)

	assert.True(t, true, atomBool.Get())
}
