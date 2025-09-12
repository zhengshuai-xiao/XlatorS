package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMethodName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Standard function", "github.com/zhengshuai-xiao/XlatorS/xlator/dedup.(*XlatorDedup).GetObject", "GetObject"},
		{"Method with pointer receiver", "github.com/zhengshuai-xiao/XlatorS/xlator/dedup.(*XlatorDedup).readDataObject", "readDataObject"},
		{"Anonymous function", "github.com/zhengshuai-xiao/XlatorS/xlator/dedup.(*XlatorDedup).GetObjectNInfo.func1", "GetObjectNInfo"},
		{"Simple function", "main.main", "main"},
		{"No package path", "MyFunction", "MyFunction"},
		{"Empty string", "", ""},
		{"Just a dot", ".", "."},
		{"Trailing dot", "some.package.", "package"},
		{"Leading dot", ".some.package", "package"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MethodName(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
