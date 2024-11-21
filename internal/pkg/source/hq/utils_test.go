package hq

import (
	"testing"
)

func TestPathToHop(t *testing.T) {
	tests := []struct {
		path     string
		expected int
	}{
		{"", 0},
		{"L", 1},
		{"LL", 2},
		{"LRL", 2},
		{"LLLL", 4},
		{"RLRLRL", 3},
	}

	for _, test := range tests {
		result := pathToHops(test.path)
		if result != test.expected {
			t.Errorf("For path %q, expected %d hops, but got %d", test.path, test.expected, result)
		}
	}
}
