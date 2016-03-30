package main

import (
	"reflect"
	"testing"
)

func TestParseOffsets(t *testing.T) {

	data := []struct {
		input       string
		expected    map[int32]interval
		expectedErr error
	}{
		{
			input:       "0",
			expected:    map[int32]interval{0: {0, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:",
			expected:    map[int32]interval{0: {0, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:1",
			expected:    map[int32]interval{0: {1, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:1-",
			expected:    map[int32]interval{0: {1, 0}},
			expectedErr: nil,
		},
		{
			input:       "0,2,6",
			expected:    map[int32]interval{0: {0, 0}, 2: {0, 0}, 6: {0, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:4-,2:1-10,6",
			expected:    map[int32]interval{0: {4, 0}, 2: {1, 10}, 6: {0, 0}},
			expectedErr: nil,
		},
	}

	for _, d := range data {
		actual, err := parseOffsets(d.input)
		if err != d.expectedErr || !reflect.DeepEqual(actual, d.expected) {
			t.Errorf(
				`
Expected: %+v, err=%v
Actual:   %+v, err=%v
Input:    %v
`,
				d.expected,
				d.expectedErr,
				actual,
				err,
				d.input,
			)
		}
	}

}
