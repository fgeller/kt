package main

import (
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
)

func TestParseOffsets(t *testing.T) {

	data := []struct {
		input       string
		expected    map[int32]interval
		expectedErr error
	}{
		{
			input:       "",
			expected:    map[int32]interval{-1: {sarama.OffsetOldest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0",
			expected:    map[int32]interval{0: {sarama.OffsetOldest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:",
			expected:    map[int32]interval{0: {sarama.OffsetOldest, 0}},
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
			expected:    map[int32]interval{0: {sarama.OffsetOldest, 0}, 2: {sarama.OffsetOldest, 0}, 6: {sarama.OffsetOldest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:4-,2:1-10,6",
			expected:    map[int32]interval{0: {4, 0}, 2: {1, 10}, 6: {sarama.OffsetOldest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:-1",
			expected:    map[int32]interval{0: {sarama.OffsetOldest, 1}},
			expectedErr: nil,
		},
		{
			input:       "-1",
			expected:    map[int32]interval{-1: {sarama.OffsetOldest, 1}},
			expectedErr: nil,
		},
		{
			input:       "0:-3,-1",
			expected:    map[int32]interval{0: {sarama.OffsetOldest, 3}, -1: {sarama.OffsetOldest, 1}},
			expectedErr: nil,
		},
		{
			input:       "1:-4,-1:2-3",
			expected:    map[int32]interval{1: {sarama.OffsetOldest, 4}, -1: {2, 3}},
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
