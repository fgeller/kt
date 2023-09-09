package main

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestChooseKafkaVersion(t *testing.T) {
	td := map[string]struct {
		arg      string
		env      string
		err      error
		expected sarama.KafkaVersion
	}{
		"default": {
			expected: sarama.V3_0_0_0,
		},
		"arg v1": {
			arg:      "v1.0.0",
			expected: sarama.V1_0_0_0,
		},
		"env v2": {
			env:      "v2.0.0",
			expected: sarama.V2_0_0_0,
		},
		"arg v1 wins over env v2": {
			arg:      "v1.0.0",
			env:      "v2.0.0",
			expected: sarama.V1_0_0_0,
		},
		"invalid": {
			arg: "234",
			err: fmt.Errorf("invalid version `234`"),
		},
	}

	for tn, tc := range td {
		actual, err := chooseKafkaVersion(tc.arg, tc.env)
		if tc.err == nil {
			require.Equal(t, tc.expected, actual, tn)
			require.NoError(t, err)
		} else {
			require.Equal(t, tc.err, err)
		}
	}
}
