package main

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

var (
	v820  = sarama.V0_8_2_0
	v821  = sarama.V0_8_2_1
	v822  = sarama.V0_8_2_2
	v900  = sarama.V0_9_0_0
	v901  = sarama.V0_9_0_1
	v1000 = sarama.V0_10_0_0
)

func kafkaVersion(s string) sarama.KafkaVersion {
	switch s {
	case "v0.8.2.0":
		return sarama.V0_8_2_0
	case "v0.8.2.1":
		return sarama.V0_8_2_1
	case "v0.8.2.2":
		return sarama.V0_8_2_2
	case "v0.9.0.0":
		return sarama.V0_9_0_0
	case "v0.9.0.1":
		return sarama.V0_9_0_1
	default:
		return sarama.V0_10_0_0
	}
}

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
