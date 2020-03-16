package main

import (
	"os"
	"reflect"
	"testing"
)

func TestTopicParseArgs(t *testing.T) {
	target := &topicCmd{}
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}
	os.Setenv(ENV_BROKERS, givenBroker)

	target.parseArgs([]string{})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}

	os.Setenv(ENV_BROKERS, "")
	expectedBrokers = []string{"localhost:9092"}

	target.parseArgs([]string{})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}

	os.Setenv(ENV_BROKERS, "BLABB")
	expectedBrokers = []string{givenBroker}

	target.parseArgs([]string{"-brokers", givenBroker})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}
}
