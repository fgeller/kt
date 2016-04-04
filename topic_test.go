package main

import (
	"os"
	"reflect"
	"testing"
)

func TestTopicParseArgs(t *testing.T) {
	configBefore := config
	defer func() {
		config = configBefore
	}()

	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}

	config.topic.args.brokers = ""
	os.Setenv("KT_BROKERS", givenBroker)

	topicParseArgs()
	if !reflect.DeepEqual(config.topic.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			config.topic.brokers,
		)
		return
	}

	// default brokers to localhost:9092
	os.Setenv("KT_BROKERS", "")
	config.topic.args.brokers = ""
	expectedBrokers = []string{"localhost:9092"}

	topicParseArgs()
	if !reflect.DeepEqual(config.topic.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			config.topic.brokers,
		)
		return
	}

	// command line arg wins
	os.Setenv("KT_BROKERS", "BLABB")
	config.topic.args.brokers = givenBroker
	expectedBrokers = []string{givenBroker}

	topicParseArgs()
	if !reflect.DeepEqual(config.topic.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			config.topic.brokers,
		)
		return
	}
}
