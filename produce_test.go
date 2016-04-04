package main

import (
	"os"
	"reflect"
	"testing"
)

func TestProduceParseArgs(t *testing.T) {
	configBefore := config
	defer func() {
		config = configBefore
	}()

	expectedTopic := "test-topic"
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}

	config.produce.args.topic = ""
	config.produce.args.brokers = ""
	os.Setenv("KT_TOPIC", expectedTopic)
	os.Setenv("KT_BROKERS", givenBroker)

	produceParseArgs()
	if config.produce.topic != expectedTopic ||
		!reflect.DeepEqual(config.produce.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.produce.topic,
			config.produce.brokers,
		)
		return
	}

	// default brokers to localhost:9092
	os.Setenv("KT_TOPIC", "")
	os.Setenv("KT_BROKERS", "")
	config.produce.args.topic = expectedTopic
	config.produce.args.brokers = ""
	expectedBrokers = []string{"localhost:9092"}

	produceParseArgs()
	if config.produce.topic != expectedTopic ||
		!reflect.DeepEqual(config.produce.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.produce.topic,
			config.produce.brokers,
		)
		return
	}

	// command line arg wins
	os.Setenv("KT_TOPIC", "BLUBB")
	os.Setenv("KT_BROKERS", "BLABB")
	config.produce.args.topic = expectedTopic
	config.produce.args.brokers = givenBroker
	expectedBrokers = []string{givenBroker}

	produceParseArgs()
	if config.produce.topic != expectedTopic ||
		!reflect.DeepEqual(config.produce.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.produce.topic,
			config.produce.brokers,
		)
		return
	}
}
