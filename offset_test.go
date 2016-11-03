package main

import (
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestOffsetsPartition(t *testing.T) {
	configBefore := config
	defer func() {
		config = configBefore
	}()

	tGetPartitions := func(t string) ([]int32, error) {
		return []int32{0, 1, 2, 3}, nil
	}
	tGetOffset := func(topic string, partitionID int32, time int64) (int64, error) {
		if partitionID == 0 {
			return 1245, nil
		}
		if partitionID == 1 {
			return 523, nil
		}
		if partitionID == 2 {
			return 34, nil
		}
		if partitionID == 3 {
			return 879, nil
		}
		return 0, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)

	out := make(chan string)
	go func(out chan string) {
		config.offset.args.topic = "test-topic"
		config.offset.partition = -1
		offsetsForTopic(nil, tGetPartitions, tGetOffset, nil, "", "test-topic", out)
		wg.Done()
	}(out)

	// verify channel
	go func() {
		expected := []string{
			"{\"topic\":\"test-topic\",\"partition\":0,\"partition-offset\":1245}",
			"{\"topic\":\"test-topic\",\"partition\":1,\"partition-offset\":523}",
			"{\"topic\":\"test-topic\",\"partition\":2,\"partition-offset\":34}",
			"{\"topic\":\"test-topic\",\"partition\":3,\"partition-offset\":879}",
		}
		i := 0
		for {
			select {
			case m := <-out:
				if m != expected[i] {
					t.Errorf(
						"Expected offset message [%v], got [%v].",
						expected[i],
						m,
					)
				}
				i = i + 1
			}
		}
	}()
	wg.Wait()
}

func TestOffsetParseArgs(t *testing.T) {
	configBefore := config

	resetConfig := func() {
		config = configBefore
	}
	defer resetConfig()

	expectedTopic := "test-topic"
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}

	config.offset.args.topic = ""
	config.offset.args.brokers = ""
	os.Setenv("KT_TOPIC", expectedTopic)
	os.Setenv("KT_BROKERS", givenBroker)

	offsetParseArgs()
	if !config.offset.topic.MatchString(expectedTopic) ||
		!reflect.DeepEqual(config.offset.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.offset.topic.String(),
			config.offset.brokers,
		)
		return
	}

	// default brokers to localhost:9092
	os.Setenv("KT_TOPIC", "")
	os.Setenv("KT_BROKERS", "")
	config.offset.args.topic = expectedTopic
	config.offset.args.brokers = ""
	expectedBrokers = []string{"localhost:9092"}

	offsetParseArgs()
	if !config.offset.topic.MatchString(expectedTopic) ||
		!reflect.DeepEqual(config.offset.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.offset.topic.String(),
			config.offset.brokers,
		)
		return
	}

	// command line arg wins
	os.Setenv("KT_TOPIC", "BLUBB")
	os.Setenv("KT_BROKERS", "BLABB")
	config.offset.args.topic = expectedTopic
	config.offset.args.brokers = givenBroker
	expectedBrokers = []string{givenBroker}

	offsetParseArgs()
	if !config.offset.topic.MatchString(expectedTopic) ||
		!reflect.DeepEqual(config.offset.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			config.offset.topic.String(),
			config.offset.brokers,
		)
		return
	}

	// set offsets to oldest
	resetConfig()
	config.offset.args.set = "oldest"
	offsetParseArgs()
	if !config.offset.setOldest ||
		config.offset.setNewest ||
		config.offset.set != 0 {
		t.Errorf(
			"Expected setOldest %v (got %v), setNewest %v (got %v), and set %v (got %v).",
			true,
			config.offset.setOldest,
			false,
			config.offset.setNewest,
			0,
			config.offset.set,
		)
	}

	// set offsets to newest
	resetConfig()
	config.offset.args.set = "newest"
	offsetParseArgs()
	if !config.offset.setNewest ||
		config.offset.setOldest ||
		config.offset.set != 0 {
		t.Errorf(
			"Expected setOldest %v (got %v), setNewest %v (got %v), and set %v (got %v).",
			false,
			config.offset.setOldest,
			true,
			config.offset.setNewest,
			0,
			config.offset.set,
		)
	}

	// set offsets to absolute value
	resetConfig()
	config.offset.args.set = "42"
	offsetParseArgs()
	if config.offset.setNewest ||
		config.offset.setOldest ||
		config.offset.set != 42 {
		t.Errorf(
			"Expected setOldest %v (got %v), setNewest %v (got %v), and set %v (got %v).",
			false,
			config.offset.setOldest,
			false,
			config.offset.setNewest,
			42,
			config.offset.set,
		)
	}

}
