package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
)

type testClient struct{}

func (c *testClient) Close() error                                         { return nil }
func (c *testClient) Closed() bool                                         { return false }
func (c *testClient) Config() *sarama.Config                               { return nil }
func (c *testClient) Coordinator(t string) (*sarama.Broker, error)         { return nil, nil }
func (c *testClient) GetOffset(t string, p int32, po int64) (int64, error) { return 0, nil }
func (c *testClient) Leader(s string, i int32) (*sarama.Broker, error)     { return nil, nil }
func (c *testClient) Partitions(s string) ([]int32, error)                 { return []int32{}, nil }
func (c *testClient) RefreshCoordinator(s string) error                    { return nil }
func (c *testClient) RefreshMetadata(s ...string) error                    { return nil }
func (c *testClient) Replicas(s string, i int32) ([]int32, error)          { return []int32{}, nil }
func (c *testClient) Topics() ([]string, error)                            { return []string{}, nil }
func (c *testClient) WritablePartitions(s string) ([]int32, error)         { return []int32{}, nil }

func TestOffsetParseArgs(t *testing.T) {
	expectedTopic := "test-topic"
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}

	os.Setenv("KT_TOPIC", expectedTopic)
	os.Setenv("KT_BROKERS", givenBroker)

	target := &offsetCmd{}
	target.parseArgs([]string{})
	if !target.topic.MatchString(expectedTopic) || !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic.String(),
			target.brokers,
		)
		return
	}

	// default brokers to localhost:9092
	os.Setenv("KT_TOPIC", "")
	os.Setenv("KT_BROKERS", "")
	expectedBrokers = []string{"localhost:9092"}

	target.parseArgs([]string{"-topic", expectedTopic})
	if !target.topic.MatchString(expectedTopic) || !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %s and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic,
			target.brokers,
		)
		return
	}

	// command line arg wins
	os.Setenv("KT_TOPIC", "BLUBB")
	os.Setenv("KT_BROKERS", "BLABB")
	expectedBrokers = []string{givenBroker}

	target.parseArgs([]string{"-topic", expectedTopic, "-brokers", givenBroker})
	if !target.topic.MatchString(expectedTopic) ||
		!reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %s and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic,
			target.brokers,
		)
		return
	}

	target.parseArgs([]string{"-setConsumerOffsets", "oldest"})
	if target.newOffsets != sarama.OffsetOldest {
		t.Errorf("Expected setConsumerOffset %v, got %v.", sarama.OffsetOldest, target.newOffsets)
	}

	target.parseArgs([]string{"-setConsumerOffsets", "newest"})
	if target.newOffsets != sarama.OffsetNewest {
		t.Errorf("Expected setConsumerOffset %v, got %v.", sarama.OffsetNewest, target.newOffsets)
	}

	target.parseArgs([]string{"-setConsumerOffsets", "42"})
	if target.newOffsets != 42 {
		t.Errorf("Expected setConsumerOffset %v, got %v.", 42, target.newOffsets)
	}
}
