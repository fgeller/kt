package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

type testClient struct {
	topics     []string
	partitions []int32
}

func (c *testClient) Close() error                                     { return nil }
func (c *testClient) Closed() bool                                     { return false }
func (c *testClient) Config() *sarama.Config                           { return nil }
func (c *testClient) Coordinator(t string) (*sarama.Broker, error)     { return nil, nil }
func (c *testClient) Leader(s string, i int32) (*sarama.Broker, error) { return nil, nil }
func (c *testClient) Partitions(s string) ([]int32, error)             { return c.partitions, nil }
func (c *testClient) RefreshCoordinator(s string) error                { return nil }
func (c *testClient) RefreshMetadata(s ...string) error                { return nil }
func (c *testClient) Replicas(s string, i int32) ([]int32, error)      { return []int32{}, nil }
func (c *testClient) Topics() ([]string, error)                        { return c.topics, nil }
func (c *testClient) WritablePartitions(s string) ([]int32, error)     { return []int32{}, nil }

func (c *testClient) GetOffset(t string, p int32, o int64) (int64, error) {
	switch p {
	case 2:
		return 4, nil
	case 23:
		return 46, nil
	default:
		return 0, fmt.Errorf("unsupported partition %v", p)
	}
}

func TestOffsetHappyPath(t *testing.T) {
	out := make(chan printContext)
	buf := make(chan printContext, 1000)
	go func() {
		for {
			o := <-out
			buf <- o
			close(o.done)
		}
	}()
	q := make(chan struct{})
	top := `test-topic`
	client := &testClient{
		topics:     []string{top},
		partitions: []int32{2, 23},
	}
	target := &offsetCmd{
		topic:     regexp.MustCompile(top),
		partition: -1,
	}
	target.out = out
	target.client = client
	target.do(q)

	require.Len(t, buf, 2)
	for _, data := range []struct {
		partition int32
		offset    int64
	}{{2, 4}, {23, 46}} {
		actual := <-buf
		expected, _ := json.Marshal(offsets{Topic: top, PartitionOffset: data.offset, Partition: data.partition})
		require.JSONEq(t, string(expected), actual.line)
	}
}

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
