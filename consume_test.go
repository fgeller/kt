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
		{
			input:       "-1-",
			expected:    map[int32]interval{-1: {sarama.OffsetNewest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:-1-",
			expected:    map[int32]interval{0: {sarama.OffsetNewest, 0}},
			expectedErr: nil,
		},
		{
			input:       "0:-1-1000",
			expected:    map[int32]interval{0: {sarama.OffsetNewest, 1000}},
			expectedErr: nil,
		},
		{
			input:       "-1:-1-",
			expected:    map[int32]interval{-1: {sarama.OffsetNewest, 0}},
			expectedErr: nil,
		},
		{
			input:       "-1:-1-1",
			expected:    map[int32]interval{-1: {sarama.OffsetNewest, 1}},
			expectedErr: nil,
		},
		{
			input:       "-1:-1-100",
			expected:    map[int32]interval{-1: {sarama.OffsetNewest, 100}},
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

func TestFindPartitionsToConsume(t *testing.T) {
	data := []struct {
		config   consumeConfig
		consumer tConsumer
		expected []int32
	}{
		{
			config: consumeConfig{
				topic:   "a",
				offsets: map[int32]interval{10: {2, 4}},
			},
			consumer: tConsumer{
				topics:              []string{"a"},
				topicsErr:           nil,
				partitions:          map[string][]int32{"a": []int32{0, 10}},
				partitionsErr:       map[string]error{"a": nil},
				consumePartition:    map[tConsumePartition]tPartitionConsumer{},
				consumePartitionErr: map[tConsumePartition]error{},
				closeErr:            nil,
			},
			expected: []int32{10},
		},
	}

	for _, d := range data {
		actual := findPartitions(d.consumer, d.config)

		if !reflect.DeepEqual(actual, d.expected) {
			t.Errorf(
				`
Expected: %+v
Actual:   %+v
Input:    config=%+v
	`,
				d.expected,
				actual,
				d.config,
			)
			return
		}
	}
}

type tConsumePartition struct {
	topic     string
	partition int32
	offset    int64
}

type tConsumerMessage struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

type tConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

type tPartitionConsumer struct {
	closeErr            error
	highWaterMarkOffset int64
	messages            <-chan *sarama.ConsumerMessage
	errors              <-chan *sarama.ConsumerError
}

func (pc tPartitionConsumer) AsyncClose() {}
func (pc tPartitionConsumer) Close() error {
	return pc.closeErr
}
func (pc tPartitionConsumer) HighWaterMarkOffset() int64 {
	return pc.highWaterMarkOffset
}
func (pc tPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}
func (pc tPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

type tConsumer struct {
	topics              []string
	topicsErr           error
	partitions          map[string][]int32
	partitionsErr       map[string]error
	consumePartition    map[tConsumePartition]tPartitionConsumer
	consumePartitionErr map[tConsumePartition]error
	closeErr            error
}

func (c tConsumer) Topics() ([]string, error) {
	return c.topics, c.topicsErr
}

func (c tConsumer) Partitions(topic string) ([]int32, error) {
	return c.partitions[topic], c.partitionsErr[topic]
}

func (c tConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cp := tConsumePartition{topic, partition, offset}
	return c.consumePartition[cp], c.consumePartitionErr[cp]
}

func (c tConsumer) Close() error {
	return c.closeErr
}
