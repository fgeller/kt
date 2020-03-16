package main

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestHashCode(t *testing.T) {

	data := []struct {
		in       string
		expected int32
	}{
		{
			in:       "",
			expected: 0,
		},
		{
			in:       "a",
			expected: 97,
		},
		{
			in:       "b",
			expected: 98,
		},
		{
			in:       "⌘",
			expected: 8984,
		},
		{
			in:       "😼", //non-bmp character, 4bytes in utf16
			expected: 1772959,
		},
		{
			in:       "hashCode",
			expected: 147696667,
		},
		{
			in:       "c03a3475-3ed6-4ed1-8ae5-1c432da43e73",
			expected: 1116730239,
		},
		{
			in:       "random",
			expected: -938285885,
		},
	}

	for _, d := range data {
		actual := hashCode(d.in)
		if actual != d.expected {
			t.Errorf("expected %v but found %v\n", d.expected, actual)
		}
	}
}

func TestHashCodePartition(t *testing.T) {

	data := []struct {
		key        string
		partitions int32
		expected   int32
	}{
		{
			key:        "",
			partitions: 0,
			expected:   -1,
		},
		{
			key:        "",
			partitions: 1,
			expected:   0,
		},
		{
			key:        "super-duper-key",
			partitions: 1,
			expected:   0,
		},
		{
			key:        "",
			partitions: 1,
			expected:   0,
		},
		{
			key:        "",
			partitions: 2,
			expected:   0,
		},
		{
			key:        "a",
			partitions: 2,
			expected:   1,
		},
		{
			key:        "b",
			partitions: 2,
			expected:   0,
		},
		{
			key:        "random",
			partitions: 2,
			expected:   1,
		},
		{
			key:        "random",
			partitions: 5,
			expected:   0,
		},
	}

	for _, d := range data {
		actual := hashCodePartition(d.key, d.partitions)
		if actual != d.expected {
			t.Errorf("expected %v but found %v for key %#v and %v partitions\n", d.expected, actual, d.key, d.partitions)
		}
	}
}

func TestProduceParseArgs(t *testing.T) {
	expectedTopic := "test-topic"
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}
	target := &produceCmd{}

	os.Setenv(ENV_TOPIC, expectedTopic)
	os.Setenv(ENV_BROKERS, givenBroker)

	target.parseArgs([]string{})
	if target.topic != expectedTopic ||
		!reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic,
			target.brokers,
		)
		return
	}

	// default brokers to localhost:9092
	os.Setenv(ENV_TOPIC, "")
	os.Setenv(ENV_BROKERS, "")
	expectedBrokers = []string{"localhost:9092"}

	target.parseArgs([]string{"-topic", expectedTopic})
	if target.topic != expectedTopic ||
		!reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic,
			target.brokers,
		)
		return
	}

	// command line arg wins
	os.Setenv(ENV_TOPIC, "BLUBB")
	os.Setenv(ENV_BROKERS, "BLABB")
	expectedBrokers = []string{givenBroker}

	target.parseArgs([]string{"-topic", expectedTopic, "-brokers", givenBroker})
	if target.topic != expectedTopic ||
		!reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected topic %v and brokers %v from env vars, got topic %v and brokers %v.",
			expectedTopic,
			expectedBrokers,
			target.topic,
			target.brokers,
		)
		return
	}
}

func newMessage(key, value string, partition int32) message {
	var k *string
	if key != "" {
		k = &key
	}

	var v *string
	if value != "" {
		v = &value
	}

	return message{
		Key:       k,
		Value:     v,
		Partition: &partition,
	}
}

func TestMakeSaramaMessage(t *testing.T) {
	target := &produceCmd{decodeKey: "string", decodeValue: "string"}
	key, value := "key", "value"
	msg := message{Key: &key, Value: &value}
	actual, err := target.makeSaramaMessage(msg)
	require.Nil(t, err)
	require.Equal(t, []byte(key), actual.Key)
	require.Equal(t, []byte(value), actual.Value)

	target.decodeKey, target.decodeValue = "hex", "hex"
	key, value = "41", "42"
	msg = message{Key: &key, Value: &value}
	actual, err = target.makeSaramaMessage(msg)
	require.Nil(t, err)
	require.Equal(t, []byte("A"), actual.Key)
	require.Equal(t, []byte("B"), actual.Value)

	target.decodeKey, target.decodeValue = "base64", "base64"
	key, value = "aGFucw==", "cGV0ZXI="
	msg = message{Key: &key, Value: &value}
	actual, err = target.makeSaramaMessage(msg)
	require.Nil(t, err)
	require.Equal(t, []byte("hans"), actual.Key)
	require.Equal(t, []byte("peter"), actual.Value)
}

func TestDeserializeLines(t *testing.T) {
	target := &produceCmd{}
	target.partitioner = "hashCode"
	data := []struct {
		in             string
		literal        bool
		partition      int32
		partitionCount int32
		expected       message
	}{
		{
			in:             "",
			literal:        false,
			partitionCount: 1,
			expected:       newMessage("", "", 0),
		},
		{
			in:             `{"key":"hans","value":"123"}`,
			literal:        false,
			partitionCount: 4,
			expected:       newMessage("hans", "123", hashCodePartition("hans", 4)),
		},
		{
			in:             `{"key":"hans","value":"123","partition":1}`,
			literal:        false,
			partitionCount: 3,
			expected:       newMessage("hans", "123", 1),
		},
		{
			in:             `{"other":"json","values":"avail"}`,
			literal:        true,
			partition:      2,
			partitionCount: 4,
			expected:       newMessage("", `{"other":"json","values":"avail"}`, 2),
		},
		{
			in:             `so lange schon`,
			literal:        false,
			partitionCount: 3,
			expected:       newMessage("", "so lange schon", 0),
		},
	}

	for _, d := range data {
		in := make(chan string, 1)
		out := make(chan message)
		target.literal = d.literal
		target.partition = d.partition
		go target.deserializeLines(in, out, d.partitionCount)
		in <- d.in

		select {
		case <-time.After(50 * time.Millisecond):
			t.Errorf("did not receive output in time")
		case actual := <-out:
			if !(reflect.DeepEqual(d.expected, actual)) {
				t.Errorf(spew.Sprintf("\nexpected %#v\nactual   %#v", d.expected, actual))
			}
		}
	}
}
