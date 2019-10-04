package main

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
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
	}{{
		key:        "",
		partitions: 0,
		expected:   -1,
	}, {
		key:        "",
		partitions: 1,
		expected:   0,
	}, {
		key:        "super-duper-key",
		partitions: 1,
		expected:   0,
	}, {
		key:        "",
		partitions: 1,
		expected:   0,
	}, {
		key:        "",
		partitions: 2,
		expected:   0,
	}, {
		key:        "a",
		partitions: 2,
		expected:   1,
	}, {
		key:        "b",
		partitions: 2,
		expected:   0,
	}, {
		key:        "random",
		partitions: 2,
		expected:   1,
	}, {
		key:        "random",
		partitions: 5,
		expected:   0,
	}}
	c := qt.New(t)
	for _, d := range data {
		testName := fmt.Sprintf("%q-%d", d.key, d.partitions)
		c.Run(testName, func(c *qt.C) {
			c.Assert(hashCodePartition(d.key, d.partitions), qt.Equals, d.expected)
		})
	}
}

func TestProduceParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_TOPIC", "test-topic")
	c.Setenv("KT_BROKERS", "hans:2000")

	var target produceCmd
	target.parseArgs(nil)
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
}

// brokers default to localhost:9092
func TestProduceParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_TOPIC", "")
	c.Setenv("KT_BROKERS", "")

	var target produceCmd
	target.parseArgs([]string{"-topic", "test-topic"})
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"localhost:9092"})
}

func TestProduceParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	// command line arg wins
	c.Setenv("KT_TOPIC", "BLUBB")
	c.Setenv("KT_BROKERS", "BLABB")

	var target produceCmd
	target.parseArgs([]string{"-topic", "test-topic", "-brokers", "hans:2000"})
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
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
	c := qt.New(t)
	mustDecoderForType := func(typ string) func(string) ([]byte, error) {
		dec, err := decoderForType(typ)
		c.Assert(err, qt.Equals, nil)
		return dec
	}
	stringDecoder := mustDecoderForType("string")
	hexDecoder := mustDecoderForType("hex")
	base64Decoder := mustDecoderForType("base64")

	target := &produceCmd{
		decodeKey:   stringDecoder,
		decodeValue: stringDecoder,
	}
	key, value := "key", "value"
	msg := message{Key: &key, Value: &value}
	actual, err := target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(actual.Key), qt.Equals, key)
	c.Assert(string(actual.Value), qt.Equals, value)

	target.decodeKey, target.decodeValue = hexDecoder, hexDecoder
	key, value = "41", "42"
	msg = message{Key: &key, Value: &value}
	actual, err = target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(actual.Key), qt.Equals, "A")
	c.Assert(string(actual.Value), qt.Equals, "B")

	target.decodeKey, target.decodeValue = base64Decoder, base64Decoder
	key, value = "aGFucw==", "cGV0ZXI="
	msg = message{Key: &key, Value: &value}
	actual, err = target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(actual.Key), qt.Equals, "hans")
	c.Assert(string(actual.Value), qt.Equals, "peter")
}

func TestDeserializeLines(t *testing.T) {
	data := []struct {
		in             string
		literal        bool
		partition      int32
		partitionCount int32
		expected       message
	}{{
		in:             "",
		literal:        false,
		partitionCount: 1,
		expected:       newMessage("", "", 0),
	}, {
		in:             `{"key":"hans","value":"123"}`,
		literal:        false,
		partitionCount: 4,
		expected:       newMessage("hans", "123", hashCodePartition("hans", 4)),
	}, {
		in:             `{"key":"hans","value":"123","partition":1}`,
		literal:        false,
		partitionCount: 3,
		expected:       newMessage("hans", "123", 1),
	}, {
		in:             `{"other":"json","values":"avail"}`,
		literal:        true,
		partition:      2,
		partitionCount: 4,
		expected:       newMessage("", `{"other":"json","values":"avail"}`, 2),
	}, {
		in:             `so lange schon`,
		literal:        false,
		partitionCount: 3,
		expected:       newMessage("", "so lange schon", 0),
	}}

	c := qt.New(t)
	for i, d := range data {
		c.Run(fmt.Sprint(i), func(c *qt.C) {
			target := &produceCmd{
				partitioner: "hashCode",
				literal:     d.literal,
				partition:   d.partition,
			}
			in := make(chan string, 1)
			out := make(chan message)
			go target.deserializeLines(in, out, d.partitionCount)
			in <- d.in

			select {
			case <-time.After(time.Second):
				t.Errorf("did not receive output in time")
			case actual := <-out:
				c.Check(actual, deepEquals, d.expected)
			}
		})
	}
}
