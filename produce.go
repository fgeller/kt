package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type produceCmd struct {
	commonFlags
	topic           string
	batch           int
	timeout         time.Duration
	pretty          bool
	literal         bool
	partition       int
	decodeKeyType   string
	decodeValueType string
	compressionType string
	bufferSize      int
	partitioner     string

	compression sarama.CompressionCodec
	decodeKey   func(string) ([]byte, error)
	decodeValue func(string) ([]byte, error)
	leaders     map[int32]*sarama.Broker
}

type producerMessage struct {
	Value     *string    `json:"value"`
	Key       *string    `json:"key"`
	Partition *int32     `json:"partition"`
	Timestamp *time.Time `json:"time"`
}

func (cmd *produceCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)

	flags.StringVar(&cmd.topic, "topic", "", "Topic to produce to (required).")
	flags.IntVar(&cmd.partition, "partition", 0, "Partition to produce to (defaults to 0).")
	flags.IntVar(&cmd.batch, "batch", 1, "Max size of a batch before sending it off")
	flags.DurationVar(&cmd.timeout, "timeout", 50*time.Millisecond, "Duration to wait for batch to be filled before sending it off")
	flags.BoolVar(&cmd.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&cmd.literal, "literal", false, "Interpret stdin line literally and pass it as value, key as null.")
	flags.StringVar(&cmd.compressionType, "compression", "", "Kafka message compression codec [gzip|snappy|lz4] (defaults to none)")
	flags.StringVar(&cmd.partitioner, "partitioner", "", "Optional partitioner to use. Available: hashCode")
	flags.StringVar(&cmd.decodeKeyType, "decodekey", "string", "Decode message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&cmd.decodeValueType, "decodevalue", "string", "Decode message value as (string|hex|base64), defaults to string.")
	flags.IntVar(&cmd.bufferSize, "buffersize", 16777216, "Buffer size for scanning stdin, defaults to 16777216=16*1024*1024.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, produceDocString)
	}
}

func (cmd *produceCmd) environFlags() map[string]string {
	return map[string]string{
		"topic":   "KT_TOPIC",
		"brokers": "KT_BROKERS",
	}
}

func (cmd *produceCmd) run(args []string) error {
	var err error
	cmd.decodeValue, err = decoderForType(cmd.decodeValueType)
	if err != nil {
		return fmt.Errorf("bad -decodevalue argument: %v", err)
	}
	cmd.decodeKey, err = decoderForType(cmd.decodeValueType)
	if err != nil {
		return fmt.Errorf("bad -decodekey argument: %v", err)
	}
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	compression, err := kafkaCompressionForType(cmd.compressionType)
	if err != nil {
		return fmt.Errorf("bad -compression argument: %v", err)
	}
	cmd.compression = compression

	defer cmd.close()
	if err := cmd.findLeaders(); err != nil {
		return fmt.Errorf("cannot find leaders for topic: %v", err)
	}
	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan producerMessage)
	batchedMessages := make(chan []producerMessage)

	go readStdinLines(cmd.bufferSize, stdin)
	out := newPrinter(cmd.pretty)

	q := make(chan struct{})
	go listenForInterrupt(q)
	go cmd.readInput(q, stdin, lines)
	go cmd.deserializeLines(lines, messages, int32(len(cmd.leaders)))
	go cmd.batchRecords(messages, batchedMessages)
	return cmd.produce(batchedMessages, out)
}

func kafkaCompressionForType(codecName string) (sarama.CompressionCodec, error) {
	switch codecName {
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "":
		return sarama.CompressionNone, nil
	}
	return sarama.CompressionNone, fmt.Errorf("unsupported compression codec %#v - supported: gzip, snappy, lz4", codecName)
}

func (cmd *produceCmd) findLeaders() error {
	cfg, err := cmd.saramaConfig("produce")
	if err != nil {
		return err
	}
	cfg.Producer.RequiredAcks = sarama.WaitForAll

loop:
	for _, addr := range cmd.brokers {
		broker := sarama.NewBroker(addr)
		if err = broker.Open(cfg); err != nil {
			warningf("failed to open broker connection to %v: %v", addr, err)
			continue loop
		}
		if connected, err := broker.Connected(); !connected || err != nil {
			warningf("Failed to open broker connection to %v: %v", addr, err)
			continue loop
		}

		res, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{cmd.topic}})
		if err != nil {
			warningf("Failed to get metadata from %#v: %v", addr, err)
			continue loop
		}

		brokers := map[int32]*sarama.Broker{}
		for _, b := range res.Brokers {
			brokers[b.ID()] = b
		}

		for _, tm := range res.Topics {
			if tm.Name == cmd.topic {
				if tm.Err != sarama.ErrNoError {
					warningf("Failed to get metadata from %#v: %v", addr, tm.Err)
					continue loop
				}

				cmd.leaders = map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						return fmt.Errorf("failed to find leader in broker response, giving up")
					}

					if err = b.Open(cfg); err != nil && err != sarama.ErrAlreadyConnected {
						return fmt.Errorf("cannot open broker connection: %v", err)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						return fmt.Errorf("failed to wait for broker connection to open: %v", err)
					}

					cmd.leaders[pm.ID] = b
				}
				return nil
			}
		}
	}
	return fmt.Errorf("failed to find leader for given topic")
}

func (cmd *produceCmd) close() {
	for _, b := range cmd.leaders {
		connected, err := b.Connected()
		if err != nil {
			warningf("Failed to check if broker is connected: %v", err)
			continue
		}
		if !connected {
			continue
		}
		if err := b.Close(); err != nil {
			warningf("Failed to close broker %v connection: %v", b, err)
		}
	}
}

func (cmd *produceCmd) deserializeLines(in chan string, out chan producerMessage, partitionCount int32) {
	defer close(out)
	partition := int32(cmd.partition)
	for l := range in {
		l := l
		var msg producerMessage
		l = strings.TrimSpace(l)
		if l == "" {
			// Ignore blank line.
			continue
		}

		if cmd.literal {
			msg.Value = &l
			msg.Partition = &partition
		} else if err := json.Unmarshal([]byte(l), &msg); err != nil {
			warningf("skipping invalid JSON input %q: %v", l, err)
			continue
		}

		var part int32
		if msg.Key != nil && cmd.partitioner == "hashCode" {
			part = hashCodePartition(*msg.Key, partitionCount)
		}
		if msg.Partition == nil {
			msg.Partition = &part
		}
		out <- msg
	}
}

func (cmd *produceCmd) batchRecords(in chan producerMessage, out chan<- []producerMessage) {
	defer close(out)

	var messages []producerMessage
	send := func() {
		if len(messages) > 0 {
			out <- messages
			messages = nil
		}
	}
	defer send()
	for {
		select {
		case m, ok := <-in:
			if !ok {
				return
			}
			messages = append(messages, m)
			if len(messages) >= cmd.batch {
				send()
			}
		case <-time.After(cmd.timeout):
			send()
		}
	}
}

type partitionProduceResult struct {
	start int64
	count int64
}

func (cmd *produceCmd) makeSaramaMessage(msg producerMessage) (*sarama.Message, error) {
	sm := &sarama.Message{Codec: cmd.compression}
	if msg.Key != nil {
		key, err := cmd.decodeKey(*msg.Key)
		if err != nil {
			return nil, fmt.Errorf("cannot decode key: %v", err)
		}
		sm.Key = key
	}
	if msg.Value != nil {
		value, err := cmd.decodeValue(*msg.Value)
		if err != nil {
			return nil, fmt.Errorf("cannot decode value: %v", err)
		}
		sm.Value = value
	}
	if cmd.version.IsAtLeast(sarama.V0_10_0_0) {
		sm.Version = 1
		if msg.Timestamp != nil {
			sm.Timestamp = *msg.Timestamp
		} else {
			sm.Timestamp = time.Now()
		}
	}
	return sm, nil
}

func (cmd *produceCmd) produceBatch(leaders map[int32]*sarama.Broker, batch []producerMessage, out *printer) error {
	requests := map[*sarama.Broker]*sarama.ProduceRequest{}
	for _, msg := range batch {
		broker, ok := leaders[*msg.Partition]
		if !ok {
			return fmt.Errorf("non-configured partition %v", *msg.Partition)
		}
		req, ok := requests[broker]
		if !ok {
			req = &sarama.ProduceRequest{RequiredAcks: sarama.WaitForAll, Timeout: 10000}
			requests[broker] = req
		}
		sm, err := cmd.makeSaramaMessage(msg)
		if err != nil {
			return err
		}
		req.AddMessage(cmd.topic, *msg.Partition, sm)
	}
	for broker, req := range requests {
		resp, err := broker.Produce(req)
		if err != nil {
			return fmt.Errorf("failed to send request to broker %#v: %v", broker, err)
		}
		offsets, err := readPartitionOffsetResults(resp)
		if err != nil {
			return fmt.Errorf("failed to read producer response: %v", err)
		}
		for p, o := range offsets {
			out.print(map[string]interface{}{"partition": p, "startOffset": o.start, "count": o.count})
		}
	}
	return nil
}

func readPartitionOffsetResults(resp *sarama.ProduceResponse) (map[int32]partitionProduceResult, error) {
	offsets := map[int32]partitionProduceResult{}
	for _, blocks := range resp.Blocks {
		for partition, block := range blocks {
			if block.Err != sarama.ErrNoError {
				warningf("Failed to send message: %v", block.Err.Error())
				return offsets, block.Err
			}

			if r, ok := offsets[partition]; ok {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: r.count + 1}
			} else {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: 1}
			}
		}
	}
	return offsets, nil
}

func (cmd *produceCmd) produce(in chan []producerMessage, out *printer) error {
	for b := range in {
		if err := cmd.produceBatch(cmd.leaders, b, out); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *produceCmd) readInput(q chan struct{}, stdin chan string, out chan string) {
	defer close(out)
	for {
		select {
		case l, ok := <-stdin:
			if !ok {
				return
			}
			out <- l
		case <-q:
			return
		}
	}
}

var produceDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

Input is read from stdin and separated by newlines.

If you want to use the -partitioner keep in mind that the hashCode
implementation is not the default for Kafka's producer anymore.

To specify the key, value and partition individually pass it as a JSON object
like the following:

    {"key": "id-23", "value": "message content", "partition": 0}

In case the input line cannot be interpeted as a JSON object the key and value
both default to the input line and partition to 0.

Examples:

Send a single message with a specific key:

  $ echo '{"key": "id-23", "value": "ola", "partition": 0}' | kt produce -topic greetings
  Sent message to partition 0 at offset 3.

  $ kt consume -topic greetings -timeout 1s -offsets 0:3-
  {"partition":0,"offset":3,"key":"id-23","message":"ola"}

Keep reading input from stdin until interrupted (via ^C).

  $ kt produce -topic greetings
  hello.
  Sent message to partition 0 at offset 4.
  bonjour.
  Sent message to partition 0 at offset 5.

  $ kt consume -topic greetings -timeout 1s -offsets 0:4-
  {"partition":0,"offset":4,"key":"hello.","message":"hello."}
  {"partition":0,"offset":5,"key":"bonjour.","message":"bonjour."}
`
