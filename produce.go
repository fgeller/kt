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
	keyCodecType    string
	valueCodecType  string
	compressionType string
	partitionerType string
	maxLineLen      int

	partitioner sarama.PartitionerConstructor
	compression sarama.CompressionCodec
	decodeKey   func(json.RawMessage) ([]byte, error)
	decodeValue func(json.RawMessage) ([]byte, error)
	leaders     map[int32]*sarama.Broker
}

// producerMessage defines the format of messages
// accepted in the producer command's standard input.
type producerMessage struct {
	Value     json.RawMessage `json:"value"`
	Key       json.RawMessage `json:"key"`
	Partition *int32          `json:"partition"`
	Timestamp *time.Time      `json:"time"`
}

var partitioners = map[string]func(topic string) sarama.Partitioner{
	"sarama":     sarama.NewHashPartitioner,
	"std":        sarama.NewReferenceHashPartitioner,
	"random":     sarama.NewRandomPartitioner,
	"roundrobin": sarama.NewRoundRobinPartitioner,
}

var compressionTypes = map[string]sarama.CompressionCodec{
	"none":   sarama.CompressionNone,
	"gzip":   sarama.CompressionGZIP,
	"snappy": sarama.CompressionSnappy,
	"lz4":    sarama.CompressionLZ4,
	"zstd":   sarama.CompressionZSTD,
}

func (cmd *produceCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)

	flags.StringVar(&cmd.topic, "topic", "", "Topic to produce to (required).")
	flags.IntVar(&cmd.batch, "batch", 1, "Max size of a batch before sending it off")
	flags.DurationVar(&cmd.timeout, "timeout", 50*time.Millisecond, "Duration to wait for batch to be filled before sending it off")
	flags.BoolVar(&cmd.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&cmd.literal, "literal", false, "Interpret stdin line literally and pass it as value, key as null.")
	flags.StringVar(&cmd.compressionType, "compression", "none", "Kafka message compression codec [gzip|snappy|lz4]")
	flags.StringVar(&cmd.partitionerType, "partitioner", "sarama", "Optional partitioner to use. Available: sarama, std, random, roundrobin")
	flags.StringVar(&cmd.keyCodecType, "keycodec", "string", "Interpret message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&cmd.valueCodecType, "valuecodec", "json", "Interpret message value as (json|string|hex|base64), defaults to json.")
	flags.IntVar(&cmd.maxLineLen, "maxline", 16*1024*1024, "Maximum length of input line")

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
	partitioner, ok := partitioners[cmd.partitionerType]
	if !ok {
		return fmt.Errorf("unrecognised -partitioner argument %q", cmd.partitionerType)
	}
	cmd.partitioner = partitioner
	var err error
	cmd.decodeValue, err = decoderForType(cmd.valueCodecType)
	if err != nil {
		return fmt.Errorf("bad -valuecodec argument: %v", err)
	}
	if cmd.keyCodecType == "json" {
		// JSON for keys is not a good idea.
		return fmt.Errorf("JSON key codec not supported")
	}
	cmd.decodeKey, err = decoderForType(cmd.keyCodecType)
	if err != nil {
		return fmt.Errorf("bad -keycodec argument: %v", err)
	}
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	compression, ok := compressionTypes[cmd.compressionType]
	if !ok {
		return fmt.Errorf("unsupported -compression codec %#v - supported: none, gzip, snappy, lz4, zstd", cmd.compressionType)
	}
	cmd.compression = compression

	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan producerMessage)

	go readStdinLines(cmd.maxLineLen, stdin)

	q := make(chan struct{})
	go listenForInterrupt(q)
	go cmd.readInput(q, stdin, lines)
	go cmd.deserializeLines(lines, messages, int32(len(cmd.leaders)))
	return cmd.produce(messages)
}

func (cmd *produceCmd) deserializeLines(in chan string, out chan producerMessage, partitionCount int32) {
	defer close(out)
	for l := range in {
		l := l
		var msg producerMessage

		if cmd.literal {
			data, _ := json.Marshal(l) // Marshaling a string can't fail.
			msg.Value = json.RawMessage(data)
			// TODO allow specifying a key?
		} else {
			if l = strings.TrimSpace(l); l == "" {
				// Ignore blank line.
				continue
			}
			if err := json.Unmarshal([]byte(l), &msg); err != nil {
				warningf("skipping invalid JSON input %q: %v", l, err)
				continue
			}
		}
		out <- msg
	}
}

func (cmd *produceCmd) makeSaramaMessage(msg producerMessage) (*sarama.ProducerMessage, error) {
	sm := &sarama.ProducerMessage{
		Topic: cmd.topic,
	}
	if msg.Partition != nil {
		// This is a hack to get the manual partition through
		// to the partitioner, because we don't know that
		// all messages being produced specify the partition or not.
		sm.Metadata = *msg.Partition
	}
	if msg.Key != nil {
		key, err := cmd.decodeKey(msg.Key)
		if err != nil {
			return nil, fmt.Errorf("cannot decode key: %v", err)
		}
		sm.Key = sarama.ByteEncoder(key)
	}
	if msg.Value != nil {
		value, err := cmd.decodeValue(msg.Value)
		if err != nil {
			return nil, fmt.Errorf("cannot decode value: %v", err)
		}
		sm.Value = sarama.ByteEncoder(value)
	}
	if msg.Timestamp != nil {
		sm.Timestamp = *msg.Timestamp
	}
	return sm, nil
}

func (cmd *produceCmd) produce(in chan producerMessage) error {
	cfg, err := cmd.saramaConfig("produce")
	if err != nil {
		return err
	}
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = cmd.compression
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return producerPartitioner{cmd.partitioner(topic)}
	}
	producer, err := sarama.NewAsyncProducer(cmd.brokers, cfg)
	if err != nil {
		return err
	}
	go func() {
		// Note: if there are producer errors, then this
		// goroutine will be left hanging around, but we don't care much.
		input := producer.Input()
		for m := range in {
			sm, err := cmd.makeSaramaMessage(m)
			if err != nil {
				warningf("invalid message: %v", err)
				continue
			}
			input <- sm
		}
		producer.AsyncClose()
	}()
	// We need to read from the producer errors channel until
	// the errors channel is closed.
	var errors sarama.ProducerErrors
	for err := range producer.Errors() {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		for _, err := range errors {
			warningf("error producing message %v", err.Err)
		}
		return errors
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

type producerPartitioner struct {
	sarama.Partitioner
}

// Partition implements sarama.Partitioner.Partition by returning the partition
// specified in the input message if it was present, or using the underlying
// user-specified partitioner if not.
func (p producerPartitioner) Partition(m *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if partition, ok := m.Metadata.(int32); ok {
		return partition, nil
	}
	return p.Partitioner.Partition(m, numPartitions)
}

// MessageRequiresConsistency implements sarama.DynamicConsistencyPartitioner.MessageRequiresConsistency
// by querying the underlying partitioner.
func (p producerPartitioner) MessageRequiresConsistency(m *sarama.ProducerMessage) bool {
	if p1, ok := p.Partitioner.(sarama.DynamicConsistencyPartitioner); ok {
		return p1.MessageRequiresConsistency(m)
	}
	return p.RequiresConsistency()
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
