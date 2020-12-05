package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type produceArgs struct {
	topic       string
	partition   int
	brokers     string
	auth        string
	batch       int
	timeout     time.Duration
	verbose     bool
	pretty      bool
	version     string
	compression string
	literal     bool
	decodeKey   string
	decodeValue string
	partitioner string
	bufferSize  int
}

type message struct {
	Key       *string `json:"key"`
	Value     *string `json:"value"`
	Partition *int32  `json:"partition"`
}

func (cmd *produceCmd) read(as []string) produceArgs {
	var args produceArgs
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to produce to (required).")
	flags.IntVar(&args.partition, "partition", 0, "Partition to produce to (defaults to 0).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", ENV_AUTH))
	flags.IntVar(&args.batch, "batch", 1, "Max size of a batch before sending it off")
	flags.DurationVar(&args.timeout, "timeout", 50*time.Millisecond, "Duration to wait for batch to be filled before sending it off")
	flags.BoolVar(&args.verbose, "verbose", false, "Verbose output")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&args.literal, "literal", false, "Interpret stdin line literally and pass it as value, key as null.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.compression, "compression", "", "Kafka message compression codec [gzip|snappy|lz4] (defaults to none)")
	flags.StringVar(&args.partitioner, "partitioner", "", "Optional partitioner to use. Available: hashCode, hashCodeByValue")
	flags.StringVar(&args.decodeKey, "decodekey", "string", "Decode message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.decodeValue, "decodevalue", "string", "Decode message value as (string|hex|base64), defaults to string.")
	flags.IntVar(&args.bufferSize, "buffersize", 16777216, "Buffer size for scanning stdin, defaults to 16777216=16*1024*1024.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, produceDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

func (cmd *produceCmd) failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	failf("use \"kt produce -help\" for more information")
}

func (cmd *produceCmd) parseArgs(as []string) {
	args := cmd.read(as)
	envTopic := os.Getenv(ENV_TOPIC)
	if args.topic == "" {
		if envTopic == "" {
			cmd.failStartup("Topic name is required.")
		} else {
			args.topic = envTopic
		}
	}
	cmd.topic = args.topic

	readAuthFile(args.auth, os.Getenv(ENV_AUTH), &cmd.auth)

	envBrokers := os.Getenv(ENV_BROKERS)
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}

	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}

	if args.decodeValue != "string" && args.decodeValue != "hex" && args.decodeValue != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported decodevalue argument %#v, only string, hex and base64 are supported.`, args.decodeValue))
		return
	}
	cmd.decodeValue = args.decodeValue

	if args.decodeKey != "string" && args.decodeKey != "hex" && args.decodeKey != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported decodekey argument %#v, only string, hex and base64 are supported.`, args.decodeValue))
		return
	}
	cmd.decodeKey = args.decodeKey

	cmd.batch = args.batch
	cmd.timeout = args.timeout
	cmd.verbose = args.verbose
	cmd.pretty = args.pretty
	cmd.literal = args.literal
	cmd.partition = int32(args.partition)
	cmd.partitioner = args.partitioner
	cmd.version = kafkaVersion(args.version)
	cmd.compression = kafkaCompression(args.compression)
	cmd.bufferSize = args.bufferSize
}

func kafkaCompression(codecName string) sarama.CompressionCodec {
	switch codecName {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "":
		return sarama.CompressionNone
	}

	failf("unsupported compression codec %#v - supported: gzip, snappy, lz4", codecName)
	panic("unreachable")
}

func (cmd *produceCmd) findLeaders() {
	var (
		usr *user.User
		err error
		res *sarama.MetadataResponse
		req = sarama.MetadataRequest{Topics: []string{cmd.topic}}
		cfg = sarama.NewConfig()
	)

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-produce-" + sanitizeUsername(usr.Username)
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

loop:
	for _, addr := range cmd.brokers {
		broker := sarama.NewBroker(addr)
		if err = broker.Open(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}
		if connected, err := broker.Connected(); !connected || err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}

		if res, err = broker.GetMetadata(&req); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get metadata from %#v. err=%v\n", addr, err)
			continue loop
		}

		brokers := map[int32]*sarama.Broker{}
		for _, b := range res.Brokers {
			brokers[b.ID()] = b
		}

		for _, tm := range res.Topics {
			if tm.Name == cmd.topic {
				if tm.Err != sarama.ErrNoError {
					fmt.Fprintf(os.Stderr, "Failed to get metadata from %#v. err=%v\n", addr, tm.Err)
					continue loop
				}

				cmd.leaders = map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						failf("failed to find leader in broker response, giving up")
					}

					if err = b.Open(cfg); err != nil && err != sarama.ErrAlreadyConnected {
						failf("failed to open broker connection err=%s", err)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						failf("failed to wait for broker connection to open err=%s", err)
					}

					cmd.leaders[pm.ID] = b
				}
				return
			}
		}
	}

	failf("failed to find leader for given topic")
}

type produceCmd struct {
	topic       string
	brokers     []string
	auth        authConfig
	batch       int
	timeout     time.Duration
	verbose     bool
	pretty      bool
	literal     bool
	partition   int32
	version     sarama.KafkaVersion
	compression sarama.CompressionCodec
	partitioner string
	decodeKey   string
	decodeValue string
	bufferSize  int

	leaders map[int32]*sarama.Broker
}

func (cmd *produceCmd) run(as []string) {
	cmd.parseArgs(as)
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	defer cmd.close()
	cmd.findLeaders()
	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan message)
	batchedMessages := make(chan []message)
	out := make(chan printContext)
	q := make(chan struct{})

	go readStdinLines(cmd.bufferSize, stdin)
	go print(out, cmd.pretty)

	go listenForInterrupt(q)
	go cmd.readInput(q, stdin, lines)
	go cmd.deserializeLines(lines, messages, int32(len(cmd.leaders)))
	go cmd.batchRecords(messages, batchedMessages)
	cmd.produce(batchedMessages, out)
}

func (cmd *produceCmd) close() {
	for _, b := range cmd.leaders {
		var (
			connected bool
			err       error
		)

		if connected, err = b.Connected(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to check if broker is connected. err=%s\n", err)
			continue
		}

		if !connected {
			continue
		}

		if err = b.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to close broker %v connection. err=%s\n", b, err)
		}
	}
}

func (cmd *produceCmd) deserializeLines(in chan string, out chan message, partitionCount int32) {
	defer func() { close(out) }()
	for {
		select {
		case l, ok := <-in:
			if !ok {
				return
			}
			var msg message

			switch {
			case cmd.literal:
				msg.Value = &l
				msg.Partition = &cmd.partition
			default:
				if err := json.Unmarshal([]byte(l), &msg); err != nil {
					if cmd.verbose {
						fmt.Fprintf(os.Stderr, "Failed to unmarshal input [%v], falling back to defaults. err=%v\n", l, err)
					}
					var v *string = &l
					if len(l) == 0 {
						v = nil
					}
					msg = message{Key: nil, Value: v}
				}
			}

			var part int32 = 0
			if msg.Value != nil && cmd.partitioner == "hashCodeByValue" {
				part = hashCodePartition(*msg.Value, partitionCount)
				msg.Partition = &part
			}else {
				if msg.Key != nil && cmd.partitioner == "hashCode" {
					part = hashCodePartition(*msg.Key, partitionCount)
				}
				if msg.Partition == nil {
					msg.Partition = &part
				}
			}

			out <- msg
		}
	}
}

func (cmd *produceCmd) batchRecords(in chan message, out chan []message) {
	defer func() { close(out) }()

	messages := []message{}
	send := func() {
		out <- messages
		messages = []message{}
	}

	for {
		select {
		case m, ok := <-in:
			if !ok {
				send()
				return
			}

			messages = append(messages, m)
			if len(messages) > 0 && len(messages) >= cmd.batch {
				send()
			}
		case <-time.After(cmd.timeout):
			if len(messages) > 0 {
				send()
			}
		}
	}
}

type partitionProduceResult struct {
	start int64
	count int64
}

func (cmd *produceCmd) makeSaramaMessage(msg message) (*sarama.Message, error) {
	var (
		err error
		sm  = &sarama.Message{Codec: cmd.compression}
	)

	if msg.Key != nil {
		switch cmd.decodeKey {
		case "hex":
			if sm.Key, err = hex.DecodeString(*msg.Key); err != nil {
				return sm, fmt.Errorf("failed to decode key as hex string, err=%v", err)
			}
		case "base64":
			if sm.Key, err = base64.StdEncoding.DecodeString(*msg.Key); err != nil {
				return sm, fmt.Errorf("failed to decode key as base64 string, err=%v", err)
			}
		default: // string
			sm.Key = []byte(*msg.Key)
		}
	}

	if msg.Value != nil {
		switch cmd.decodeValue {
		case "hex":
			if sm.Value, err = hex.DecodeString(*msg.Value); err != nil {
				return sm, fmt.Errorf("failed to decode value as hex string, err=%v", err)
			}
		case "base64":
			if sm.Value, err = base64.StdEncoding.DecodeString(*msg.Value); err != nil {
				return sm, fmt.Errorf("failed to decode value as base64 string, err=%v", err)
			}
		default: // string
			sm.Value = []byte(*msg.Value)
		}
	}

	if cmd.version.IsAtLeast(sarama.V0_10_0_0) {
		sm.Version = 1
		sm.Timestamp = time.Now()
	}

	return sm, nil
}

func (cmd *produceCmd) produceBatch(leaders map[int32]*sarama.Broker, batch []message, out chan printContext) error {
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
			return fmt.Errorf("failed to send request to broker %#v. err=%s", broker, err)
		}

		offsets, err := readPartitionOffsetResults(resp)
		if err != nil {

			return fmt.Errorf("failed to read producer response err=%s", err)
		}

		for p, o := range offsets {
			result := map[string]interface{}{"partition": p, "startOffset": o.start, "count": o.count}
			ctx := printContext{output: result, done: make(chan struct{})}
			out <- ctx
			<-ctx.done
		}
	}

	return nil
}

func readPartitionOffsetResults(resp *sarama.ProduceResponse) (map[int32]partitionProduceResult, error) {
	offsets := map[int32]partitionProduceResult{}
	for _, blocks := range resp.Blocks {
		for partition, block := range blocks {
			if block.Err != sarama.ErrNoError {
				fmt.Fprintf(os.Stderr, "Failed to send message. err=%s\n", block.Err.Error())
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

func (cmd *produceCmd) produce(in chan []message, out chan printContext) {
	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			if err := cmd.produceBatch(cmd.leaders, b, out); err != nil {
				fmt.Fprintln(os.Stderr, err.Error()) // TODO: failf
				return
			}
		}
	}
}

func (cmd *produceCmd) readInput(q chan struct{}, stdin chan string, out chan string) {
	defer func() { close(out) }()
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

var produceDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

Input is read from stdin and separated by newlines.

If you want to use the -partitioner keep in mind that the hashCode
implementation is not the default for Kafka's producer anymore.

To specify the key, value and partition individually pass it as a JSON object
like the following:

    {"key": "id-23", "value": "message content", "partition": 0}

In case the input line cannot be interpeted as a JSON object the key and value
both default to the input line and partition to 0.

If you don't want to specify key for single message, in other words, it doesn't matter that a message goes
to a random paritition (with equal probability), you can set the flag '-partitioner' with 'hashCodeByValue'.
That will tell kt to take the value of a message to calculate a hashcode deciding which paritition it will go to.
This can be helpful when you just want there are many messages distributed in partitions of a topic, and don't
care about what the content is. 

Examples:

Send a single message with a specific key:

  $ echo '{"key": "id-23", "value": "ola", "partition": 0}' | kt produce -topic greetings
  Sent message to partition 0 at offset 3.

  $ kt consume -topic greetings -timeout 1s -offsets 0:3-
  {"partition":0,"offset":3,"key":"id-23","message":"ola"}

Send a single message without specified key:
  $ echo 'no key specified message' | kt produce -topic greetings -partitioner hashCodeByValue
  Sent message to a partition decided by your case
  

Keep reading input from stdin until interrupted (via ^C).

  $ kt produce -topic greetings
  hello.
  Sent message to partition 0 at offset 4.
  bonjour.
  Sent message to partition 0 at offset 5.

  $ kt consume -topic greetings -timeout 1s -offsets 0:4-
  {"partition":0,"offset":4,"key":"hello.","message":"hello."}
  {"partition":0,"offset":5,"key":"bonjour.","message":"bonjour."}
`, ENV_TOPIC, ENV_BROKERS)
