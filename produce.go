package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/Shopify/sarama"
)

type produceArgs struct {
	topic       string
	partition   int
	brokers     string
	batch       int
	timeout     time.Duration
	verbose     bool
	version     string
	literal     bool
	partitioner string
}

type message struct {
	Key       *string `json:"key"`
	Value     *string `json:"value"`
	Partition *int32  `json:"partition"`
}

func (p *produceCmd) read(as []string) produceArgs {
	var args produceArgs
	flags := flag.NewFlagSet("produce", flag.ExitOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to produce to (required).")
	flags.IntVar(&args.partition, "partition", 0, "Partition to produce to (defaults to 0).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.IntVar(&args.batch, "batch", 1, "Max size of a batch before sending it off")
	flags.DurationVar(&args.timeout, "timeout", 50*time.Millisecond, "Duration to wait for batch to be filled before sending it off")
	flags.BoolVar(&args.verbose, "verbose", false, "Verbose output")
	flags.BoolVar(&args.literal, "literal", false, "Interpret stdin line literally and pass it as value, key as null.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.partitioner, "partitioner", "", "Optional partitioner to use. Available: hashCode")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, produceDocString)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

func (p *produceCmd) failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	fmt.Fprintln(os.Stderr, "Use \"kt produce -help\" for more information.")
	os.Exit(1)
}

func (p *produceCmd) parseArgs(as []string) {
	args := p.read(as)
	envTopic := os.Getenv("KT_TOPIC")
	if args.topic == "" {
		if envTopic == "" {
			p.failStartup("Topic name is required.")
		} else {
			args.topic = envTopic
		}
	}
	p.topic = args.topic

	envBrokers := os.Getenv("KT_BROKERS")
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}

	p.brokers = strings.Split(args.brokers, ",")
	for i, b := range p.brokers {
		if !strings.Contains(b, ":") {
			p.brokers[i] = b + ":9092"
		}
	}

	p.batch = args.batch
	p.timeout = args.timeout
	p.verbose = args.verbose
	p.literal = args.literal
	p.partition = int32(args.partition)
	p.version = kafkaVersion(args.version)
}

func (p *produceCmd) mkSaramaConfig() {
	var (
		usr *user.User
		err error
	)

	p.saramaConfig = sarama.NewConfig()
	p.saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	p.saramaConfig.Version = p.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	p.saramaConfig.ClientID = "kt-produce-" + usr.Username
	if p.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", p.saramaConfig)
	}

}

func (p *produceCmd) findLeaders() {
	var (
		usr *user.User
		err error
		res *sarama.MetadataResponse
		req = sarama.MetadataRequest{Topics: []string{p.topic}}
		cfg = sarama.NewConfig()
	)

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Version = p.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-produce-" + usr.Username
	if p.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

loop:
	for _, addr := range p.brokers {
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
			if tm.Name == p.topic {
				if tm.Err != sarama.ErrNoError {
					fmt.Fprintf(os.Stderr, "Failed to get metadata from %#v. err=%v\n", addr, tm.Err)
					continue loop
				}

				p.leaders = map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						fmt.Fprintf(os.Stderr, "Failed to find leader in broker response, giving up.\n")
						os.Exit(1)
					}

					if err = b.Open(cfg); err != nil && err != sarama.ErrAlreadyConnected {
						fmt.Fprintf(os.Stderr, "Failed to open broker connection. err=%s\n", err)
						os.Exit(1)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						fmt.Fprintf(os.Stderr, "Failed to wait for broker connection to open. err=%s\n", err)
						os.Exit(1)
					}

					p.leaders[pm.ID] = b
				}
				return
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Failed to find leader for given topic.\n")
	os.Exit(1)
}

type produceCmd struct {
	topic       string
	brokers     []string
	batch       int
	timeout     time.Duration
	verbose     bool
	literal     bool
	partition   int32
	version     sarama.KafkaVersion
	partitioner string

	saramaConfig *sarama.Config
	leaders      map[int32]*sarama.Broker
}

func (p *produceCmd) run(as []string, q chan struct{}) {
	p.parseArgs(as)
	if p.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	defer p.close()
	p.findLeaders()
	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan message)
	batchedMessages := make(chan []message)

	go readStdinLines(stdin)

	go p.readInput(q, stdin, lines)
	go p.deserializeLines(lines, messages, int32(len(p.leaders)))
	go p.batchRecords(messages, batchedMessages)
	p.produce(batchedMessages)
}

func (c *produceCmd) close() {
	for _, b := range c.leaders {
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

func (p *produceCmd) deserializeLines(in chan string, out chan message, partitionCount int32) {
	defer func() { close(out) }()
	for {
		select {
		case l, ok := <-in:
			if !ok {
				return
			}
			var msg message

			switch {
			case p.literal:
				msg.Value = &l
				msg.Partition = &p.partition
			default:
				if err := json.Unmarshal([]byte(l), &msg); err != nil {
					if p.verbose {
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
			if msg.Key != nil && p.partitioner == "hashCode" {
				part = hashCodePartition(*msg.Key, partitionCount)
			}
			if msg.Partition == nil {
				msg.Partition = &part
			}

			out <- msg
		}
	}
}

func (p *produceCmd) batchRecords(in chan message, out chan []message) {
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
			if len(messages) > 0 && len(messages) >= p.batch {
				send()
			}
		case <-time.After(p.timeout):
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

func (m message) asSaramaMessage() *sarama.Message {
	msg := sarama.Message{Codec: sarama.CompressionNone}
	if m.Key != nil {
		msg.Key = []byte(*m.Key)
	}
	if m.Value != nil {
		msg.Value = []byte(*m.Value)
	}
	return &msg
}

func (p *produceCmd) produceBatch(leaders map[int32]*sarama.Broker, batch []message) error {
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

		req.AddMessage(p.topic, *msg.Partition, msg.asSaramaMessage())
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
			fmt.Fprintf(
				os.Stdout,
				`{"partition": %v, "startOffset": %v, "count": %v}
`,
				p,
				o.start,
				o.count,
			)
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

func (p *produceCmd) produce(in chan []message) {
	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			if err := p.produceBatch(p.leaders, b); err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				return
			}
		}
	}
}

func readStdinLines(out chan string) {
	defer close(out)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		out <- scanner.Text()
	}
}

func (p *produceCmd) readInput(q chan struct{}, stdin chan string, out chan string) {
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

// hashCode imitates the behavior of the JDK's String#hashCode method.
// https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#hashCode()
//
// As strings are encoded in utf16 on the JVM, this implementation checks wether
// s contains non-bmp runes and uses utf16 surrogate pairs for those.
func hashCode(s string) (hc int32) {
	for _, r := range s {
		r1, r2 := utf16.EncodeRune(r)
		if r1 == 0xfffd && r1 == r2 {
			hc = hc*31 + r
		} else {
			hc = (hc*31+r1)*31 + r2
		}
	}
	return
}

func kafkaAbs(i int32) int32 {
	switch {
	case i == -2147483648: // Integer.MIN_VALUE
		return 0
	case i < 0:
		return i * -1
	default:
		return i
	}
}

func hashCodePartition(key string, partitions int32) int32 {
	if partitions <= 0 {
		return -1
	}

	return kafkaAbs(hashCode(key)) % partitions
}

var produceDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

Input is read from stdin and separated by newlines.

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
