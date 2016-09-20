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
	"sync"
	"time"
	"unicode/utf16"

	"github.com/Shopify/sarama"
)

type produceConfig struct {
	topic       string
	brokers     []string
	batch       int
	timeout     time.Duration
	verbose     bool
	version     sarama.KafkaVersion
	literal     bool
	partitioner string
	args        struct {
		topic       string
		brokers     string
		batch       int
		timeout     time.Duration
		verbose     bool
		version     string
		literal     bool
		partitioner string
	}
}

type message struct {
	Key       *string `json:"key"`
	Value     *string `json:"value"`
	Partition *int32  `json:"partition"`
}

func produceFlags() *flag.FlagSet {
	flags := flag.NewFlagSet("produce", flag.ExitOnError)
	flags.StringVar(
		&config.produce.args.topic,
		"topic",
		"",
		"Topic to produce to (required).",
	)
	flags.StringVar(
		&config.produce.args.brokers,
		"brokers",
		"",
		"Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).",
	)
	flags.IntVar(
		&config.produce.args.batch,
		"batch",
		1,
		"Max size of a batch before sending it off",
	)
	flags.DurationVar(
		&config.produce.args.timeout,
		"timeout",
		50*time.Millisecond,
		"Duration to wait for batch to be filled before sending it off",
	)
	flags.BoolVar(
		&config.produce.args.verbose,
		"verbose",
		false,
		"Verbose output",
	)
	flags.BoolVar(
		&config.produce.args.literal,
		"literal",
		false,
		"Interpret stdin line literally and pass it as value, key as null.",
	)
	flags.StringVar(&config.produce.args.version, "version", "", "Kafka protocol version")
	flags.StringVar(
		&config.produce.args.partitioner,
		"partitioner",
		"",
		"Optional partitioner to use. Available: hashCode",
	)

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		flags.PrintDefaults()

		fmt.Fprintln(os.Stderr, `
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
`)
		os.Exit(2)
	}

	return flags
}

func produceParseArgs() {
	failStartup := func(msg string) {
		fmt.Fprintln(os.Stderr, msg)
		fmt.Fprintln(os.Stderr, "Use \"kt produce -help\" for more information.")
		os.Exit(1)
	}

	envTopic := os.Getenv("KT_TOPIC")
	if config.produce.args.topic == "" {
		if envTopic == "" {
			failStartup("Topic name is required.")
		} else {
			config.produce.args.topic = envTopic
		}
	}
	config.produce.topic = config.produce.args.topic

	envBrokers := os.Getenv("KT_BROKERS")
	if config.produce.args.brokers == "" {
		if envBrokers != "" {
			config.produce.args.brokers = envBrokers
		} else {
			config.produce.args.brokers = "localhost:9092"
		}
	}
	config.produce.brokers = strings.Split(config.produce.args.brokers, ",")
	for i, b := range config.produce.brokers {
		if !strings.Contains(b, ":") {
			config.produce.brokers[i] = b + ":9092"
		}
	}

	config.produce.batch = config.produce.args.batch
	config.produce.timeout = config.produce.args.timeout
	config.produce.verbose = config.produce.args.verbose
	config.produce.literal = config.produce.args.literal
	config.produce.version = kafkaVersion(config.produce.args.version)
}

func mustFindLeaders() map[int32]*sarama.Broker {
	topic := config.produce.topic
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Version = config.produce.version
	u, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	conf.ClientID = "kt-produce-" + u.Username
	if config.produce.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", conf)
	}
	metaReq := sarama.MetadataRequest{[]string{topic}}

tryingBrokers:
	for _, brokerString := range config.produce.brokers {
		broker := sarama.NewBroker(brokerString)
		err := broker.Open(conf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open broker connection to %v. err=%s\n", brokerString, err)
			continue tryingBrokers
		}
		if connected, err := broker.Connected(); !connected || err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open broker connection to %v. err=%s\n", brokerString, err)
			continue tryingBrokers
		}

		metaResp, err := broker.GetMetadata(&metaReq)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get metadata from [%v]. err=%v\n", brokerString, err)
			continue tryingBrokers
		}

		brokers := map[int32]*sarama.Broker{}
		for _, b := range metaResp.Brokers {
			brokers[b.ID()] = b
		}

		for _, tm := range metaResp.Topics {
			if tm.Name == topic {
				if tm.Err != sarama.ErrNoError {
					fmt.Fprintf(os.Stderr, "Failed to get metadata from %v. err=%v\n", brokerString, tm.Err)
					continue tryingBrokers
				}

				bs := map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						fmt.Fprintf(os.Stderr, "Failed to find leader in broker response, giving up.\n")
						os.Exit(1)
					}

					err := b.Open(conf)
					if err != nil && err != sarama.ErrAlreadyConnected {
						fmt.Fprintf(os.Stderr, "Failed to open broker connection. err=%s\n", err)
						os.Exit(1)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						fmt.Fprintf(os.Stderr, "Failed to wait for broker connection to open. err=%s\n", err)
						os.Exit(1)
					}

					bs[pm.ID] = b
				}
				return bs
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Failed to find leader for given topic.\n")
	os.Exit(1)
	return nil
}

func produceCommand() command {
	return command{
		flags:     produceFlags(),
		parseArgs: produceParseArgs,
		run: func(closer chan struct{}) {
			if config.produce.verbose {
				sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
			}
			leaders := mustFindLeaders()
			defer func() {
				for _, b := range leaders {
					if err := b.Close(); err != nil {
						fmt.Fprintf(os.Stderr, "Failed to close broker connection. err=%s\n", err)
					}
				}
			}()

			stdin := make(chan string)
			lines := make(chan string)
			messages := make(chan message)
			batchedMessages := make(chan []message)
			go readStdinLines(stdin)
			var wg sync.WaitGroup
			wg.Add(4)
			go readInput(&wg, closer, stdin, lines)
			go deserializeLines(&wg, lines, messages, int32(len(leaders)))
			go batchRecords(&wg, messages, batchedMessages)
			go produce(&wg, leaders, batchedMessages)

			wg.Wait()
		},
	}
}

func deserializeLines(wg *sync.WaitGroup, in chan string, out chan message, partitionCount int32) {
	defer func() {
		close(out)
		wg.Done()
	}()

	for {
		select {
		case l, ok := <-in:
			if !ok {
				return
			}
			var msg message

			switch {
			case config.produce.literal:
				msg.Value = &l
			default:
				if err := json.Unmarshal([]byte(l), &msg); err != nil {
					if config.produce.verbose {
						fmt.Printf("Failed to unmarshal input [%v], falling back to defaults. err=%v\n", l, err)
					}
					var v *string = &l
					if len(l) == 0 {
						v = nil
					}
					msg = message{Key: nil, Value: v}
				}
			}

			var p int32 = 0
			if msg.Key != nil && config.produce.partitioner == "hashCode" {
				p = hashCodePartition(*msg.Key, partitionCount)
			}
			if msg.Partition == nil {
				msg.Partition = &p
			}

			out <- msg
		}
	}
}

func batchRecords(wg *sync.WaitGroup, in chan message, out chan []message) {
	defer func() {
		close(out)
		wg.Done()
	}()

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
			if len(messages) > 0 && len(messages) >= config.produce.batch {
				send()
			}
		case <-time.After(config.produce.timeout):
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

func produceBatch(leaders map[int32]*sarama.Broker, batch []message) error {
	requests := map[*sarama.Broker]*sarama.ProduceRequest{}
	for _, msg := range batch {
		broker, ok := leaders[*msg.Partition]
		if !ok {
			err := fmt.Errorf("Non-configured partition %v", *msg.Partition)
			fmt.Fprintf(os.Stderr, "%v.\n", err)
			return err
		}
		req, ok := requests[broker]
		if !ok {
			req = &sarama.ProduceRequest{RequiredAcks: sarama.WaitForAll, Timeout: 10000}
			requests[broker] = req
		}

		req.AddMessage(config.produce.topic, *msg.Partition, msg.asSaramaMessage())
	}

	for broker, req := range requests {
		resp, err := broker.Produce(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send request to broker %#v. err=%s\n", broker, err)
			return err
		}

		offsets, err := readPartitionOffsetResults(resp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read producer response. err=%s\n", err)
			return err
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

func produce(wg *sync.WaitGroup, leaders map[int32]*sarama.Broker, in chan []message) {
	defer wg.Done()

	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			if err := produceBatch(leaders, b); err != nil {
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

func readInput(wg *sync.WaitGroup, signals chan struct{}, stdin chan string, out chan string) {
	defer func() {
		close(out)
		wg.Done()
	}()

	for {
		select {
		case l, ok := <-stdin:
			if !ok {
				return
			}
			out <- l
		case <-signals:
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
