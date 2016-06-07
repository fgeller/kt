package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type produceConfig struct {
	topic   string
	brokers []string
	batch   int
	timeout time.Duration
	verbose bool
	args    struct {
		topic   string
		brokers string
		batch   int
		timeout time.Duration
		verbose bool
	}
}

type message struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int32  `json:"partition"`
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
}

func produceCommand() command {
	return command{
		flags:     produceFlags(),
		parseArgs: produceParseArgs,
		run: func(closer chan struct{}) {
			if config.produce.verbose {
				sarama.Logger = log.New(os.Stdout, "", log.LstdFlags)
			}

			broker := sarama.NewBroker(config.produce.brokers[0])
			conf := sarama.NewConfig()
			conf.Producer.RequiredAcks = sarama.WaitForAll
			err := broker.Open(conf)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open broker connection. err=%s\n", err)
				os.Exit(1)
			}
			if connected, err := broker.Connected(); !connected || err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open broker connection. err=%s\n", err)
				os.Exit(1)
			}

			defer func() {
				if err := broker.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to close broker connection. err=%s\n", err)
				}
			}()

			stdinLines := make(chan string)
			messages := make(chan message)
			batchedMessages := make(chan []message)
			go readStdinLines(closer, stdinLines)
			go batchRecords(closer, messages, batchedMessages)
			go produce(closer, broker, batchedMessages)

			for {
				select {
				case _, ok := <-closer:
					if !ok {
						return
					}
				case l := <-stdinLines:
					var in message
					err := json.Unmarshal([]byte(l), &in)
					if err != nil {
						if config.produce.verbose {
							fmt.Printf("Failed to unmarshal input, falling back to defaults. err=%v\n", err)
						}
						in = message{Key: l, Value: l, Partition: 0}
					}
					messages <- in
				}
			}
		},
	}
}

func batchRecords(closer chan struct{}, in chan message, out chan []message) {
	messages := []message{}
	send := func() {
		out <- messages
		messages = []message{}
	}

	for {
		select {
		case _, ok := <-closer:
			if !ok {
				fmt.Fprintf(os.Stderr, "Stopping, discarding %v messages.\n", len(messages))
				return
			}
		case m := <-in:
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

func produce(closer chan struct{}, broker *sarama.Broker, in chan []message) {
	for {
		select {
		case _, ok := <-closer:
			if !ok {
				return
			}
		case batch := <-in:
			req := &sarama.ProduceRequest{
				RequiredAcks: sarama.WaitForAll,
				Timeout:      1000,
			}
			for _, m := range batch {
				msg := sarama.Message{
					Codec: sarama.CompressionNone,
					Key:   []byte(m.Key),
					Value: []byte(m.Value),
					Set:   nil,
				}
				req.AddMessage(config.produce.topic, m.Partition, &msg)
			}
			resp, err := broker.Produce(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to send message, quitting. err=%s\n", err)
				return
			}

			offsets := map[int32]partitionProduceResult{}
			for _, m := range batch {
				block := resp.GetBlock(config.produce.topic, m.Partition)
				if block.Err != sarama.ErrNoError {
					fmt.Fprintf(os.Stderr, "Failed to send message, quitting. err=%s\n", block.Err.Error())
					return
				}

				if r, ok := offsets[m.Partition]; ok {
					offsets[m.Partition] = partitionProduceResult{start: block.Offset, count: r.count + 1}
				} else {
					offsets[m.Partition] = partitionProduceResult{start: block.Offset, count: 1}
				}
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
	}
}

func readStdinLines(stop chan struct{}, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		out <- line
	}
	close(stop)
}
