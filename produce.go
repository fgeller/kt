package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

type produceConfig struct {
	topic   string
	brokers []string
	args    struct {
		topic   string
		brokers string
	}
}

type message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func produceCommand() command {
	produce := flag.NewFlagSet("produce", flag.ExitOnError)
	produce.StringVar(&config.produce.args.topic, "topic", "", "Topic to produce to.")
	produce.StringVar(&config.produce.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")

	produce.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		produce.PrintDefaults()

		fmt.Fprintln(os.Stderr, `
Input is read from stdin and separated by newlines.

To specify the key and value individually pass it as a JSON object like the
following:

    {"key": "id-23", "value": "message content"}

In case the input line cannot be interpeted as a JSON object the key and value
both default to the input line.

Examples:

Send a single message with a specific key:

  $ echo '{"key": "id-23", "value": "ola"}' | kt produce -topic greetings
  Sent message to partition 0 at offset 3.

  $ kt consume -topic greetings -json -timeout 1s -offsets 3:
  {"partition":0,"offset":3,"key":"id-23","message":"ola"}

Keep reading input from stdin until interrupted (via ^C).

  $ kt produce -topic greetings
  hello.
  Sent message to partition 0 at offset 4.
  bonjour.
  Sent message to partition 0 at offset 5.

  $ kt consume -topic greetings -json -timeout 1s -offsets 4:
  {"partition":0,"offset":4,"key":"hello.","message":"hello."}
  {"partition":0,"offset":5,"key":"bonjour.","message":"bonjour."}
`)
		os.Exit(2)
	}

	return command{
		flags: produce,
		parseArgs: func(args []string) {
			produce.Parse(args)

			failStartup := func(msg string) {
				fmt.Fprintln(os.Stderr, msg)
				fmt.Fprintln(os.Stderr, "Use \"kt produce -help\" for more information.")
				os.Exit(1)
			}

			if config.produce.args.topic == "" {
				failStartup("Topic name is required.")
			}
			config.produce.topic = config.produce.args.topic

			config.produce.brokers = strings.Split(config.produce.args.brokers, ",")
			for i, b := range config.produce.brokers {
				if !strings.Contains(b, ":") {
					config.produce.brokers[i] = b + ":9092"
				}
			}
		},

		run: func(closer chan struct{}) {

			producer, err := sarama.NewSyncProducer(config.produce.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create producer. err=%s\n", err)
				os.Exit(1)
			}
			defer func() {
				if err := producer.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to close producer. err=%s\n", err)
				}
			}()

			stdinLines := make(chan string)
			go readStdinLines(stdinLines, closer)

			for {
				select {
				case <-closer:
					return
				case l := <-stdinLines:
					var in message
					err := json.Unmarshal([]byte(l), &in)
					if err != nil {
						in = message{Key: l, Value: l}
					}

					msg := &sarama.ProducerMessage{
						Topic: config.produce.topic,
						Key:   sarama.StringEncoder(in.Key),
						Value: sarama.StringEncoder(in.Value),
					}
					partition, offset, err := producer.SendMessage(msg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to send message, quitting. err=%s\n", err)
						return
					}
					fmt.Fprintf(os.Stderr, "Sent message to partition %d at offset %d.\n", partition, offset)
				}
			}
		},
	}
}

func readStdinLines(out chan string, stop chan struct{}) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		out <- line
	}
	stop <- struct{}{}
}
