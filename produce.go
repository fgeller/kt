package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

func produceCommand() command {
	produce := flag.NewFlagSet("produce", flag.ExitOnError)
	produce.StringVar(&config.produce.args.topic, "topic", "", "Topic to produce to.")
	produce.StringVar(&config.produce.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")

	produce.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of produce:")
		produce.PrintDefaults()
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
					msg := &sarama.ProducerMessage{
						Topic: config.produce.topic,
						Value: sarama.StringEncoder(l),
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
