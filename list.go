package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
)

func listCommand() command {
	list := flag.NewFlagSet("list", flag.ExitOnError)
	list.StringVar(&config.list.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")

	list.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of list:")
		list.PrintDefaults()
		os.Exit(2)
	}

	return command{
		flags: list,
		parseArgs: func(args []string) {

			list.Parse(args)

			config.list.brokers = strings.Split(config.list.args.brokers, ",")
			for i, b := range config.list.brokers {
				if !strings.Contains(b, ":") {
					config.list.brokers[i] = b + ":9092"
				}
			}
		},

		run: func(closer chan struct{}) {

			client, err := sarama.NewClient(config.list.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
				os.Exit(1)
			}
			defer client.Close()

			topics, err := client.Topics()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
				os.Exit(1)
			}

			sort.Strings(topics)

			for _, topic := range topics {
				fmt.Println(topic)
			}
		},
	}
}
