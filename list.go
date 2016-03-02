package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

type listConfig struct {
	brokers []string
	args    struct {
		brokers string
	}
}

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

			for _, tn := range topics {
				t := topic{Name: tn}

				ps, err := client.Partitions(tn)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %s. err=%v\n", tn, err)
					os.Exit(1)
				}

				for _, p := range ps {
					oldest, err := client.GetOffset(tn, p, sarama.OffsetOldest)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to read oldest offset for partition %s on topic %s. err=%v\n", p, tn, err)
						os.Exit(1)
					}

					newest, err := client.GetOffset(tn, p, sarama.OffsetNewest)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to read newest offset for partition %s on topic %s. err=%v\n", p, tn, err)
						os.Exit(1)
					}

					t.Partitions = append(t.Partitions, partition{Id: p, OldestOffset: oldest, NewestOffset: newest})
				}

				bs, err := json.Marshal(t)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to marshal JSON for topic %s. err=%v\n", tn, err)
					os.Exit(1)
				}
				fmt.Printf("%s\n", bs)
			}
		},
	}
}

type topic struct {
	Name       string      `json:name`
	Partitions []partition `json:partitions`
}

type partition struct {
	Id           int32 `json:id`
	OldestOffset int64 `json:oldestOffset`
	NewestOffset int64 `json:newestOffset`
}
