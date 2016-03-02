package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

type topicConfig struct {
	brokers    []string
	name       string
	list       bool
	partitions bool
	args       struct {
		brokers    string
		name       string
		list       bool
		partitions bool
	}
}

type topic struct {
	Name       string      `json:"name"`
	Partitions []partition `json:"partitions,omitempty"`
}

type partition struct {
	Id           int32 `json:"id"`
	OldestOffset int64 `json:"oldestOffset"`
	NewestOffset int64 `json:"newestOffset"`
}

func topicCommand() command {
	topic := flag.NewFlagSet("topic", flag.ExitOnError)
	topic.StringVar(&config.topic.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	topic.BoolVar(&config.topic.args.partitions, "partitions", false, "Include detailed partition information.")
	topic.BoolVar(&config.topic.args.list, "list", false, "List all topics.")
	topic.StringVar(&config.topic.args.name, "name", "", "Name of specific topic to show information about (ignored when -list is specified).")

	topic.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		topic.PrintDefaults()
		os.Exit(2)
	}

	return command{
		flags: topic,
		parseArgs: func(args []string) {

			topic.Parse(args)

			config.topic.brokers = strings.Split(config.topic.args.brokers, ",")
			for i, b := range config.topic.brokers {
				if !strings.Contains(b, ":") {
					config.topic.brokers[i] = b + ":9092"
				}
			}

			if !config.topic.args.list && config.topic.args.name == "" {
				fmt.Fprintln(os.Stderr, "Either -list or -name need to be specified.")
				fmt.Fprintln(os.Stderr, "Use \"kt topic -help\" for more information.")
				os.Exit(1)
			}

			config.topic.list = config.topic.args.list
			config.topic.partitions = config.topic.args.partitions
			config.topic.name = config.topic.args.name
		},

		run: func(closer chan struct{}) {
			var err error

			client, err := sarama.NewClient(config.topic.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
				os.Exit(1)
			}
			defer client.Close()

			topics := []string{config.topic.name}
			if config.topic.list {
				topics, err = client.Topics()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
					os.Exit(1)
				}
			}

			for _, tn := range topics {
				t, err := readTopic(client, tn)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to read info for topic %s. err=%v\n", tn, err)
					os.Exit(1)
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

func readTopic(client sarama.Client, name string) (topic, error) {
	t := topic{Name: name}

	if config.topic.partitions {
		ps, err := client.Partitions(name)
		if err != nil {
			return t, err
		}

		for _, p := range ps {
			oldest, err := client.GetOffset(name, p, sarama.OffsetOldest)
			if err != nil {
				return t, err
			}

			newest, err := client.GetOffset(name, p, sarama.OffsetNewest)
			if err != nil {
				return t, err
			}

			t.Partitions = append(t.Partitions, partition{Id: p, OldestOffset: oldest, NewestOffset: newest})
		}
	}

	return t, nil
}
