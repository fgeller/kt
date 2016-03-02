package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/Shopify/sarama"
)

type topicConfig struct {
	flags      *flag.FlagSet
	brokers    []string
	filter     *regexp.Regexp
	partitions bool
	args       struct {
		brokers    string
		filter     string
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
	return command{
		parseArgs: topicParseArgs,
		run:       topicRun,
	}
}

func init() {
	topic := flag.NewFlagSet("topic", flag.ExitOnError)
	topic.StringVar(&config.topic.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	topic.BoolVar(&config.topic.args.partitions, "partitions", false, "Include detailed partition information.")
	topic.StringVar(&config.topic.args.filter, "filter", "", "Regex to filter topics by name.")

	topic.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		topic.PrintDefaults()
		os.Exit(2)
	}

	config.topic.flags = topic
}

func topicParseArgs(args []string) {
	config.topic.flags.Parse(args)

	config.topic.brokers = strings.Split(config.topic.args.brokers, ",")
	for i, b := range config.topic.brokers {
		if !strings.Contains(b, ":") {
			config.topic.brokers[i] = b + ":9092"
		}
	}

	re, err := regexp.Compile(config.topic.args.filter)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid regex for filter. err=%s", err)
		os.Exit(2)
	}

	config.topic.filter = re
	config.topic.partitions = config.topic.args.partitions
}

func topicRun(closer chan struct{}) {
	var err error

	client, err := sarama.NewClient(config.topic.brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	allTopics, err := client.Topics()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
		os.Exit(1)
	}

	topics := []string{}
	for _, t := range allTopics {
		if config.topic.filter.MatchString(t) {
			topics = append(topics, t)
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
