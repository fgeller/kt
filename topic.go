package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

type topicConfig struct {
	flags      *flag.FlagSet
	brokers    []string
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	args       struct {
		brokers    string
		filter     string
		partitions bool
		leaders    bool
		replicas   bool
		verbose    bool
	}
}

type topic struct {
	Name       string      `json:"name"`
	Partitions []partition `json:"partitions,omitempty"`
}

type partition struct {
	Id           int32   `json:"id"`
	OldestOffset int64   `json:"oldest"`
	NewestOffset int64   `json:"newest"`
	Leader       string  `json:"leader,omitempty"`
	Replicas     []int32 `json:"replicas,omitempty"`
}

func topicCommand() command {
	return command{
		flags:     topicFlags(),
		parseArgs: topicParseArgs,
		run:       topicRun,
	}
}

func topicFlags() *flag.FlagSet {
	topic := flag.NewFlagSet("topic", flag.ExitOnError)
	topic.StringVar(&config.topic.args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	topic.BoolVar(&config.topic.args.partitions, "partitions", false, "Include information per partition.")
	topic.BoolVar(&config.topic.args.leaders, "leaders", false, "Include leader information per partition.")
	topic.BoolVar(&config.topic.args.replicas, "replicas", false, "Include replica ids per partition.")
	topic.StringVar(&config.topic.args.filter, "filter", "", "Regex to filter topics by name.")
	topic.BoolVar(&config.topic.args.verbose, "verbose", false, "More verbose logging to stderr.")

	topic.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		topic.PrintDefaults()
		fmt.Fprintln(os.Stderr, `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.
`)
		os.Exit(2)
	}

	return topic
}

func topicParseArgs() {
	envBrokers := os.Getenv("KT_BROKERS")
	if config.topic.args.brokers == "" {
		if envBrokers != "" {
			config.topic.args.brokers = envBrokers
		} else {
			config.topic.args.brokers = "localhost:9092"
		}
	}
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
	config.topic.leaders = config.topic.args.leaders
	config.topic.replicas = config.topic.args.replicas
	config.topic.verbose = config.topic.args.verbose
}

func topicRun(closer chan struct{}) {
	var err error
	if config.topic.verbose {
		sarama.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}

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

	out := make(chan string)
	go func() {
		for {
			select {
			case m := <-out:
				fmt.Println(m)
			}
		}
	}()

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(t string) {
			printTopic(client, t, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func printTopic(client sarama.Client, name string, out chan string) {
	var t topic
	var byts []byte
	var err error

	if t, err = readTopic(client, name); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read info for topic %s. err=%v\n", name, err)
		return
	}

	if byts, err = json.Marshal(t); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal JSON for topic %s. err=%v\n", name, err)
		return
	}

	out <- string(byts)
}

func readTopic(client sarama.Client, name string) (topic, error) {
	t := topic{Name: name}

	if config.topic.partitions {
		ps, err := client.Partitions(name)
		if err != nil {
			return t, err
		}

		for _, p := range ps {
			np := partition{Id: p}

			oldest, err := client.GetOffset(name, p, sarama.OffsetOldest)
			if err != nil {
				return t, err
			}
			np.OldestOffset = oldest

			newest, err := client.GetOffset(name, p, sarama.OffsetNewest)
			if err != nil {
				return t, err
			}
			np.NewestOffset = newest

			if config.topic.leaders {
				b, err := client.Leader(name, p)
				if err != nil {
					return t, err
				}
				np.Leader = b.Addr()
			}

			if config.topic.replicas {
				rs, err := client.Replicas(name, p)
				if err != nil {
					return t, err
				}
				np.Replicas = rs
			}

			t.Partitions = append(t.Partitions, np)
		}
	}

	return t, nil
}
