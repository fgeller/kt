package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

type topicArgs struct {
	brokers    string
	filter     string
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	version    string
}

type topicCmd struct {
	brokers    []string
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	version    sarama.KafkaVersion

	client sarama.Client
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

func (t *topicCmd) parseFlags(as []string) topicArgs {
	var (
		args  topicArgs
		flags = flag.NewFlagSet("topic", flag.ExitOnError)
	)

	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.BoolVar(&args.partitions, "partitions", false, "Include information per partition.")
	flags.BoolVar(&args.leaders, "leaders", false, "Include leader information per partition.")
	flags.BoolVar(&args.replicas, "replicas", false, "Include replica ids per partition.")
	flags.StringVar(&args.filter, "filter", "", "Regex to filter topics by name.")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.
`)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

func (t *topicCmd) parseArgs(as []string) {
	var (
		err error
		re  *regexp.Regexp

		args       = t.parseFlags(as)
		envBrokers = os.Getenv("KT_BROKERS")
	)
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	t.brokers = strings.Split(args.brokers, ",")
	for i, b := range t.brokers {
		if !strings.Contains(b, ":") {
			t.brokers[i] = b + ":9092"
		}
	}

	if re, err = regexp.Compile(args.filter); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid regex for filter. err=%s\n", err)
		os.Exit(2)
	}

	t.filter = re
	t.partitions = args.partitions
	t.leaders = args.leaders
	t.replicas = args.replicas
	t.verbose = args.verbose
	t.version = kafkaVersion(args.version)
}

func (t *topicCmd) mkClient() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = t.version

	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-topic-" + usr.Username
	if t.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	if t.client, err = sarama.NewClient(t.brokers, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}
}

func (t *topicCmd) run(as []string, closer chan struct{}) {
	var (
		err error
		all []string
		out = make(chan printContext)
	)

	if t.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	t.mkClient()
	defer t.client.Close()

	if all, err = t.client.Topics(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
		os.Exit(1)
	}

	topics := []string{}
	for _, a := range all {
		if t.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	go print(out)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			t.print(top, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (t *topicCmd) print(name string, out chan printContext) {
	var (
		top topic
		buf []byte
		err error
	)

	if top, err = t.readTopic(name); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read info for topic %s. err=%v\n", name, err)
		return
	}

	if buf, err = json.Marshal(top); err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal JSON for topic %s. err=%v\n", name, err)
		return
	}

	ctx := printContext{string(buf), make(chan struct{})}
	out <- ctx
	<-ctx.done
}

func (t *topicCmd) readTopic(name string) (topic, error) {
	var (
		err error
		ps  []int32
		led *sarama.Broker
		top = topic{Name: name}
	)

	if !t.partitions {
		return top, nil
	}

	if ps, err = t.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{Id: p}

		if np.OldestOffset, err = t.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = t.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if t.leaders {
			if led, err = t.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if t.replicas {
			if np.Replicas, err = t.client.Replicas(name, p); err != nil {
				return top, err
			}
		}

		top.Partitions = append(top.Partitions, np)
	}

	return top, nil
}
