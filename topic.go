package main

import (
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
	tlsCA      string
	tlsCert    string
	tlsCertKey string
	filter     string
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	pretty     bool
	version    string
}

type topicCmd struct {
	brokers    []string
	tlsCA      string
	tlsCert    string
	tlsCertKey string
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	pretty     bool
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
	ISRs         []int32 `json:"isrs,omitempty"`
}

func (cmd *topicCmd) parseFlags(as []string) topicArgs {
	var (
		args  topicArgs
		flags = flag.NewFlagSet("topic", flag.ExitOnError)
	)

	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.StringVar(&args.tlsCA, "tlsca", "", "Path to the tls certificate authority file")
	flags.StringVar(&args.tlsCert, "tlscert", "", "Path to the tls client certificate file")
	flags.StringVar(&args.tlsCertKey, "tlscertkey", "", "Path to the tls client certificate key file")
	flags.BoolVar(&args.partitions, "partitions", false, "Include information per partition.")
	flags.BoolVar(&args.leaders, "leaders", false, "Include leader information per partition.")
	flags.BoolVar(&args.replicas, "replicas", false, "Include replica ids per partition.")
	flags.StringVar(&args.filter, "filter", "", "Regex to filter topics by name.")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, topicDocString)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

func (cmd *topicCmd) parseArgs(as []string) {
	var (
		err error
		re  *regexp.Regexp

		args       = cmd.parseFlags(as)
		envBrokers = os.Getenv("KT_BROKERS")
	)
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}

	if re, err = regexp.Compile(args.filter); err != nil {
		failf("invalid regex for filter err=%s", err)
	}

	cmd.tlsCA = args.tlsCA
	cmd.tlsCert = args.tlsCert
	cmd.tlsCertKey = args.tlsCertKey
	cmd.filter = re
	cmd.partitions = args.partitions
	cmd.leaders = args.leaders
	cmd.replicas = args.replicas
	cmd.pretty = args.pretty
	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)
}

func (cmd *topicCmd) connect() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version

	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-topic-" + sanitizeUsername(usr.Username)
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	tlsConfig, err := setupCerts(cmd.tlsCert, cmd.tlsCA, cmd.tlsCertKey)
	if err != nil {
		failf("failed to setup certificates err=%v", err)
	}
	if tlsConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsConfig
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
}

func (cmd *topicCmd) run(as []string) {
	var (
		err error
		all []string
		out = make(chan printContext)
	)

	cmd.parseArgs(as)
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.connect()
	defer cmd.client.Close()

	if all, err = cmd.client.Topics(); err != nil {
		failf("failed to read topics err=%v", err)
	}

	topics := []string{}
	for _, a := range all {
		if cmd.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	go print(out, cmd.pretty)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			cmd.print(top, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (cmd *topicCmd) print(name string, out chan printContext) {
	var (
		top topic
		err error
	)

	if top, err = cmd.readTopic(name); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read info for topic %s. err=%v\n", name, err)
		return
	}

	ctx := printContext{output: top, done: make(chan struct{})}
	out <- ctx
	<-ctx.done
}

func (cmd *topicCmd) readTopic(name string) (topic, error) {
	var (
		err error
		ps  []int32
		led *sarama.Broker
		top = topic{Name: name}
	)

	if !cmd.partitions {
		return top, nil
	}

	if ps, err = cmd.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{Id: p}

		if np.OldestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if cmd.leaders {
			if led, err = cmd.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if cmd.replicas {
			if np.Replicas, err = cmd.client.Replicas(name, p); err != nil {
				return top, err
			}

			if np.ISRs, err = cmd.client.InSyncReplicas(name, p); err != nil {
				return top, err
			}
		}

		top.Partitions = append(top.Partitions, np)
	}

	return top, nil
}

var topicDocString = `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.`
