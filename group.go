package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

type groupCmd struct {
	topic   string
	brokers []string
	group   string
	set     int64
	verbose bool
	version sarama.KafkaVersion

	q chan struct{}
}

type group struct {
	Name string `json:"name"`
}

func (cmd *groupCmd) run(args []string, q chan struct{}) {
	cmd.parseArgs(args)
	cmd.q = q

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	brokers := cmd.findBrokers()
	if len(brokers) == 0 {
		failf("failed to find brokers")
	}

	groups := []group{{cmd.group}}
	if cmd.group == "" {
		groups = cmd.findGroups(brokers)
	}

	for _, g := range groups {
		buf, _ := json.Marshal(g)
		fmt.Println(string(buf))
	}

	// DONE: kt group
	// TODO: kt group -topic events
	// TODO: kt group -topic events -group duper
	// TODO: kt group -topic events -group duper -set 123
}

func (cmd *groupCmd) findGroups(brokers []*sarama.Broker) []group {
	var (
		err    error
		groups = []group{}
		resp   *sarama.ListGroupsResponse
	)
	for _, broker := range brokers {
		if err = cmd.connect(broker); err != nil {
			failf("failed to connect to broker %#v err=%s\n", broker.Addr(), err)
		}

		if resp, err = broker.ListGroups(&sarama.ListGroupsRequest{}); err != nil {
			failf("failed to list brokers on %#v err=%v", broker.Addr(), err)
		}

		if resp.Err != sarama.ErrNoError {
			failf("failed to list brokers on %#v err=%v", broker.Addr(), resp.Err)
		}

		for name := range resp.Groups {
			groups = append(groups, group{name})
		}

	}
	return groups
}

func (cmd *groupCmd) connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	if err := broker.Open(cmd.saramaConfig()); err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}

	return nil
}

func (cmd *groupCmd) findBrokers() []*sarama.Broker {
	var (
		err  error
		meta *sarama.MetadataResponse
	)

loop:
	for _, addr := range cmd.brokers {
		broker := sarama.NewBroker(addr)
		if err = cmd.connect(broker); err != nil {
			fmt.Fprintf(os.Stderr, "failed to open broker connection to %#v err=%s\n", addr, err)
			continue loop
		}

		if meta, err = broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{}}); err != nil {
			fmt.Fprintf(os.Stderr, "failed to request metadata from %#v. err=%s\n", addr, err)
			continue loop
		}

		return meta.Brokers
	}

	return []*sarama.Broker{}
}

func (cmd *groupCmd) saramaConfig() *sarama.Config {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-group-" + sanitizeUsername(usr.Username)

	return cfg
}

func (cmd *groupCmd) failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	failf("use \"kt group -help\" for more information")
}

func (cmd *groupCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

	envTopic := os.Getenv("KT_TOPIC")
	if args.topic == "" {
		args.topic = envTopic
	}

	cmd.topic = args.topic
	cmd.group = args.group
	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)

	switch args.set {
	case "newest":
		cmd.set = sarama.OffsetNewest
	case "oldest":
		cmd.set = sarama.OffsetOldest
	case "":
		// optional flag
	default:
		cmd.set, err = strconv.ParseInt(args.set, 10, 64)
		if err != nil {
			if cmd.verbose {
				fmt.Fprintf(os.Stderr, "failed to parse set %#v err=%v", args.set, err)
			}
			cmd.failStartup(fmt.Sprintf(`set value %#v not valid. either newest, oldest or specific offset expected.`, args.set))
		}
	}

	envBrokers := os.Getenv("KT_BROKERS")
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
}

type groupArgs struct {
	topic   string
	brokers string
	group   string
	set     string
	verbose bool
	version string
}

func (cmd *groupCmd) parseFlags(as []string) groupArgs {
	var args groupArgs
	flags := flag.NewFlagSet("group", flag.ExitOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.group, "group", "", "Consumer group name.")
	flags.StringVar(&args.set, "set", "", "Target offset to set for consumer group (newest, oldest, or specific offset)")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of group:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, groupDocString)
		os.Exit(2)
	}

	_ = flags.Parse(as)
	return args
}

var groupDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

TODO
`
