package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

// TODO kt zk -create -name
// TODO kt zk topic create

type zkArgs struct {
	brokers    string
	zookeepers string
	partitions int
	replicas   int
	verbose    bool
	pretty     bool
	create     bool
	name       string
	version    string
}

type zkCmd struct {
	brokers    []string
	zookeepers []string
	partitions int
	replicas   int
	verbose    bool
	pretty     bool
	create     bool
	name       string
	version    sarama.KafkaVersion

	client sarama.Client
}

func (cmd *zkCmd) parseFlags(as []string) zkArgs {
	var (
		args  zkArgs
		flags = flag.NewFlagSet("topic", flag.ExitOnError)
	)

	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.StringVar(&args.zookeepers, "zookeepers", "", "Comma separated list of zookeeper nodes. Defaults to brokers and port 2181 when omitted.")
	flags.IntVar(&args.partitions, "partitions", 1, "")
	flags.IntVar(&args.replicas, "replicas", 1, "")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&args.create, "create", false, "Create the specified topic.")
	flags.StringVar(&args.name, "name", "", "Exact name of the topic, required for topic management.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of zk:")
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

func (cmd *zkCmd) parseArgs(as []string) {
	var (
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
	cmd.zookeepers = []string{}
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
		if args.zookeepers == "" {
			if strings.Contains(b, ":") {
				cmd.zookeepers = append(cmd.zookeepers, strings.Split(b, ":")[0]+":2181")
			} else {
				cmd.zookeepers = append(cmd.zookeepers, b+":2181")
			}
		}
	}

	// TODO store zookeepers when supplied

	if args.create {
		if args.name == "" {
			failf("name required when creating a topic")
		}
		if len(cmd.zookeepers) == 0 {
			failf("zookeepers required when creating a topic")
		}
	}
	cmd.create = args.create
	cmd.name = args.name

	cmd.partitions = args.partitions
	cmd.replicas = args.replicas
	cmd.pretty = args.pretty
	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)
}

func (cmd *zkCmd) connect() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version

	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-zk-" + sanitizeUsername(usr.Username)
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
}

var (
	zkPathBrokers    = "/brokers"
	zkPathBrokersIDs = fmt.Sprintf("%s/ids", zkPathBrokers)
)

type broker struct {
	id        int
	endpoints []endpoint
	rack      string
}

type endpoint struct {
	host             string
	port             string
	listener         string
	securityProtocol string

	raw string
}

type brokerInfo struct {
	ListenerSecurityProtocolMap map[string]string `json:"listener_security_protocol_map"`
	Endpoints                   []string          `json:"endpoints"`
	JMXPort                     int               `json:"jmx_port"`
	Host                        string            `json:"host"`
	Port                        int               `json:"port"`
	Rack                        string            `json:"rack"`
	Timestamp                   string            `json:"timestamp"`
	Version                     int               `json:"version"`
}

func printZookeeperEvents(evs <-chan zk.Event) {
	for {
		fmt.Fprintf(os.Stderr, "received zookeeper event %#v", <-evs)
	}
}

func (cmd *zkCmd) createTopic() {
	// TODO replication factor
	// TODO partitions
	fmt.Printf("should create topic %#v connecting to %#v\n", cmd.name, cmd.zookeepers)
	var (
		conn *zk.Conn
		evs  <-chan zk.Event
		err  error
	)

	// TODO quiet mode for connect?
	if conn, evs, err = zk.Connect(cmd.zookeepers, 15*time.Second); err != nil {
		failf("failed to establish zookeeper connection err=%v", err)
	}

	if cmd.verbose {
		go printZookeeperEvents(evs)
	}

	brokers := readBrokers(conn)
	fmt.Printf("found brokers %#v\n", brokers)
}

func readBrokers(conn *zk.Conn) []broker {
	var (
		rawIDs  []string
		brokers []broker
		err     error
	)

	if rawIDs, _, err = conn.Children(zkPathBrokersIDs); err != nil {
		failf("failed to retrieve broker ids from zookeeper err=%v", err)
	}

	fmt.Printf("found broker ids %#v\n", rawIDs)
	for _, rawID := range rawIDs {
		id, err := strconv.Atoi(rawID)
		if err != nil {
			failf("failed to convert broker id %#v to int err=%v", rawID, err)
		}

		buf, _, err := conn.Get(fmt.Sprintf("%s/%v", zkPathBrokersIDs, id))
		if err != nil {
			failf("failed to read info for broker %#v err=%v", id, err)
		}

		brokers = append(brokers, newBroker(id, buf))
	}

	return brokers
}

func newBroker(id int, info []byte) broker {
	var (
		uInfo brokerInfo
		err   error
	)

	if err = json.Unmarshal(info, &uInfo); err != nil {
		failf("failed to unmarshal broker info %s err=%v", info, err)
	}

	if uInfo.Version != 4 {
		failf("unsupported broker info version %v, only 4 is supported", uInfo.Version)
	}

	// TODO newEndpoint
	endpoints := []endpoint{}
	for _, rawEP := range uInfo.Endpoints {
		u, err := url.Parse(rawEP)
		if err != nil {
			failf("failed to parse endpoint uri %#v err=%v", rawEP, err)
		}

		lstn := strings.ToUpper(u.Scheme)
		proto, ok := uInfo.ListenerSecurityProtocolMap[lstn]
		if !ok {
			fmt.Printf("found no matching security protocol for %#v in %#v\n", lstn, uInfo.ListenerSecurityProtocolMap)
		}

		ep := endpoint{u.Hostname(), u.Port(), lstn, proto, rawEP}
		endpoints = append(endpoints, ep)
	}

	return broker{id, endpoints, uInfo.Rack}
}

func (cmd *zkCmd) run(as []string, q chan struct{}) {
	go func() { <-q; failf("received quit signal.") }()

	cmd.parseArgs(as)
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.create {
		cmd.createTopic()
	}
}
