package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

type topicArgs struct {
	brokers    string
	zookeepers string
	filter     string
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	pretty     bool
	create     bool
	name       string
	version    string
}

type topicCmd struct {
	brokers    []string
	zookeepers []string
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	verbose    bool
	pretty     bool
	create     bool
	name       string
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
	flags.StringVar(&args.zookeepers, "zookeepers", "", "Comma separated list of zookeeper nodes. Defaults to brokers and port 2181 when omitted.")
	flags.BoolVar(&args.partitions, "partitions", false, "Include information per partition.")
	flags.BoolVar(&args.leaders, "leaders", false, "Include leader information per partition.")
	flags.BoolVar(&args.replicas, "replicas", false, "Include replica ids per partition.")
	flags.StringVar(&args.filter, "filter", "", "Regex to filter topics by name.")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&args.create, "create", false, "Create the specified topic.")
	flags.StringVar(&args.name, "name", "", "Exact name of the topic, required for topic management.")
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

	if re, err = regexp.Compile(args.filter); err != nil {
		failf("invalid regex for filter err=%s", err)
	}

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

func (cmd *topicCmd) createTopic() {
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
		go func() {
			for {
				fmt.Fprintf(os.Stderr, "received zookeeper event %#v", <-evs)
			}
		}()
	}

	var (
		rawBrokerIDs []string
		brokers      []broker
	)

	if rawBrokerIDs, _, err = conn.Children(zkPathBrokersIDs); err != nil {
		failf("failed to retrieve broker ids from zookeeper err=%v", err)
	}

	fmt.Printf("found broker ids %#v\n", rawBrokerIDs)
	for _, rawID := range rawBrokerIDs {
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

	fmt.Printf("found brokers %#v\n", brokers)
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

func (cmd *topicCmd) run(as []string, q chan struct{}) {
	var (
		err error
		all []string
		out = make(chan printContext)
	)

	go func() { <-q; failf("received quit signal.") }()

	cmd.parseArgs(as)
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.create {
		cmd.createTopic()
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
