package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
)

type adminCmd struct {
	brokers []string
	verbose bool
	version sarama.KafkaVersion

	client sarama.Client
}

type adminArgs struct {
	brokers string
	verbose bool
	version string
}

// kt admin -createtopic name -topiccfg path
func (cmd *adminCmd) parseArgs(as []string) {
	var (
		args = cmd.parseFlags(as)
	)

	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)

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

func (cmd *adminCmd) run(args []string) {
	var err error

	cmd.parseArgs(args)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cmd.saramaConfig()); err != nil {
		failf("failed to create client err=%v", err)
	}

	brokers := cmd.client.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))

	spew.Printf("hello there TODO\n")

}

func (cmd *adminCmd) saramaConfig() *sarama.Config {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-admin-" + sanitizeUsername(usr.Username)

	return cfg
}

func (cmd *adminCmd) parseFlags(as []string) adminArgs {
	var args adminArgs
	flags := flag.NewFlagSet("consume", flag.ExitOnError)
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of admin:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}
