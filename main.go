package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

type consumerConfig struct {
	topic       string
	brokers     []string
	startOffset int64
	endOffset   int64
	json        bool
	timeout     time.Duration
	args        struct {
		topic   string
		brokers string
		timeout time.Duration
		offsets string
		json    bool
	}
}

type listConfig struct {
	brokers []string
	args    struct {
		brokers string
	}
}

var config struct {
	consume consumerConfig
	list    listConfig
}

func listenForInterrupt() chan struct{} {
	closer := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Printf("Received interrupt - shutting down...")
		close(closer)
	}()

	return closer
}

func print(msg *sarama.ConsumerMessage) {

	if config.consume.json {
		fmt.Printf(
			`{"partition":%v,"offset":%v,"key":%#v,"message":%#v}
`,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
		)

		return
	}

	fmt.Printf(
		"Partition=%v Offset=%v Key=%s Message=%s\n",
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}

type command struct {
	flags     *flag.FlagSet
	parseArgs func([]string)
	run       func(chan struct{})
}

var usageMessage = `kt is a tool for Kafka.

Usage:

	kt command [arguments]

The commands are:

	consume        consume messages.
	list           list topics.

Use "kt [command] -help" for for information about the command.

`

func usage() {
	fmt.Fprintln(os.Stderr, usageMessage)
	os.Exit(2)
}

func parseArgs() command {
	if len(os.Args) < 2 {
		usage()
	}

	commands := map[string]command{
		"consume": consumerCommand(),
		"list":    listCommand(),
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
	}

	cmd.parseArgs(os.Args[2:])

	return cmd
}

func main() {
	cmd := parseArgs()
	closer := listenForInterrupt()
	cmd.run(closer)
}
