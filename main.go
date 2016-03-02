package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"
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

type produceConfig struct {
	topic   string
	brokers []string
	args    struct {
		topic   string
		brokers string
	}
}

var config struct {
	consume consumerConfig
	produce produceConfig
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
	produce        produce messages.
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
		"produce": produceCommand(),
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
