package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)

var buildVersion, buildTime string

var config struct {
	consume consumeConfig
	produce produceConfig
	topic   topicConfig
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
	parseArgs func()
	run       func(chan struct{})
}

func init() {
	if len(buildTime) > 0 && len(buildVersion) > 0 {
		usageMessage = fmt.Sprintf(`%v
Build %v from %v.`, usageMessage, buildVersion, buildTime)
	}
}

var usageMessage = `kt is a tool for Kafka.

Usage:

	kt command [arguments]

The commands are:

	consume        consume messages.
	produce        produce messages.
	topic          topic information.

Use "kt [command] -help" for for information about the command.

More at https://github.com/fgeller/kt`

func usage() {
	fmt.Fprintln(os.Stderr, usageMessage)
	os.Exit(2)
}

func parseArgs() command {
	if len(os.Args) < 2 {
		usage()
	}

	commands := map[string]command{
		"consume": consumeCommand(),
		"produce": produceCommand(),
		"topic":   topicCommand(),
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
	}

	cmd.flags.Parse(os.Args[2:])
	cmd.parseArgs()

	return cmd
}

func main() {
	cmd := parseArgs()
	closer := listenForInterrupt()
	cmd.run(closer)
}
