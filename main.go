package main

import (
	"flag"
	"fmt"
	"os"
)

type command interface {
	addFlags(*flag.FlagSet)
	environFlags() map[string]string
	run(args []string) error
}

var usageMessage = `kt is a tool for Kafka.

Usage:

	kt command [arguments]

The commands are:

	consume    consume messages.
	produce    produce messages.
	topic      topic information.
	group      consumer group information and modification.
	admin      basic cluster administration.

Use "kt [command] -help" for for information about the command.

More at https://github.com/fgeller/kt
`

var commands = map[string]command{
	"consume": &consumeCmd{},
	"produce": &produceCmd{},
	"topic":   &topicCmd{},
	"group":   &groupCmd{},
	"admin":   &adminCmd{},
}

var errSilent = fmt.Errorf("silent error; you should not be seeing this")

func main() {
	os.Exit(main1())
}

func main1() int {
	if err := main2(); err != nil {
		if err != errSilent && err != flag.ErrHelp {
			fmt.Fprintf(os.Stderr, "hkt: %v\n", err)
			return 1
		}
	}
	return 0
}

func main2() error {
	c, args, err := parseCmd(os.Args...)
	if err != nil {
		return err
	}
	return c.run(args)
}

func parseCmd(args ...string) (command, []string, error) {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprint(os.Stderr, usageMessage)
	}
	if err := flags.Parse(args[1:]); err != nil {
		return nil, nil, errSilent
	}
	if flags.NArg() < 1 {
		flags.Usage()
		return nil, nil, errSilent
	}
	cmdName := flags.Arg(0)
	c := commands[cmdName]
	if c == nil {
		return nil, nil, fmt.Errorf("unknown command %q; use `hkt -help` for help", cmdName)
	}
	subcmdFlags := flag.NewFlagSet("hkt "+cmdName, flag.ContinueOnError)
	c.addFlags(subcmdFlags)
	if err := subcmdFlags.Parse(flags.Args()[1:]); err != nil {
		return nil, nil, err
	}
	if err := setFlagsFromEnv(subcmdFlags, c.environFlags()); err != nil {
		return nil, nil, err
	}
	return c, subcmdFlags.Args(), nil
}
