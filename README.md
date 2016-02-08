# kt - a Kafka tool

Some reasons why you might be interested:

* Fast start up time.
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/)
* Consume messages between specific offsets.
* No buffering of output.

## Example usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.

    Use "kt [command] -help" for for information about the command.


    $ kt consume
    Usage of consume:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -json
            Print output in JSON format.
      -offsets string
            Colon separated offsets where to start and end reading messages.
      -timeout duration
            Timeout after not reading messages (default 0 to disable).
      -topic string
            Topic to consume.

    $ kt consume -topic kt-test
    Partition=0 Offset=3 Key= Message=Hello, World 0
    Partition=0 Offset=4 Key= Message=Hello, World.
    Partition=0 Offset=5 Key= Message=Hallo, Welt.
    Partition=0 Offset=6 Key= Message=Bonjour, monde.
    ^C2016/02/08 19:17:40 Received interrupt - shutting down...

    $ kt consume -topic kt-test -timeout 1s
    Partition=0 Offset=3 Key= Message=Hello, World 0
    Partition=0 Offset=4 Key= Message=Hello, World.
    Partition=0 Offset=5 Key= Message=Hallo, Welt.
    Partition=0 Offset=6 Key= Message=Bonjour, monde.
    2016/02/08 19:18:08 Consuming from partition [0] timed out.

    $ kt consume -topic kt-test -timeout 50ms -offsets 4:
    Partition=0 Offset=4 Key= Message=Hello, World.
    Partition=0 Offset=5 Key= Message=Hallo, Welt.
    Partition=0 Offset=6 Key= Message=Bonjour, monde.
    2016/02/08 19:19:12 Consuming from partition [0] timed out.

    $ kt consume -topic kt-test -timeout 50ms -offsets 4:4
    Partition=0 Offset=4 Key= Message=Hello, World.

    $ kt consume -topic kt-test -timeout 50ms -offsets 4: -json
    {"partition":0,"offset":4,"key":"","message":"Hello, World."}
    {"partition":0,"offset":5,"key":"","message":"Hallo, Welt."}
    {"partition":0,"offset":6,"key":"","message":"Bonjour, monde."}
    2016/02/08 19:19:52 Consuming from partition [0] timed out.

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GO15VENDOREXPERIMENT=1 go install github.com/fgeller/kt
