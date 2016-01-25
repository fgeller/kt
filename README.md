# kt - a Kafka tool

## Example usage:

    $ kt -help
    Usage of kt:
      -brokers string
            Comma separated list of brokers. (default "localhost:9092")
      -offset string
            Colon separated offsets where to start and end reading messages.
      -topic string
            Topic to consume.
    $ kt -topic kt-test
    Partition=0 Offset=0 Key= Value=Hello, World 0
    Partition=0 Offset=1 Key= Value=Hallo, Welt
    Partition=0 Offset=2 Key= Value=Bonjour, monde
    ^C2016/01/26 06:29:19 Received interrupt - shutting down...
    $ kt -topic kt-test -offset 1:
    Partition=0 Offset=1 Key= Value=Hallo, Welt
    Partition=0 Offset=2 Key= Value=Bonjour, monde
    ^C2016/01/26 06:29:29 Received interrupt - shutting down...
    $ kt -topic kt-test -offset 1:1
    Partition=0 Offset=1 Key= Value=Hallo, Welt
    $

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GO15VENDOREXPERIMENT=1 go install github.com/fgeller/kt
