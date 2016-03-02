# kt - a Kafka tool

Some reasons why you might be interested:

* Fast start up time.
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/)
* Consume messages between specific offsets.
* No buffering of output.
* Display topic information (e.g., offsets per partitions)

## Example usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.

    Use "kt [command] -help" for for information about the command.

### consume

    $ kt consume -help
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

### produce

    $ kt produce -help
    Usage of produce:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -topic string
            Topic to produce to.

    $ kt produce -topic test
    Hello, world.
    Sent message to partition 0 at offset 3.
    ^C2016/03/02 22:51:47 Received interrupt - shutting down...

    $ echo "Hallo, Welt" | kt produce -topic test
    Sent message to partition 0 at offset 4.

    $ kt consume -topic test
    Partition=0 Offset=3 Key= Message=Hello, world.
    Partition=0 Offset=4 Key= Message=Hallo, Welt
    ^C2016/03/02 22:52:24 Received interrupt - shutting down...

### topic

    $ kt topic -help
    Usage of topic:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -filter string
            Regex to filter topics by name.
      -leaders
            Include leader information per partition.
      -partitions
            Include information per partition.
      -replicas
            Include replica ids per partition.

    $ kt topic
    {"name":"__consumer_offsets"}
    {"name":"kt-test"}
    {"name":"test"}

    $ kt topic -filter test
    {"name":"kt-test"}
    {"name":"test"}

    $ kt topic -filter test -partitions
    {"name":"test","partitions":[{"id":0,"oldestOffset":3,"newestOffset":5}]}
    {"name":"kt-test","partitions":[{"id":0,"oldestOffset":7,"newestOffset":37}]}

    $ kt topic -filter test -partitions -replicas -leaders
    {"name":"kt-test","partitions":[{"id":0,"oldestOffset":7,"newestOffset":37,"leader":"bert:9092","replicas":[0]}]}
    {"name":"test","partitions":[{"id":0,"oldestOffset":3,"newestOffset":5,"leader":"bert:9092","replicas":[0]}]}

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GO15VENDOREXPERIMENT=1 go get github.com/fgeller/kt
    $ GO15VENDOREXPERIMENT=1 go install github.com/fgeller/kt
