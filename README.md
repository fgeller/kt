# kt - a Kafka tool that likes JSON [![Build Status](https://travis-ci.org/fgeller/kt.svg?branch=master)](https://travis-ci.org/fgeller/kt)

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Display topic information (e.g., with partition offset and leader info).
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition).
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers and topic via environment variables `KT_BROKERS` and `KT_TOPIC` for a shell session.
* Fast start up time.
* No buffering of output.
* Binary keys and payloads can be passed and presented in base64 or hex encoding.
* Support for TLS authentication.
* Basic cluster admin functions: Create & delete topics.

## Examples

<details><summary>Read details about topics that match a regex</summary>

```sh
$ kt topic -filter news -partitions
{
  "name": "actor-news",
  "partitions": [
    {
      "id": 0,
      "oldest": 0,
      "newest": 0
    }
  ]
}
```
</details>

<details><summary>Produce messages</summary>

```sh
$ echo 'Alice wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ echo 'Bob wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ for i in {6..9} ; do echo Bourne sequel $i in production. | kt produce -topic actor-news -literal ;done
{
  "count": 1,
  "partition": 0,
  "startOffset": 1
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 2
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 3
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 4
}
```
</details>

<details><summary>Or pass in JSON object to control key, value and partition</summary>

```sh
$ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | kt produce -topic actor-news
{
  "count": 1,
  "partition": 0,
  "startOffset": 5
}
```
</details>

<details><summary>Read messages at specific offsets on specific partitions</summary>

```sh
$ kt consume -topic actor-news -offsets 0=1:2
{
  "partition": 0,
  "offset": 1,
  "key": "",
  "value": "Bourne sequel 6 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 2,
  "key": "",
  "value": "Bourne sequel 7 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
```
</details>

<details><summary>Follow a topic, starting relative to newest offset</summary>

```sh
$ kt consume -topic actor-news -offsets all=newest-1:
{
  "partition": 0,
  "offset": 4,
  "key": "",
  "value": "Bourne sequel 9 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 5,
  "key": "Arni",
  "value": "Terminator terminated",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
^Creceived interrupt - shutting down
shutting down partition consumer for partition 0
```
</details>

<details><summary>View offsets for a given consumer group</summary>

```sh
$ kt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 6,
      "lag": 0
    }
  ]
}
```
</details>

<details><summary>Change consumer group offset</summary>

```sh
$ kt group -group enews -topic actor-news -partitions 0 -reset 1
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
$ kt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
```
</details>

<details><summary>Create and delete a topic</summary>

```sh
$ kt admin -createtopic morenews -topicdetail <(jsonify =NumPartitions 1 =ReplicationFactor 1)
$ kt topic -filter news
{
  "name": "morenews"
}
$ kt admin -deletetopic morenews
$ kt topic -filter news
```

</details>

<details><summary>Change broker address via environment variable</summary>

```sh
$ export KT_BROKERS=brokers.kafka:9092
$ kt <command> <option>
```

</details>

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ go get -u github.com/fgeller/kt

Or via Homebrew on OSX:

    $ brew tap fgeller/tap
    $ brew install kt

### Docker

[@Paxa](https://github.com/Paxa) maintains an image to run kt in a Docker environment - thanks!

For more information: [https://github.com/Paxa/kt](https://github.com/Paxa/kt)

## Usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            group          consumer group information and modification.
            admin          basic cluster administration.

    Use "kt [command] -help" for for information about the command.

    Authorization:

    Authorization with Kafka can be configured via a JSON file.
    You can set the file name via an "-auth" flag to each command or
    set it via the environment variable KT_AUTH.


## Authorization

Currently the only supported mode is TLS, please create an
[https://github.com/fgeller/kt/issues/new](issue) with details in case you need
another mode. Example for an authorization configuration that is used for the
system tests:


    {
        "mode": "TLS",
        "client-certificate": "test-secrets/kt-test.crt",
        "client-certificate-key": "test-secrets/kt-test.key",
        "ca-certificate": "test-secrets/snakeoil-ca-1.crt"
    }
