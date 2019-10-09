package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func listenForInterrupt(q chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	sig := <-signals
	fmt.Fprintf(os.Stderr, "received signal %s\n", sig)
	close(q)
}

var defaultKafkaVersion = sarama.V2_0_0_0

type commonFlags struct {
	verbose    bool
	brokers    []string
	version    sarama.KafkaVersion
	tlsCA      string
	tlsCert    string
	tlsCertKey string
}

func (f *commonFlags) addFlags(flags *flag.FlagSet) {
	f.brokers = []string{"localhost:9092"}
	f.version = defaultKafkaVersion
	flags.Var(listFlag{&f.brokers}, "brokers", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.Var(kafkaVersionFlag{v: &f.version}, "version", "Kafka protocol version")
	flags.StringVar(&f.tlsCA, "tlsca", "", "Path to the TLS certificate authority file")
	flags.StringVar(&f.tlsCert, "tlscert", "", "Path to the TLS client certificate file")
	flags.StringVar(&f.tlsCertKey, "tlscertkey", "", "Path to the TLS client certificate key file")
	flags.BoolVar(&f.verbose, "verbose", false, "More verbose logging to stderr.")
}

func (f *commonFlags) saramaConfig(name string) (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Version = f.version
	usr, err := user.Current()
	var username string
	if err != nil {
		warningf("failed to read current user name: %v", err)
		username = "anon"
	} else {
		username = usr.Username
	}
	cfg.ClientID = "kt-" + name + "-" + sanitizeUsername(username)

	tlsConfig, err := setUpCerts(f.tlsCert, f.tlsCA, f.tlsCertKey)
	if err != nil {
		return nil, fmt.Errorf("cannot set up certificates: %v", err)
	}
	if tlsConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsConfig
	}
	if f.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}
	return cfg, nil
}

type listFlag struct {
	v *[]string
}

func (v listFlag) String() string {
	if v.v == nil {
		return ""
	}
	return strings.Join(*v.v, ",")
}

func (v listFlag) Set(s string) error {
	if s == "" {
		*v.v = nil
	} else {
		*v.v = strings.Split(s, ",")
	}
	return nil
}

type kafkaVersionFlag struct {
	v *sarama.KafkaVersion
}

func (v kafkaVersionFlag) String() string {
	if v.v == nil {
		return ""
	}
	return v.v.String()
}

func (v kafkaVersionFlag) Set(s string) error {
	vers, err := sarama.ParseKafkaVersion(strings.TrimPrefix(s, "v"))
	if err != nil {
		return fmt.Errorf("invalid kafka version %q: %v", s, err)
	}
	*v.v = vers
	return nil
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		warningf("failed to close %#v: %v", name, err)
	}
}

type printer struct {
	mu      sync.Mutex
	marshal func(interface{}) ([]byte, error)
}

func newPrinter(pretty bool) *printer {
	marshal := json.Marshal
	if pretty && terminal.IsTerminal(1) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}
	return &printer{
		marshal: marshal,
	}
}

func (p *printer) print(val interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf, err := p.marshal(val)
	if err != nil {
		warningf("failed to marshal output %#v: %v", val, err)
	}
	fmt.Println(string(buf))
}

func warningf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "hkt: warning: %s\n", fmt.Sprintf(f, a...))
}

func readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(nil, max)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		warningf("error reading standard input: %v", err)
	}
	close(out)
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

// setUpCerts takes the paths to a tls certificate, CA, and certificate key in
// a PEM format and returns a constructed tls.Config object.
func setUpCerts(certPath, caPath, keyPath string) (*tls.Config, error) {
	if certPath == "" && caPath == "" && keyPath == "" {
		return nil, nil
	}

	if certPath == "" || caPath == "" || keyPath == "" {
		return nil, fmt.Errorf("certificate, CA and key path are required - got cert=%#v ca=%#v key=%#v", certPath, caPath, keyPath)
	}

	caString, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		return nil, fmt.Errorf("unable to add cert at %s to certificate pool", caPath)
	}

	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	bundle := &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	}
	bundle.BuildNameToCertificate()
	return bundle, nil
}

// setFlagsFromEnv sets unset flags in fs from environment
// variables as specified by the flags map, which maps
// from flag name to the environment variable for that name.
//
// If a flag f is part of fs but has not been explicitly set on the
// command line, and flags[f] exists, then it will
// be set from os.Getenv(flags[f]).
func setFlagsFromEnv(fs *flag.FlagSet, flags map[string]string) error {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = true
	})
	for name, env := range flags {
		f := fs.Lookup(name)
		if f == nil {
			panic(fmt.Errorf("flag %q ($%s) not found", name, env))
		}
		if set[name] {
			continue
		}
		if v := os.Getenv(env); v != "" {
			if err := f.Value.Set(v); err != nil {
				return fmt.Errorf("cannot parse $%s as -%s flag value: %v", env, name, err)
			}
		}
	}
	return nil
}

func decoderForType(typ string) (func(m json.RawMessage) ([]byte, error), error) {
	var dec func(s string) ([]byte, error)
	switch typ {
	case "json":
		// Easy case - we already have the JSON-marshaled data.
		return func(m json.RawMessage) ([]byte, error) {
			return m, nil
		}, nil
	case "hex":
		dec = hex.DecodeString
	case "base64":
		dec = base64.StdEncoding.DecodeString
	case "string":
		dec = func(s string) ([]byte, error) {
			return []byte(s), nil
		}
	default:
		return nil, fmt.Errorf(`unsupported decoder %#v, only json, string, hex and base64 are supported`, typ)
	}
	return func(m json.RawMessage) ([]byte, error) {
		var s string
		if err := json.Unmarshal(m, &s); err != nil {
			return nil, err
		}
		return dec(s)
	}, nil
}

var nullJSON = json.RawMessage("null")

func encoderForType(typ string) (func([]byte) (json.RawMessage, error), error) {
	var enc func([]byte) string
	switch typ {
	case "json":
		return func(data []byte) (json.RawMessage, error) {
			var j json.RawMessage
			if err := json.Unmarshal(data, &j); err != nil {
				return nil, fmt.Errorf("invalid JSON value %q: %v", data, err)
			}
			return json.RawMessage(data), nil
		}, nil
	case "hex":
		enc = hex.EncodeToString
	case "base64":
		enc = base64.StdEncoding.EncodeToString
	case "string":
		enc = func(data []byte) string {
			return string(data)
		}
	default:
		return nil, fmt.Errorf(`unsupported decoder %#v, only json, string, hex and base64 are supported`, typ)
	}
	return func(data []byte) (json.RawMessage, error) {
		if data == nil {
			return nullJSON, nil
		}
		data1, err := json.Marshal(enc(data))
		if err != nil {
			// marshaling a string cannot fail but be defensive.
			return nil, err
		}
		return json.RawMessage(data1), nil
	}, nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
