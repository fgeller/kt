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
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

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

func kafkaVersionFlagVar(fs *flag.FlagSet, vp *sarama.KafkaVersion) {
	*vp = defaultKafkaVersion
	fs.Var(kafkaVersionFlag{
		v: vp,
	}, "version", "Kafka protocol version")
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
		fmt.Fprintf(os.Stderr, "failed to close %#v err=%v", name, err)
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
		failf("failed to marshal output %#v, err=%v", val, err)
	}
	fmt.Println(string(buf))
}

func quitf(msg string, args ...interface{}) {
	exitf(0, msg, args...)
}

func failf(msg string, args ...interface{}) {
	exitf(1, msg, args...)
}

func exitf(code int, msg string, args ...interface{}) {
	if code == 0 {
		fmt.Fprintf(os.Stdout, msg+"\n", args...)
	} else {
		fmt.Fprintf(os.Stderr, msg+"\n", args...)
	}
	os.Exit(code)
}

func readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, max), max)

	for scanner.Scan() {
		out <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanning input failed err=%v\n", err)
	}
	close(out)
}

// hashCode imitates the behavior of the JDK's String#hashCode method.
// https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#hashCode()
//
// As strings are encoded in utf16 on the JVM, this implementation checks wether
// s contains non-bmp runes and uses utf16 surrogate pairs for those.
func hashCode(s string) (hc int32) {
	for _, r := range s {
		r1, r2 := utf16.EncodeRune(r)
		if r1 == 0xfffd && r1 == r2 {
			hc = hc*31 + r
		} else {
			hc = (hc*31+r1)*31 + r2
		}
	}
	return
}

func kafkaAbs(i int32) int32 {
	switch {
	case i == -2147483648: // Integer.MIN_VALUE
		return 0
	case i < 0:
		return i * -1
	default:
		return i
	}
}

func hashCodePartition(key string, partitions int32) int32 {
	if partitions <= 0 {
		return -1
	}

	return kafkaAbs(hashCode(key)) % partitions
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, length)
	r.Read(buf)
	return fmt.Sprintf("%x", buf)[:length]
}

// setupCerts takes the paths to a tls certificate, CA, and certificate key in
// a PEM format and returns a constructed tls.Config object.
func setupCerts(certPath, caPath, keyPath string) (*tls.Config, error) {
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
			panic(fmt.Errorf("flag %q ($%s) not found", f, env))
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

func decoderForType(typ string) (func(s string) ([]byte, error), error) {
	switch typ {
	case "hex":
		return hex.DecodeString, nil
	case "base64":
		return base64.StdEncoding.DecodeString, nil
	case "string":
		return func(s string) ([]byte, error) {
			return []byte(s), nil
		}, nil
	}
	return nil, fmt.Errorf(`unsupported decoder %#v, only string, hex and base64 are supported.`, typ)
}

func encoderForType(typ string) (func([]byte) *string, error) {
	var enc func([]byte) string
	switch typ {
	case "hex":
		enc = hex.EncodeToString
	case "base64":
		enc = base64.StdEncoding.EncodeToString
	case "string":
		enc = func(data []byte) string {
			return string(data)
		}
	default:
		return nil, fmt.Errorf(`unsupported decoder %#v, only string, hex and base64 are supported.`, typ)
	}
	return func(data []byte) *string {
		if data == nil {
			return nil
		}
		s := enc(data)
		return &s
	}, nil
}
