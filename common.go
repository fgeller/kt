package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"

	"github.com/IBM/sarama"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	ENV_AUTH          = "KT_AUTH"
	ENV_ADMIN_TIMEOUT = "KT_ADMIN_TIMEOUT"
	ENV_BROKERS       = "KT_BROKERS"
	ENV_TOPIC         = "KT_TOPIC"
	ENV_KAFKA_VERSION = "KT_KAFKA_VERSION"
)

var invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

type command interface {
	run(args []string)
}

type baseCmd struct {
	verbose bool
}

func (b *baseCmd) infof(msg string, args ...interface{}) {
	if b.verbose {
		warnf(msg, args...)
	}
}

func warnf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
}

func outf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, args...)
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		warnf("failed to close %#v err=%v", name, err)
	}
}

func chooseKafkaVersion(arg, env string) (sarama.KafkaVersion, error) {
	switch {
	case arg != "":
		return sarama.ParseKafkaVersion(strings.TrimPrefix(arg, "v"))
	case env != "":
		return sarama.ParseKafkaVersion(strings.TrimPrefix(env, "v"))
	default:
		return sarama.V3_0_0_0, nil
	}
}

type printContext struct {
	output interface{}
	done   chan struct{}
}

func print(in <-chan printContext, pretty bool) {
	var (
		buf     []byte
		err     error
		marshal = json.Marshal
	)

	if pretty && terminal.IsTerminal(int(syscall.Stdout)) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}

	for {
		ctx := <-in
		if buf, err = marshal(ctx.output); err != nil {
			failf("failed to marshal output %#v, err=%v", ctx.output, err)
		}

		fmt.Println(string(buf))
		close(ctx.done)
	}
}

func quitf(msg string, args ...interface{}) {
	exitf(0, msg, args...)
}

func failf(msg string, args ...interface{}) {
	exitf(1, msg, args...)
}

func exitf(code int, msg string, args ...interface{}) {
	if code == 0 {
		outf(msg+"\n", args...)
	} else {
		warnf(msg+"\n", args...)
	}
	os.Exit(code)
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
		err := fmt.Errorf("certificate, CA and key path are required - got cert=%#v ca=%#v key=%#v", certPath, caPath, keyPath)
		return nil, err
	}

	caString, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		failf("unable to add ca at %s to certificate pool", caPath)
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

type authConfig struct {
	Mode              string `json:"mode"`
	CACert            string `json:"ca-certificate"`
	ClientCert        string `json:"client-certificate"`
	ClientCertKey     string `json:"client-certificate-key"`
	SASLPlainUser     string `json:"sasl_plain_user"`
	SASLPlainPassword string `json:"sasl_plain_password"`
}

func setupAuth(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.Mode == "" {
		return nil
	}

	switch auth.Mode {
	case "TLS":
		return setupAuthTLS(auth, saramaCfg)
	case "TLS-1way":
		return setupAuthTLS1Way(auth, saramaCfg)
	case "SASL":
		return setupSASL(auth, saramaCfg)
	default:
		return fmt.Errorf("unsupport auth mode: %#v", auth.Mode)
	}
}

func setupSASL(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = auth.SASLPlainUser
	saramaCfg.Net.SASL.Password = auth.SASLPlainPassword
	return nil
}

func setupAuthTLS1Way(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = &tls.Config{}

	if auth.CACert == "" {
		return nil
	}

	caString, err := ioutil.ReadFile(auth.CACert)
	if err != nil {
		return fmt.Errorf("failed to read ca-certificate err=%v", err)
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		failf("unable to add ca-certificate at %s to certificate pool", auth.CACert)
	}

	tlsCfg := &tls.Config{RootCAs: caPool}
	tlsCfg.BuildNameToCertificate()

	saramaCfg.Net.TLS.Config = tlsCfg
	return nil
}

func setupAuthTLS(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.CACert == "" || auth.ClientCert == "" || auth.ClientCertKey == "" {
		return fmt.Errorf("client-certificate, client-certificate-key and ca-certificate are required - got auth=%#v", auth)
	}

	caString, err := ioutil.ReadFile(auth.CACert)
	if err != nil {
		return fmt.Errorf("failed to read ca-certificate err=%v", err)
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		failf("unable to add ca-certificate at %s to certificate pool", auth.CACert)
	}

	clientCert, err := tls.LoadX509KeyPair(auth.ClientCert, auth.ClientCertKey)
	if err != nil {
		return err
	}

	tlsCfg := &tls.Config{RootCAs: caPool, Certificates: []tls.Certificate{clientCert}}
	tlsCfg.BuildNameToCertificate()

	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = tlsCfg

	return nil
}

func qualifyPath(argFN string, target *string) {
	if *target != "" && !filepath.IsAbs(*target) && filepath.Dir(*target) == "." {
		*target = filepath.Join(filepath.Dir(argFN), *target)
	}
}

func readAuthFile(argFN string, envFN string, target *authConfig) {
	if argFN == "" && envFN == "" {
		return
	}

	fn := argFN
	if fn == "" {
		fn = envFN
	}

	byts, err := ioutil.ReadFile(fn)
	if err != nil {
		failf("failed to read auth file err=%v", err)
	}

	if err := json.Unmarshal(byts, target); err != nil {
		failf("failed to unmarshal auth file err=%v", err)
	}

	qualifyPath(fn, &target.CACert)
	qualifyPath(fn, &target.ClientCert)
	qualifyPath(fn, &target.ClientCertKey)
}
