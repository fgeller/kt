#!/bin/bash
# original at https://github.com/confluentinc/cp-docker-images/blob/5.3.1-post/examples/kafka-cluster-ssl/secrets/create-certs.sh
set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

# Generate CA key
openssl req -new -x509 -keyout snakeoil-ca-1.key -out snakeoil-ca-1.crt -days 365 -subj '/CN=localhost/OU=TEST/O=KT' -passin pass:ktktkt -passout pass:ktktkt

for i in broker1
do
	echo $i
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=localhost, OU=TEST, O=KT" \
				 -keystore kafka.$i.keystore.jks \
				 -keyalg RSA \
				 -storepass ktktkt \
				 -keypass ktktkt

	# Create CSR, sign the key and import back into keystore
	keytool -keystore kafka.$i.keystore.jks -alias $i -certreq -file $i.csr -storepass ktktkt -keypass ktktkt

	openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:ktktkt

	keytool -keystore kafka.$i.keystore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass ktktkt -keypass ktktkt
	keytool -keystore kafka.$i.keystore.jks -alias $i -import -file $i-ca1-signed.crt -storepass ktktkt -keypass ktktkt

	# Create truststore and import the CA cert.
	keytool -keystore kafka.$i.truststore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass ktktkt -keypass ktktkt

  echo "ktktkt" > ${i}_sslkey_creds
  echo "ktktkt" > ${i}_keystore_creds
  echo "ktktkt" > ${i}_truststore_creds
done

# generate public/private key pair for kt
openssl genrsa -out kt-test.key 2048
openssl req -new -key kt-test.key -out kt-test.csr -subj '/CN=localhost/OU=TEST/O=KT'
openssl x509 -req -days 9999 -in kt-test.csr -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -CAcreateserial -out kt-test.crt
