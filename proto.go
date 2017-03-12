package main

import (
	"errors"
	"net/rpc"
	"os/exec"
)

// Args is a predefined contract struct between client and remote process for arguments
type Args struct {
	TypeName string
	Data     []byte
}

// ProtoDecoder is a struct that holds the rpc client and the cmd that started the RPC process
type ProtoDecoder struct {
	rpcClient  *rpc.Client
	rpcProcess *exec.Cmd
}

// DeserializeAny tries to deserialize a given byte array of data into a proto.Message specified by given typeName via an RPC
func (c *ProtoDecoder) DeserializeAny(typeName string, data []byte) (string, error) {

	args := &Args{
		TypeName: typeName,
		Data:     data,
	}
	var str string
	err := c.rpcClient.Call("ProtoDeserializer.DeserializeAny", args, &str)
	if err != nil {
		return "", err
	}
	return str, nil
}

// MustNewProtoDecoder returns a new ProtoDecoder by execing a new RPC decoder and initializing a new rpc.client to talk to decoder
func MustNewProtoDecoder(command, network, address string) (*ProtoDecoder, error) {

	cmd := exec.Command(command)
	err := cmd.Start()

	if err != nil {
		return nil, errors.New("Failed to exec RPC decoder. " + err.Error())
	}
	client, err := rpc.DialHTTP(network, address)
	if err != nil {
		return nil, err
	}
	return &ProtoDecoder{
		client,
		cmd,
	}, nil

}

// Close terminates both the RPC client and RPC process
func (c *ProtoDecoder) Close() error {
	if err := c.rpcClient.Close(); err != nil {
		return errors.New("Failed to close rpc client. " + err.Error())
	}

	if err := c.rpcProcess.Process.Kill(); err != nil {
		return errors.New("Failed to kill rpc process. " + err.Error())
	}
	return nil
}
