package litequeue

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
)

func EncodeMessage(payload interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")

	err := enc.Encode(payload)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeMessage(pack []byte, target interface{}) error {
	var buf bytes.Buffer
	buf.Write(pack)

	enc := msgpack.NewDecoder(&buf)
	enc.SetCustomStructTag("json")

	err := enc.Decode(&target)
	if err != nil {
		return err
	}

	return nil
}
