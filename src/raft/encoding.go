package raft

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
)

func encodeLogs(logs []LogEntry) []byte {
	encodedLogs, err := encodePayload(logs)

	if err != nil {
		log.Printf("Failed to encode arguments: %v", err)
	}

	return encodedLogs
}

func decodeLogs(encodedLogs []byte) []LogEntry {
	decodedLogs := []LogEntry{}

	// Decode the reply
	err := decodePayload(encodedLogs, &decodedLogs)
	if err != nil {
		log.Printf("Failed to decode reply: %v", err)
	}

	return decodedLogs
}

func encodePayload(data interface{}) ([]byte, error) {
	// Marshal the data into JSON format
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Create a new gzip writer
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)

	// Write the JSON data to the gzip writer
	_, err = gzipWriter.Write(jsonData)
	if err != nil {
		return nil, err
	}

	// Close the gzip writer to flush any remaining data
	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}

	// Get the compressed payload
	compressedPayload := buf.Bytes()

	return compressedPayload, nil
}

func decodePayload(compressedPayload []byte, data interface{}) error {
	// Create a new gzip reader
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressedPayload))
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	// Read the compressed data
	jsonData, err := io.ReadAll(gzipReader)
	if err != nil {
		return err
	}

	// Unmarshal the JSON data into the provided data structure
	err = json.Unmarshal(jsonData, data)
	if err != nil {
		return err
	}

	return nil
}

func decodeParams(encodedArgs []byte, encodedReply []byte) (*AppendEntriesArgs, *AppendEntriesReply) {

	decodedReply := &AppendEntriesReply{}
	decodedArgs := &AppendEntriesArgs{}

	// Decode the reply
	err := decodePayload(encodedReply, decodedReply)
	if err != nil {
		log.Printf("Failed to decode reply: %v", err)
	}

	err = decodePayload(encodedArgs, decodedArgs)

	if err != nil {
		log.Printf("Failed to decode reply: %v", err)
	}

	return decodedArgs, decodedReply
}

func encodeParams(args interface{}, reply interface{}) ([]byte, []byte) {
	encodedArgs, err := encodePayload(args)

	if err != nil {
		log.Printf("Failed to encode arguments: %v", err)
	}

	encodedReply, err := encodePayload(reply)

	if err != nil {
		log.Printf("Failed to encode arguments: %v", err)
	}

	return encodedArgs, encodedReply
}
