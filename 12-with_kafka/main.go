package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()

	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	// Get the flow
	flowOrg := kis.Pool().GetFlow("CalStuAvgScore")
	if flowOrg == nil {
		panic("flowOrg is nil")
	}

	// Create a new Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "SourceStuScore",
		GroupID:     "group1",
		MinBytes:    10e3,                   // 10KB
		MaxBytes:    10e6,                   // 10MB
		MaxWait:     500 * time.Millisecond, // 最长等待时间
		StartOffset: kafka.FirstOffset,
	})

	defer reader.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ { // use 5 consumers to consume in parallel
		wg.Add(1)
		go func() {
			// fork a new flow for each consumer
			flowCopy := flowOrg.Fork(ctx)

			defer wg.Done()
			for {
				// Read a message from Kafka
				message, err := reader.ReadMessage(ctx)
				if err != nil {
					fmt.Printf("error reading message: %v\n", err)
					break
				}

				// Commit the message to the flow
				_ = flowCopy.CommitRow(string(message.Value))

				// Run the flow
				if err := flowCopy.Run(ctx); err != nil {
					fmt.Println("err: ", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
