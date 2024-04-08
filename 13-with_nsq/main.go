package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"github.com/nsqio/go-nsq"
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

	// Create a new NSQ consumer
	config := nsq.NewConfig()
	config.MaxInFlight = 5

	consumer, err := nsq.NewConsumer("SourceStuScore", "channel1", config)
	if err != nil {
		panic(err)
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		// fork a new flow for each message
		flowCopy := flowOrg.Fork(ctx)

		// Commit the message to the flow
		_ = flowCopy.CommitRow(string(message.Body))

		// Run the flow
		if err := flowCopy.Run(ctx); err != nil {
			fmt.Println("err: ", err)
			return err
		}

		return nil
	}))

	err = consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		panic(err)
	}

	defer consumer.Stop()

	select {}
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
