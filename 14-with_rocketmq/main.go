package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	// Get the flow
	myFloq := kis.Pool().GetFlow("CalStuAvgScore")
	if myFloq == nil {
		panic("myFloq is nil")
	}

	// Create a new RocketMQ consumer
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group1"),
		consumer.WithNameServer([]string{"localhost:9876"}),
	)
	if err != nil {
		panic(err)
	}

	err = c.Subscribe("SourceStuScore", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {

		for _, msg := range msgs {
			// Commit the message to the flow
			_ = myFloq.CommitRow(string(msg.Body))

		}

		// Run the flow
		if err := myFloq.Run(ctx); err != nil {
			fmt.Println("err: ", err)
			return consumer.ConsumeRetryLater, err
		}

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		panic(err)
	}

	err = c.Start()
	if err != nil {
		panic(err)
	}

	defer c.Shutdown()

	select {}
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
