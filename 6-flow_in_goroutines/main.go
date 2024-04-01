package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"sync"
)

func main() {
	ctx := context.Background()
	// Get a WaitGroup
	var wg sync.WaitGroup

	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	// Get the flow
	flow1 := kis.Pool().GetFlow("CalStuAvgScore")
	if flow1 == nil {
		panic("flow1 is nil")
	}
	// Fork the flow
	flowClone1 := flow1.Fork(ctx)

	// Add to WaitGroup
	wg.Add(2)

	// Run Flow1
	go func() {
		defer wg.Done()
		// Submit a string
		_ = flow1.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90, "score_3":80}`)
		// Submit a string
		_ = flow1.CommitRow(`{"stu_id":1001, "score_1":100, "score_2":70, "score_3":60}`)

		// Run the flow
		if err := flow1.Run(ctx); err != nil {
			fmt.Println("err: ", err)
		}
	}()

	// Run FlowClone1
	go func() {
		defer wg.Done()
		// Submit a string
		_ = flowClone1.CommitRow(`{"stu_id":201, "score_1":100, "score_2":90, "score_3":80}`)
		// Submit a string
		_ = flowClone1.CommitRow(`{"stu_id":2001, "score_1":100, "score_2":70, "score_3":60}`)

		if err := flowClone1.Run(ctx); err != nil {
			fmt.Println("err: ", err)
		}
	}()

	// Wait for Goroutines to finish
	wg.Wait()

	fmt.Println("All flows completed.")

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
