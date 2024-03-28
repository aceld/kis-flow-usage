package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
)

func main() {
	ctx := context.Background()

	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	// Get the flow
	flow1 := kis.Pool().GetFlow("CalStuAvgScore")
	if flow1 == nil {
		panic("flow1 is nil")
	}

	// Run the flow
	times := 3
	for times > 0 {
		if err := flow1.Run(ctx); err != nil {
			fmt.Println("err: ", err)
		}
		times--
	}

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
