package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"time"
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

	stuId := 100

	for {
		// make 1 row data
		dataStr := fmt.Sprintf(`{"stu_id":%d, "score_1":100, "score_2":90, "score_3":80}`, stuId)

		// Submit a string
		_ = flow1.CommitRow(dataStr)

		// Run the flow
		if err := flow1.Run(ctx); err != nil {
			fmt.Println("err: ", err)
		}

		stuId++
		time.Sleep(1 * time.Second)
	}

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
