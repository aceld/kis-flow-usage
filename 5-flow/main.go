package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/common"
	"github.com/aceld/kis-flow/config"
	"github.com/aceld/kis-flow/flow"
	"github.com/aceld/kis-flow/kis"
)

/*
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

	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90, "score_3":80}`)
	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":1001, "score_1":100, "score_2":70, "score_3":60}`)

	// Run the flow
	if err := flow1.Run(ctx); err != nil {
		fmt.Println("err: ", err)
	}

	return
}
*/

func main() {
	ctx := context.Background()

	// Create a new flow configuration
	myFlowConfig1 := config.NewFlowConfig("CalStuAvgScore", common.FlowEnable)

	// Create new function configuration
	verifyStuConfig := config.NewFuncConfig("VerifyStu", common.V, nil, nil)
	avgStuScoreConfig := config.NewFuncConfig("AvgStuScore", common.C, nil, nil)
	printStuScoreConfig := config.NewFuncConfig("PrintStuAvgScore", common.E, nil, nil)

	// Create a new flow
	flow1 := flow.NewKisFlow(myFlowConfig1)

	// Link functions to the flow
	_ = flow1.Link(verifyStuConfig, config.FParam{"school": "TsingHua", "country": "China"})
	_ = flow1.Link(avgStuScoreConfig, config.FParam{"school": "TsingHua", "country": "China"})
	_ = flow1.Link(printStuScoreConfig, config.FParam{"school": "TsingHua", "country": "China"})

	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90, "score_3":80}`)

	// Run the flow
	if err := flow1.Run(ctx); err != nil {
		fmt.Println("err: ", err)
	}

	// Fork the flow
	flowClone1 := flow1.Fork(ctx)

	// Run the flow
	_ = flowClone1.CommitRow(`{"stu_id":102, "score_1":100, "score_2":70, "score_3":60}`)

	if err := flowClone1.Run(ctx); err != nil {
		fmt.Println("err: ", err)
	}

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
