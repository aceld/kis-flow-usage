package main

import (
	"context"
	"github.com/aceld/kis-flow/common"
	"github.com/aceld/kis-flow/config"
	"github.com/aceld/kis-flow/flow"
	"github.com/aceld/kis-flow/kis"
	"sync"
)

func RunFlowCalStuAvgScore12(ctx context.Context, flow kis.Flow) error {

	// Submit a string
	_ = flow.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90}`)
	_ = flow.CommitRow(`{"stu_id":102, "score_1":100, "score_2":80}`)

	// Run the flow
	if err := flow.Run(ctx); err != nil {
		return err
	}

	return nil
}

func RunFlowCalStuAvgScore3(ctx context.Context, flow kis.Flow) error {

	// Submit a string
	_ = flow.CommitRow(`{"stu_id":101, "score_3": 80}`)
	_ = flow.CommitRow(`{"stu_id":102, "score_3": 70}`)

	// Run the flow
	if err := flow.Run(ctx); err != nil {
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()

	// source
	source := config.KisSource{
		Name: "SourceStuScore",
		Must: []string{"stu_id", "user_id"},
	}

	// Connector: Score12Cache (like: conf/conn/conn-Score12Cache.yml)
	connScore12CacheConf := config.NewConnConfig("Score12Cache", "127.0.0.1:6379", common.REDIS, "stu_score12_avg_", nil)
	if connScore12CacheConf == nil {
		panic("connScore12CacheConf is nil")
	}

	// Function: VerifyStu（like: conf/func/func-VerifyStu.yml）
	funcVerifyStuConf := config.NewFuncConfig("VerifyStu", common.V, &source, nil)
	if funcVerifyStuConf == nil {
		panic("funcVerifyStuConf is nil")
	}

	// Function: AvgStuScore12 (like: conf/func/func-AvgStuScore-1-2.yml)
	funcAvgStuScore12Conf := config.NewFuncConfig("AvgStuScore12", common.C, &source, nil)
	if funcAvgStuScore12Conf == nil {
		panic("funcAvgStuScore12Conf is nil")
	}

	// Function: SaveScoreAvg12 (like: conf/func/func-SaveScoreAvg-1-2.yml)
	funcSaveScoreAvg12Conf := config.NewFuncConfig("SaveScoreAvg12", common.S, &source, nil)
	if funcSaveScoreAvg12Conf == nil {
		panic("funcSaveScoreAvg12Conf is nil")
	}

	// ---> Add connector to function
	if err := funcSaveScoreAvg12Conf.AddConnConfig(connScore12CacheConf); err != nil {
		panic(err)
	}

	// Function: PrintStuAvgScore (like: conf/func/func-PrintStuAvgScore.yml)
	funcPrintStuAvgScoreConf := config.NewFuncConfig("PrintStuAvgScore", common.E, &source, nil)
	if funcPrintStuAvgScoreConf == nil {
		panic("funcPrintStuAvgScoreConf is nil")
	}

	// Function: LoadScoreAvg12 (like: conf/func/func-LoadScoreAvg-1-2.yml)
	funcLoadScoreAvg12Conf := config.NewFuncConfig("LoadScoreAvg12", common.L, &source, nil)
	if funcLoadScoreAvg12Conf == nil {
		panic("funcLoadScoreAvg12Conf is nil")
	}

	// ---> Add connector to function
	if err := funcLoadScoreAvg12Conf.AddConnConfig(connScore12CacheConf); err != nil {
		panic(err)
	}

	// Function: AvgStuScore3 (like: conf/func/func-AvgStuScore-3.yml)
	funcAvgStuScore3Conf := config.NewFuncConfig("AvgStuScore3", common.C, &source, nil)
	if funcAvgStuScore3Conf == nil {
		panic("funcAvgStuScore3Conf is nil")
	}

	// Flow: CalStuAvgScore12 (like: conf/flow/flow-CalStuAvgScore-1-2.yml)
	flowCalStuAvgScore12Conf := config.NewFlowConfig("CalStuAvgScore12", common.FlowEnable)
	if flowCalStuAvgScore12Conf == nil {
		panic("flowCalStuAvgScore12Conf is nil")
	}

	// Flow: CalStuAvgScore3 (like: conf/flow/flow-CalStuAvgScore-3.yml)
	flowCalStuAvgScore3Conf := config.NewFlowConfig("CalStuAvgScore3", common.FlowEnable)
	if flowCalStuAvgScore3Conf == nil {
		panic("flowCalStuAvgScore3Conf is nil")
	}

	// Create a new flow1
	flow1 := flow.NewKisFlow(flowCalStuAvgScore12Conf)

	// Link Functions to flow1
	if err := flow1.Link(funcVerifyStuConf, nil); err != nil {
		panic(err)
	}
	if err := flow1.Link(funcAvgStuScore12Conf, nil); err != nil {
		panic(err)
	}
	if err := flow1.Link(funcSaveScoreAvg12Conf, nil); err != nil {
		panic(err)
	}
	if err := flow1.Link(funcPrintStuAvgScoreConf, nil); err != nil {
		panic(err)
	}

	// Create a new flow2
	flow2 := flow.NewKisFlow(flowCalStuAvgScore12Conf)

	// Link Functions to flow1
	if err := flow2.Link(funcVerifyStuConf, nil); err != nil {
		panic(err)
	}
	if err := flow2.Link(funcLoadScoreAvg12Conf, nil); err != nil {
		panic(err)
	}
	if err := flow2.Link(funcAvgStuScore3Conf, nil); err != nil {
		panic(err)
	}
	if err := flow2.Link(funcPrintStuAvgScoreConf, nil); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		// run flow1
		if err := RunFlowCalStuAvgScore12(ctx, flow1); err != nil {
			panic(err)
		}
	}()

	go func() {
		defer wg.Done()
		// run flow2
		if err := RunFlowCalStuAvgScore3(ctx, flow2); err != nil {
			panic(err)
		}
	}()

	wg.Wait()

	return
}

/*
func main() {
	ctx := context.Background()

	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		// run flow1
		defer wg.Done()
		// Get the flow
		flow1 := kis.Pool().GetFlow("CalStuAvgScore12")
		if flow1 == nil {
			panic("flow1 is nil")
		}

		if err := RunFlowCalStuAvgScore12(ctx, flow1); err != nil {
			panic(err)
		}
	}()

	go func() {
		defer wg.Done()
		// Get the flow
		flow2 := kis.Pool().GetFlow("CalStuAvgScore3")
		if flow2 == nil {
			panic("flow2 is nil")
		}

		// run flow2
		if err := RunFlowCalStuAvgScore3(ctx, flow2); err != nil {
			panic(err)
		}
	}()

	wg.Wait()

	return
}
*/

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore12", AvgStuScore12)
	kis.Pool().FaaS("SaveScoreAvg12", SaveScoreAvg12)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
	kis.Pool().FaaS("LoadScoreAvg12", LoadScoreAvg12)
	kis.Pool().FaaS("AvgStuScore3", AvgStuScore3)

	// Register connectors
	kis.Pool().CaaSInit("Score12Cache", InitScore12Cache)
}
