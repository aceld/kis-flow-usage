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

	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90, "score_3":80}`)
	_ = flow1.CommitRow(`{"stu_id":102, "score_1":100, "score_2":70, "score_3":60}`)

	// data string slice
	dataStrings := []string{
		`{"stu_id":103, "score_1":100, "score_2":90, "score_3":80}`,
		`{"stu_id":104, "score_1":100, "score_2":70, "score_3":60}`,
		`{"stu_id":105, "score_1":80, "score_2":50, "score_3":50}`,
	}

	// Submit string slice
	if err := flow1.CommitRowBatch(dataStrings); err != nil {
		fmt.Println("err: ", err)
		return
	}

	stu106 := AvgStuScoreIn{
		StuId:  106,
		Score1: 80,
		Score2: 81,
		Score3: 82,
	}

	_ = flow1.CommitRow(stu106)

	stu107 := AvgStuScoreIn{
		StuId:  107,
		Score1: 82,
		Score2: 83,
		Score3: 84,
	}

	_ = flow1.CommitRow(&stu107)

	dataStructs := []AvgStuScoreIn{
		{
			StuId:  108,
			Score1: 82,
			Score2: 83,
			Score3: 84,
		},
		{
			StuId:  109,
			Score1: 82,
			Score2: 83,
			Score3: 84,
		},
	}

	// Submit struct slice
	if err := flow1.CommitRowBatch(dataStructs); err != nil {
		fmt.Println("err: ", err)
		return
	}

	dataStructsPtr := []*AvgStuScoreIn{
		{
			StuId:  110,
			Score1: 82,
			Score2: 83,
			Score3: 84,
		},
		{
			StuId:  110,
			Score1: 82,
			Score2: 83,
			Score3: 84,
		},
	}

	// Submit struct slice
	if err := flow1.CommitRowBatch(dataStructsPtr); err != nil {
		fmt.Println("err: ", err)
		return
	}

	// Run the flow
	if err := flow1.Run(ctx); err != nil {
		fmt.Println("err: ", err)
	}

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
