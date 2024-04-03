package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
)

type AvgStuScoreIn_1_2 struct {
	serialize.DefaultSerialize
	StuScore1_2
}

type AvgStuScoreOut_1_2 struct {
	serialize.DefaultSerialize
	StuScoreAvg
}

func AvgStuScore12(ctx context.Context, flow kis.Flow, rows []*AvgStuScoreIn_1_2) error {
	fmt.Printf("->Call Func AvgStuScore12\n")

	for _, row := range rows {

		out := AvgStuScoreOut_1_2{
			StuScoreAvg: StuScoreAvg{
				StuId:    row.StuId,
				AvgScore: float64(row.Score1+row.Score2) / 2,
			},
		}

		// 提交结果数据
		_ = flow.CommitRow(out)
	}

	return flow.Next()
}
