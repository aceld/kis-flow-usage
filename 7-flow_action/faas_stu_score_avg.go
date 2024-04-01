package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
)

type AvgStuScoreIn struct {
	serialize.DefaultSerialize
	StuScore
}

type AvgStuScoreOut struct {
	serialize.DefaultSerialize
	StuScoreAvg
}

func AvgStuScore(ctx context.Context, flow kis.Flow, rows []*AvgStuScoreIn) error {
	fmt.Printf("->Call Func AvgStuScore\n")

	for _, row := range rows {

		out := AvgStuScoreOut{
			StuScoreAvg: StuScoreAvg{
				StuId:    row.StuId,
				AvgScore: float64(row.Score1+row.Score2+row.Score3) / 3,
			},
		}

		// 提交结果数据
		_ = flow.CommitRow(out)
	}

	return flow.Next()
}
