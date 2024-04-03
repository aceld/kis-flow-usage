package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
)

type AvgStuScore3In struct {
	serialize.DefaultSerialize
	StuScore3
}

type AvgStuScore3Out struct {
	serialize.DefaultSerialize
	StuScoreAvg
}

func AvgStuScore3(ctx context.Context, flow kis.Flow, rows []*AvgStuScore3In) error {
	fmt.Printf("->Call Func AvgStuScore12\n")

	for _, row := range rows {

		out := AvgStuScore3Out{
			StuScoreAvg: StuScoreAvg{
				StuId:    row.StuId,
				AvgScore: (float64(row.Score3) + row.AvgScore12*2) / 3,
			},
		}

		// 提交结果数据
		_ = flow.CommitRow(out)
	}

	return flow.Next()
}
