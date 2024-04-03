package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
)

type PrintStuAvgScoreIn struct {
	serialize.DefaultSerialize
	StuId    int     `json:"stu_id"`
	AvgScore float64 `json:"avg_score"`
}

func PrintStuAvgScore(ctx context.Context, flow kis.Flow, rows []*PrintStuAvgScoreIn) error {
	fmt.Printf("->Call Func PrintStuAvgScore, in Flow[%s]\n", flow.GetName())

	for _, row := range rows {
		fmt.Printf("stuid: [%+v], avg score: [%+v]\n", row.StuId, row.AvgScore)
	}

	return flow.Next()
}
