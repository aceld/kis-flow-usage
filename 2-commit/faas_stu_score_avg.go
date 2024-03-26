package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
	"reflect"
)

type AvgStuScoreIn struct {
	serialize.DefaultSerialize
	StuId  int `json:"stu_id"`
	Score1 int `json:"score_1"`
	Score2 int `json:"score_2"`
	Score3 int `json:"score_3"`
}

type AvgStuScoreOut struct {
	serialize.DefaultSerialize
	StuId    int     `json:"stu_id"`
	AvgScore float64 `json:"avg_score"`
}

// AvgStuScore(FaaS) 计算学生平均分
func AvgStuScore(ctx context.Context, flow kis.Flow, rows []*AvgStuScoreIn) error {

	// 得到原始数据
	for _, data := range flow.Input() {
		fmt.Printf("data value= %+v, data type = %+v\n", data, reflect.TypeOf(data))
	}

	// 得到反序列化之后的数据
	for _, row := range rows {

		out := AvgStuScoreOut{
			StuId:    row.StuId,
			AvgScore: float64(row.Score1+row.Score2+row.Score3) / 3,
		}

		// 提交结果数据
		_ = flow.CommitRow(out)
	}

	return nil
}
