package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
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

	// 获取Funciton的配置信息
	funcConfig := flow.GetThisFunction().GetConfig()
	fmt.Printf("function config: %+v\n", funcConfig)

	fmt.Printf("function Params: school = %+v\n", flow.GetFuncParam("school"))
	fmt.Printf("function Params: country = %+v\n", flow.GetFuncParam("country"))

	// 设置临时数据
	myTempNum := 1

	function := flow.GetThisFunction()
	if function.GetMetaData("num") == nil {
		function.SetMetaData("num", myTempNum)
	} else {
		myTempNum = function.GetMetaData("num").(int)
		myTempNum++
		function.SetMetaData("num", myTempNum)
	}

	fmt.Printf("myTempNum = %+v\n", myTempNum)

	return nil
}
