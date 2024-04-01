package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
)

type VerifyStuIn struct {
	serialize.DefaultSerialize
	StuScore
}

func VerifyStu(ctx context.Context, flow kis.Flow, rows []*VerifyStuIn) error {
	fmt.Printf("->Call Func VerifyStu\n")

	for _, stu := range rows {
		// 过滤掉不合法的数据
		if stu.StuId < 0 || stu.StuId > 999 {
			// 终止当前Flow流程，不会再继续执行当前Flow的后续Function
			return flow.Next(kis.ActionAbort)
		}
	}

	return flow.Next(kis.ActionDataReuse)
	//return flow.Next(kis.ActionForceEntryNext)
}
