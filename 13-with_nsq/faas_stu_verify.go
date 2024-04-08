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

	fmt.Println("school = ", flow.GetFuncParam("school"))
	fmt.Println("country = ", flow.GetFuncParam("country"))

	for _, stu := range rows {
		// 过滤掉不合法的数据
		if stu.StuId < 0 || stu.StuId > 999 {
			continue
		}

		_ = flow.CommitRow(stu)
	}

	return nil
}
