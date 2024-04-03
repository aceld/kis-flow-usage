package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
	"github.com/go-redis/redis/v8"
	"strconv"
)

type SaveStuScoreIn struct {
	serialize.DefaultSerialize
	StuScoreAvg
}

func BatchSetStuScores(ctx context.Context, conn kis.Connector, rows []*SaveStuScoreIn) error {

	var rdb *redis.Client

	// Get Redis Client
	rdb = conn.GetMetaData("rdb").(*redis.Client)

	// Set data to redis
	pipe := rdb.Pipeline()

	for _, score := range rows {
		// make key
		key := conn.GetConfig().Key + strconv.Itoa(score.StuId)

		pipe.HMSet(context.Background(), key, map[string]interface{}{
			"avg_score": score.AvgScore,
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func SaveScoreAvg12(ctx context.Context, flow kis.Flow, rows []*SaveStuScoreIn) error {
	fmt.Printf("->Call Func SaveScoreScore12\n")

	conn, err := flow.GetConnector()
	if err != nil {
		fmt.Printf("SaveScoreScore12(): GetConnector err = %s\n", err.Error())
		return err
	}

	if BatchSetStuScores(ctx, conn, rows) != nil {
		fmt.Printf("SaveScoreScore12(): BatchSetStuScores err = %s\n", err.Error())
		return err
	}

	return flow.Next(kis.ActionDataReuse)
}
