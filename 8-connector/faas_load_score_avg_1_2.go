package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/serialize"
	"github.com/go-redis/redis/v8"
	"strconv"
)

type LoadStuScoreIn struct {
	serialize.DefaultSerialize
	StuScore3
}

type LoadStuScoreOut struct {
	serialize.DefaultSerialize
	StuScore3
}

func GetStuScoresByStuId(ctx context.Context, conn kis.Connector, stuId int) (float64, error) {

	var rdb *redis.Client

	// Get Redis Client
	rdb = conn.GetMetaData("rdb").(*redis.Client)

	// make key
	key := conn.GetConfig().Key + strconv.Itoa(stuId)

	// get data from redis
	result, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	// get value
	avgScoreStr, ok := result["avg_score"]
	if !ok {
		return 0, fmt.Errorf("avg_score not found for stuId: %d", stuId)
	}

	// parse to float64
	avgScore, err := strconv.ParseFloat(avgScoreStr, 64)
	if err != nil {
		return 0, err
	}

	return avgScore, nil
}

func LoadScoreAvg12(ctx context.Context, flow kis.Flow, rows []*LoadStuScoreIn) error {
	fmt.Printf("->Call Func LoadScoreAvg12\n")

	conn, err := flow.GetConnector()
	if err != nil {
		fmt.Printf("LoadScoreAvg12(): GetConnector err = %s\n", err.Error())
		return err
	}

	for _, row := range rows {
		stuScoreAvg1_2, err := GetStuScoresByStuId(ctx, conn, row.StuId)
		if err != nil {
			fmt.Printf("LoadScoreAvg12(): GetStuScoresByStuId err = %s\n", err.Error())
			return err
		}

		out := LoadStuScoreOut{
			StuScore3: StuScore3{
				StuId:      row.StuId,
				Score3:     row.Score3,
				AvgScore12: stuScoreAvg1_2, // avg score of score1 and score2 (load from redis)
			},
		}

		// commit result
		_ = flow.CommitRow(out)
	}

	return flow.Next()
}
