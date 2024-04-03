package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/log"
	"github.com/go-redis/redis/v8"
)

// type ConnInit func(conn Connector) error

func InitScore12Cache(connector kis.Connector) error {
	fmt.Println("===> Call Connector InitScore12Cache")

	// init Redis Conn Client
	rdb := redis.NewClient(&redis.Options{
		Addr:     connector.GetConfig().AddrString, // Redis-Server address
		Password: "",                               // password
		DB:       0,                                // select db
	})

	// Ping test
	pong, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Logger().ErrorF("Failed to connect to Redis: %v", err)
		return err
	}
	fmt.Println("Connected to Redis:", pong)

	// set rdb to connector
	connector.SetMetaData("rdb", rdb)

	return nil
}
