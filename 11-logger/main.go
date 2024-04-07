package main

import (
	"context"
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/log"
	"sync"
)

// MyLogger Custom Logger
type MyLogger struct {
	debugMode bool
	mu        sync.Mutex
}

func (log *MyLogger) SetDebugMode(enable bool) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.debugMode = enable
}

func (log *MyLogger) InfoF(str string, v ...interface{}) {
	fmt.Printf(str, v...)
	fmt.Printf("\n")
}

func (log *MyLogger) ErrorF(str string, v ...interface{}) {
	fmt.Printf(str, v...)
	fmt.Printf("\n")
}

func (log *MyLogger) DebugF(str string, v ...interface{}) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.debugMode {
		fmt.Printf(str, v...)
		fmt.Printf("\n")
	}
}

func (log *MyLogger) InfoFX(ctx context.Context, str string, v ...interface{}) {
	fmt.Println(ctx)
	fmt.Printf(str, v...)
	fmt.Printf("\n")
}

func (log *MyLogger) ErrorFX(ctx context.Context, str string, v ...interface{}) {
	fmt.Println(ctx)
	fmt.Printf(str, v...)
	fmt.Printf("\n")
}

func (log *MyLogger) DebugFX(ctx context.Context, str string, v ...interface{}) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.debugMode {
		fmt.Println(ctx)
		fmt.Printf(str, v...)
		fmt.Printf("\n")
	}
}

func main() {
	ctx := context.Background()

	// Set Custom Logger
	log.SetLogger(&MyLogger{})

	// Set Debug Mode
	log.Logger().SetDebugMode(true)

	// Load Configuration from file
	if err := file.ConfigImportYaml("conf/"); err != nil {
		panic(err)
	}

	// Get the flow
	flow1 := kis.Pool().GetFlow("CalStuAvgScore")
	if flow1 == nil {
		panic("flow1 is nil")
	}

	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":101, "score_1":100, "score_2":90, "score_3":80}`)
	// Submit a string
	_ = flow1.CommitRow(`{"stu_id":1001, "score_1":100, "score_2":70, "score_3":60}`)

	// Run the flow
	if err := flow1.Run(ctx); err != nil {
		fmt.Println("err: ", err)
	}

	return
}

func init() {
	// Register functions
	kis.Pool().FaaS("VerifyStu", VerifyStu)
	kis.Pool().FaaS("AvgStuScore", AvgStuScore)
	kis.Pool().FaaS("PrintStuAvgScore", PrintStuAvgScore)
}
