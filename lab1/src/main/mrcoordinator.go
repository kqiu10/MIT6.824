package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"os/signal"
	"syscall"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	// Create a channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Register the channel to receive notifications for specific signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Wait for a signal in a separate goroutine
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig, "signal received. Exiting gracefully.")
		os.Exit(0)
	}()

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

	// Simulate some work
	select {}
}
