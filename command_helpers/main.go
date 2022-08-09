package command_helpers

import (
	"context"
	"log"
	"os"
)

// CancelOnSignal calls the cancel function when an OS signal is received.
// If a second os signal is received, the function calls os.Exit(1).
func CancelOnSignal(sig <-chan os.Signal, cancel context.CancelFunc) {
	s := <-sig
	log.Printf("received signal: %+v. attempting to stop ...", s)
	cancel()
	s = <-sig
	log.Printf("received second os signal: %+v. exiting ...", s)
	os.Exit(1)
}
