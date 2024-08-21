package main

import (
	"os"
	"os/signal"
	"syscall"

	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
)

func (s *Server) handleSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		var signalcnt int = 0
		for {
			sig := <-sigs
			switch sig {
			case syscall.SIGINT, os.Interrupt, syscall.SIGTERM:
				if s.Sessions > 0 && signalcnt < 1 {
					s3backuplog.WarnPrint("%d sessions active skipping shutdown.", s.Sessions)
					s3backuplog.WarnPrint("Send signal again to force exit.")
					signalcnt += 1
					continue
				}
				s3backuplog.InfoPrint("Received signal %d, exiting", sig)
				os.Exit(1)
			}
		}
	}()
}
