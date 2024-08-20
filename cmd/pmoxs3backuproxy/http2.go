package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/juju/clock"
	"github.com/juju/mutex/v2"
	"golang.org/x/net/http2"
)

func (s *Server) backup(sock net.Conn, C TicketEntry, ds string, S s3pmoxcommon.Snapshot) {
	s.SessionsMutex.Lock()
	if s.Sessions == 0 {
		h := sha256.Sum256([]byte(C.Endpoint + "|" + ds))
		lockname := "PBSS3" + hex.EncodeToString(h[:])[:16]
		sp := mutex.Spec{
			Clock:   clock.WallClock,
			Name:    lockname,
			Delay:   time.Millisecond,
			Timeout: time.Second * 30,
		}
		var err error
		s.SessionsRelease, err = mutex.Acquire(sp)
		if err != nil {
			sock.Close()
			s3backuplog.ErrorPrint("Failed to acquire Lock for %s", lockname)
			return
		}
		s3backuplog.DebugPrint("Locked %s", lockname)
	}
	s.Sessions++
	s.SessionsMutex.Unlock()

	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{
		H2Ticket:          &C,
		SelectedDataStore: &ds,
		Snapshot:          &S,
		Writers:           make(map[int32]*Writer),
		Finished:          false,
	}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})
	if !snew.Finished { //Incomplete backup because connection died pve side, remove from S3
		S.Datastore = ds
		if err := S.Delete(*C.Client); err != nil {
			s3backuplog.ErrorPrint("Failed to remove incomplete backup: " + err.Error())
		} else {
			s3backuplog.WarnPrint("Removed incomplete backup %s", snew.Snapshot.S3Prefix())
		}
	}
	s.SessionsMutex.Lock()
	s.Sessions--
	if s.Sessions == 0 {
		s.SessionsRelease.Release()
	}
	s.SessionsMutex.Unlock()
}

func (s *Server) restore(sock net.Conn, C TicketEntry, ds string, S s3pmoxcommon.Snapshot) {
	s.SessionsMutex.Lock()
	if s.Sessions == 0 {
		h := sha256.Sum256([]byte(C.Endpoint + C.AccessKeyID + ds))
		lockname := "PBSS3" + hex.EncodeToString(h[:])[:16]
		sp := mutex.Spec{
			Clock:   clock.WallClock,
			Name:    lockname,
			Delay:   time.Millisecond,
			Timeout: time.Second * 30,
		}
		var err error
		s.SessionsRelease, err = mutex.Acquire(sp)
		if err != nil {
			sock.Close()
			s3backuplog.ErrorPrint("Failed to acquire Lock for %s", lockname)
			return
		}
	}
	s.Sessions++
	s.SessionsMutex.Unlock()

	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{
		H2Ticket:          &C,
		SelectedDataStore: &ds,
		Snapshot:          &S,
		Writers:           make(map[int32]*Writer),
		Finished:          false,
	}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})

	s.SessionsMutex.Lock()
	s.Sessions--
	if s.Sessions == 0 {
		s.SessionsRelease.Release()
	}
	s.SessionsMutex.Unlock()
}
