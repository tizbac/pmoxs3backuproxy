/*
Copyright (C) 2024  Michael Ablassmeier <abi@grinser.de>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package main

import (
	"crypto/sha256"
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	bps "github.com/elbandi/go-proxmox-backup-client"
)

const (
	fingerprint = "55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4"
)

var sums []string

func backup(id string, backupTime time.Time, repo string, password string, chunks uint64) {
	t := uint64(backupTime.Unix())
	client, err := bps.NewBackup(repo, "", id, t, password, fingerprint, "", "", false)
	if err != nil {
		log.Fatalln(err)
	}

	defer client.Close()
	log.Printf("Create config backup")
	err = client.AddConfig("test", []byte("config content"))
	if err != nil {
		log.Fatalln(err)
	}

	imgsize := bps.GetDefaultChunkSize() * chunks
	log.Printf("Create image with %d random chunks, size: %d", chunks, imgsize)
	image, err := client.RegisterImage("test", imgsize)
	if err != nil {
		log.Fatalln(err)
	} else {
		var cnt uint64 = 0
		for cnt = 0; cnt < chunks; cnt++ {
			data := make([]byte, bps.GetDefaultChunkSize())
			for i := range data {
				data[i] = byte(rand.Intn(256))
			}
			sum := sha256.Sum256(data)
			sums = append(sums, string(sum[:]))
			off := bps.GetDefaultChunkSize() * uint64(cnt)
			wlen, err := image.WriteAt(data, int64(off))
			if wlen != len(data) {
				log.Fatalf("short write during backup")
			}
			if err != nil {
				log.Fatalln(err)
			}
		}
		image.Close()
	}
	err = client.Finish()
	if err != nil {
		log.Fatalln(err)
	}
}

func restore(id string, backupTime time.Time, repo string, password string) {
	t := uint64(backupTime.Unix())
	client, err := bps.NewRestore(repo, "vm", "vm", id, t, password, fingerprint, "", "")
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()
	image, err := client.OpenImage("test.img.fidx")
	if err != nil {
		log.Fatalln(err)
	} else {
		size, _ := image.Size()
		log.Printf("Image size was: %d", size)
		log.Println("Comparing chunks")
		var cnt uint64 = 0
		for cnt = 0; cnt < size/bps.GetDefaultChunkSize(); cnt++ {
			data := make([]byte, bps.GetDefaultChunkSize())
			off := bps.GetDefaultChunkSize() * uint64(cnt)
			rlen, err := image.ReadAt(data, int64(off))
			if rlen != len(data) {
				log.Fatalf("short read during restore")
			}
			if err != nil {
				log.Fatal(err)
			}
			sum := sha256.Sum256(data)
			if sums[cnt] != string(sum[:]) {
				log.Fatalf("Checksum for restored block does not match: %s", string(sum[:]))
			}
		}
		log.Printf("All restored checksums match")
	}
}

func main() {
	repoFlag := flag.String("repo", "", "Endpoint address")
	pwFlag := flag.String("password", "", "Password")
	lenFlag := flag.Uint64("len", 10, "Amount of chunks")
	flag.Parse()
	if *repoFlag == "" || *pwFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	log.Println(bps.GetVersion())
	t := time.Now()
	log.Println("Create fixed index backup")
	backup("testbackup", t, *repoFlag, *pwFlag, *lenFlag)
	log.Println("Restore fixed index backup")
	restore("testbackup", t, *repoFlag, *pwFlag)
}
