/*
*

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
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	bps "github.com/elbandi/go-proxmox-backup-client"
)

const (
	fingerprint = "55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4"
)

func backup(id string, backupTime time.Time, repo string, password string) {
	t := uint64(backupTime.Unix())
	client, err := bps.NewBackup(repo, "", id, t, password, fingerprint, "", "", false)
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
	defer client.Close()
	err = client.AddConfig("test", []byte("test2"))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	image, err := client.RegisterImage("test", 11)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	} else {
		//data, _ := io.ReadAll(resp.Body)
		image.WriteAt([]byte("testcontent"), 0)
		image.Close()
	}
	err = client.Finish()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func restore(id string, backupTime time.Time, repo string, password string) {
	t := uint64(backupTime.Unix())
	client, err := bps.NewRestore(repo, "vm", "vm", id, t, password, fingerprint, "", "")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
	defer client.Close()
	image, err := client.OpenImage("test.img.fidx")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	} else {
		data := make([]byte, 11)
		image.ReadAt(data, 0)
		fmt.Println(string(data))
	}
}

func main() {
	repoFlag := flag.String("repo", "", "Endpoint address")
	pwFlag := flag.String("password", "", "Password")
	flag.Parse()
	if *repoFlag == "" || *pwFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	fmt.Println(bps.GetVersion())
	t := time.Now()
	fmt.Println("Create fixed index backup")
	backup("testbackup", t, *repoFlag, *pwFlag)
	fmt.Println("Restore fixed index backup")
	restore("testbackup", t, *repoFlag, *pwFlag)
}
