/*
PMOX S3 Backup Proxy
Copyright (C) 2024  Tiziano Bacocco

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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var connectionList = make(map[string]*minio.Client)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func main() {
	certFlag := flag.String("cert", "server.crt", "Server SSL certificate file")
	keyFlag := flag.String("key", "server.key", "Server SSL key file")
	endpointFlag := flag.String("endpoint", "", "S3 Endpoint without https/http , host:port")
	bindAddress := flag.String("bind", "127.0.0.1:8007", "PBS Protocol bind address, recommended 127.0.0.1:8007, use :8007 for all")
	insecureFlag := flag.Bool("usessl", false, "Use SSL for endpoint connection: default: false")
	ticketExpireFlag := flag.Uint64("ticketexpire", 3600, "API Ticket expire time in seconds")
	debug := flag.Bool("debug", false, "Debug logging")
	flag.Parse()
	if *endpointFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *debug {
		s3backuplog.EnableDebug()
	}
	S := &Server{Auth: make(map[string]TicketEntry), S3Endpoint: *endpointFlag, SecureFlag: *insecureFlag, TicketExpire: *ticketExpireFlag}
	srv := &http.Server{Addr: *bindAddress, Handler: S}
	srv.SetKeepAlivesEnabled(true)
	go S.ticketGC()
	s3backuplog.InfoPrint("Starting PBS api server on [%s], upstream: [%s] ssl: [%t]", *bindAddress, *endpointFlag, *insecureFlag)

	err := srv.ListenAndServeTLS(*certFlag, *keyFlag)
	if err != nil {
		panic(err)
	}
}

func (s *Server) ticketGC() {
	for {
		expireKeys := make([]string, 0)
		now := uint64(time.Now().Unix())
		for k, v := range s.Auth {
			if v.Expire < now {
				expireKeys = append(expireKeys, k)
			}
		}
		s3backuplog.DebugPrint("Expire %d tickets", len(expireKeys))
		for _, k := range expireKeys {
			delete(s.Auth, k)
			s3backuplog.DebugPrint("Expired ticket %s", k)
		}
		time.Sleep(30 * time.Second)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cookiere := regexp.MustCompile(`PBSAuthCookie=([^;]+)`)
	matches := cookiere.FindStringSubmatch(r.Header.Get("Cookie"))
	auth := false

	C := TicketEntry{}
	if len(matches) >= 2 {
		C, auth = s.Auth[matches[1]]
	}

	s3backuplog.DebugPrint("Request:" + r.RequestURI)
	path := strings.Split(r.RequestURI, "/")

	if strings.HasPrefix(r.RequestURI, "/dynamic") && s.H2Ticket != nil && r.Method == "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("not implemented"))
		return
	}

	if len(path) >= 7 && strings.HasPrefix(r.RequestURI, "/api2/json/admin/datastore/") && auth {
		ds := path[5]
		action := path[6]
		if action == "status" {
			//Seems to not be supported by minio fecthing used size so we return dummy values to make all look fine
			resp, _ := json.Marshal(Response{
				Data: DataStoreStatus{
					Used:  10000,
					Avail: 10000000,
					Total: 10000 + 10000000,
				},
			})
			w.Header().Add("Content-Type", "application/json")
			w.Write(resp)
		}

		if strings.HasPrefix(action, "snapshots") {
			resparray, err := s3pmoxcommon.ListSnapshots(*C.Client, ds, false)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, err.Error())
			}
			resp, _ := json.Marshal(Response{
				Data: resparray,
			})
			w.Header().Add("Content-Type", "application/json")
			w.Write(resp)
		}

	}
	/*Backup HTTP2 Api*/
	if strings.HasPrefix(r.RequestURI, "/finish") && s.H2Ticket != nil && r.Method == "POST" {

		s.Finished = true
	}
	if strings.HasPrefix(r.RequestURI, "/previous?") && s.H2Ticket != nil && r.Method == "GET" {
		s3backuplog.InfoPrint("Handling get request for previous (%s)", r.URL.Query().Get("archive-name"))
		snapshots, err := s3pmoxcommon.ListSnapshots(*s.H2Ticket.Client, *s.SelectedDataStore, false)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			s3backuplog.ErrorPrint(err.Error())
			return
		}

		var mostRecent *s3pmoxcommon.Snapshot
		for _, sl := range snapshots {
			if (mostRecent == nil || sl.BackupTime > mostRecent.BackupTime) && s.Snapshot.BackupID == sl.BackupID {
				mostRecent = &sl
			}
		}

		if mostRecent == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		for _, f := range mostRecent.Files {
			if f.Filename == r.URL.Query().Get("archive-name") {
				obj, err := s.H2Ticket.Client.GetObject(
					context.Background(),
					*s.SelectedDataStore,
					mostRecent.S3Prefix()+"/"+f.Filename,
					minio.GetObjectOptions{},
				)
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					s3backuplog.ErrorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f.Filename)
					io.WriteString(w, err.Error())
					return
				}
				s, err := obj.Stat()
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					s3backuplog.ErrorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f.Filename)
					io.WriteString(w, err.Error())
					return
				}
				w.Header().Add("Content-Length", fmt.Sprintf("%d", s.Size))
				w.WriteHeader(http.StatusOK)

				io.Copy(w, obj)
				return
			}
		}

		s3backuplog.WarnPrint("File %s not found in snapshot %s (%s)", r.URL.Query().Get("archive-name"), mostRecent.S3Prefix(), mostRecent.Files)

		w.WriteHeader(http.StatusNotFound)
		return
	}
	if strings.HasPrefix(r.RequestURI, "/fixed_index?") && s.H2Ticket != nil && r.Method == "POST" {
		fidxname := r.URL.Query().Get("archive-name")
		reusecsum := r.URL.Query().Get("reuse-csum")
		size := r.URL.Query().Get("size")
		S, _ := strconv.ParseUint(size, 10, 64)
		s3backuplog.InfoPrint("Archive name : %s, size: %s\n", fidxname, size)
		wid := atomic.AddInt32(&s.CurWriter, 1)
		resp, _ := json.Marshal(Response{
			Data: wid,
		})

		s.Writers[wid] = &Writer{Assignments: make(map[int64][]byte), FidxName: fidxname, Size: S, ReuseCSUM: reusecsum}
		w.Header().Add("Content-Type", "application/json")
		w.Write(resp)
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_close?") && s.H2Ticket != nil && r.Method == "POST" {
		wid, _ := strconv.ParseInt(r.URL.Query().Get("wid"), 10, 32)
		csumindex, _ := hex.DecodeString(r.URL.Query().Get("csum"))
		outFile := make([]byte, 0)
		//FIDX format is documented on Proxmox Backup docs pdf
		if s.Writers[int32(wid)].ReuseCSUM != "" {
			//In that case we load from S3 the specified reuse index
			obj, err := s.H2Ticket.Client.GetObject(
				context.Background(),
				*s.SelectedDataStore,
				"indexed/"+s.Writers[int32(wid)].ReuseCSUM+".fidx",
				minio.GetObjectOptions{},
			)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				s3backuplog.ErrorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			outFile, err = io.ReadAll(obj)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				s3backuplog.ErrorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			//Chunk size and size cannot be known since incremental may potentially upload 0 chunks, so we take them from reused index
			s.Writers[int32(wid)].Size = binary.LittleEndian.Uint64(outFile[64:72])
			s.Writers[int32(wid)].Chunksize = binary.NativeEndian.Uint64(outFile[72:80])

			s3backuplog.DebugPrint("Reusing old index")
		} else {
			//In that case a new index is allocated, 4096 is the header, then size/chunksize blocks follow of 32 bytes ( chunk digest sha 256 )
			outFile = make([]byte, 4096+32*len(s.Writers[int32(wid)].Assignments))
			outFile[0], outFile[1], outFile[2], outFile[3], outFile[4], outFile[5], outFile[6], outFile[7] = 47, 127, 65, 237, 145, 253, 15, 205 //Header magic as per PBS docs
			//Chunksize in that case is derived from at least one chunk having been uploaded
			sl := binary.LittleEndian.AppendUint64(make([]byte, 0), s.Writers[int32(wid)].Size)
			copy(outFile[64:72], sl)
			sl = binary.LittleEndian.AppendUint64(make([]byte, 0), s.Writers[int32(wid)].Chunksize)
			copy(outFile[72:80], sl)

		}
		copy(outFile[32:64], csumindex[0:32]) //Checksum is almost never the same , so it is changed with new backup
		u := uuid.New()                       //Generate a new uuid too
		b, _ := u.MarshalBinary()
		copy(outFile[8:24], b)
		sl := binary.LittleEndian.AppendUint64(make([]byte, 0), uint64(time.Now().Unix()))
		copy(outFile[24:32], sl)
		k := uint64(0)

		for i := uint64(0); i < s.Writers[int32(wid)].Size; i += s.Writers[int32(wid)].Chunksize {
			val, ok := s.Writers[int32(wid)].Assignments[int64(i)]
			if !ok {
				if s.Writers[int32(wid)].ReuseCSUM == "" {
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, "Hole in index")
					s3backuplog.ErrorPrint("Backup failed because of hole in fixed index")
					return
				}
			} else {
				//4096 bytes is the header, being each element 32 bytes (sha256) after the header
				copy(outFile[4096+32*k:4096+32*k+32], val)
			}

			k++
		}
		R := bytes.NewReader(outFile)
		_, err := s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+s.Writers[int32(wid)].FidxName,
			R,
			int64(len(outFile)),
			minio.PutObjectOptions{
				UserMetadata: map[string]string{"csum": r.URL.Query().Get("csum")},
			},
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload to S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}
		//This copy of the object is later used to lookup when reuse-csum is in play ( incremental backup )
		//It will waste a bit of space, but indexes overall are much smaller than actual data , so for now is a price that can be paid to avoid going thru all the files
		_, err = s.H2Ticket.Client.CopyObject(
			context.Background(),
			minio.CopyDestOptions{Bucket: *s.SelectedDataStore, Object: "indexed/" + r.URL.Query().Get("csum") + ".fidx"},
			minio.CopySrcOptions{Bucket: *s.SelectedDataStore, Object: s.Snapshot.S3Prefix() + "/" + s.Writers[int32(wid)].FidxName},
		)
		/*_, err = s.H2Ticket.Client.PutObject(context.Background(), *s.SelectedDataStore, "indexed/"+r.URL.Query().Get("csum"), R, int64(len(outFile)), minio.PutObjectOptions{UserMetadata: map[string]string{"csum": r.URL.Query().Get("csum")}})
		 */
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to make a copy of the index on S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_index") && s.H2Ticket != nil && r.Method == "PUT" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		req := AssignmentRequest{}
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		if len(req.DigestList) != len(req.OffsetList) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "Digest list and Offset list size does not match")
			s3backuplog.ErrorPrint("%s: Digest list and Offset list size does not match", s.Writers[req.Wid].FidxName)
			return
		}
		for i := 0; i < len(req.DigestList); i++ {
			if req.OffsetList[i]%s.Writers[req.Wid].Chunksize != 0 {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, "Chunk offset not at chunk-size boundary")
				s3backuplog.ErrorPrint("%s: Chunk offset not at chunk-size boundary", s.Writers[req.Wid].FidxName)
				return
			}
		}

		for i := 0; i < len(req.DigestList); i++ {
			b, err := hex.DecodeString(req.DigestList[i])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, err.Error())
			}
			s.Writers[req.Wid].Assignments[int64(req.OffsetList[i])] = b
		}
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_chunk?") {
		esize, _ := strconv.Atoi(r.URL.Query().Get("encoded-size"))
		size, _ := strconv.Atoi(r.URL.Query().Get("size"))
		digest := r.URL.Query().Get("digest")
		wid, _ := strconv.ParseInt(r.URL.Query().Get("wid"), 10, 32)
		s3name := fmt.Sprintf("chunks/%s/%s/%s", digest[0:2], digest[2:4], digest[4:])

		obj, err := s.H2Ticket.Client.GetObject(
			context.Background(),
			*s.SelectedDataStore,
			s3name,
			minio.GetObjectOptions{},
		)

		if err == nil {
			_, err = obj.Stat()
		}
		if err != nil {
			_, err := s.H2Ticket.Client.PutObject(
				context.Background(),
				*s.SelectedDataStore,
				s3name,
				r.Body,
				int64(esize),
				minio.PutObjectOptions{},
			)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}
		} else {
			s3backuplog.DebugPrint("%s already in S3", digest)
		}
		if s.Writers[int32(wid)].Chunksize == 0 {
			//Here chunk size is derived
			s.Writers[int32(wid)].Chunksize = uint64(size)
		}
	}

	if strings.HasPrefix(r.RequestURI, "/blob?") && s.H2Ticket != nil {
		blobname := r.URL.Query().Get("file-name")
		esize, _ := strconv.Atoi(r.URL.Query().Get("encoded-size"))
		_, err := s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+blobname, r.Body, int64(esize), minio.PutObjectOptions{},
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload blob to S3 bucket: %s", blobname, err.Error())
		}

	}
	/* End of HTTP2 Backup API */

	/* HTTP2 Restore API */
	if strings.HasPrefix(r.RequestURI, "/download?") && s.H2Ticket != nil {
		blobname := r.URL.Query().Get("file-name")
		obj, err := s.H2Ticket.Client.GetObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+blobname,
			minio.GetObjectOptions{},
		)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
		st, err := obj.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s3backuplog.ErrorPrint(err.Error() + " " + s.Snapshot.S3Prefix() + "/" + blobname)
			io.WriteString(w, err.Error())
			return
		}

		w.Header().Add("Content-Length", fmt.Sprintf("%d", st.Size))
		w.WriteHeader(http.StatusOK)

		io.Copy(w, obj)

	}

	if strings.HasPrefix(r.RequestURI, "/chunk?") && s.H2Ticket != nil {
		digest := r.URL.Query().Get("digest")
		s3name := fmt.Sprintf("chunks/%s/%s/%s", digest[0:2], digest[2:4], digest[4:])
		obj, err := s.H2Ticket.Client.GetObject(
			context.Background(),
			*s.SelectedDataStore,
			s3name,
			minio.GetObjectOptions{},
		)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
			return
		}
		st, err := obj.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s3backuplog.ErrorPrint(err.Error() + " " + s3name)
			io.WriteString(w, err.Error())
			s3backuplog.ErrorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
			return
		}

		w.Header().Add("Content-Length", fmt.Sprintf("%d", st.Size))
		w.WriteHeader(http.StatusOK)

		io.Copy(w, obj)
	}

	/* End of HTTP 2 Restore API */

	if r.RequestURI == "/api2/json/admin/datastore" && r.Method == "GET" && auth {
		/*for name, values := range r.Header {
			// Loop over all values for the name.
			for _, value := range values {
				fmt.Println(name, value)
			}
		}*/
		s3backuplog.DebugPrint("List buckets")
		bckts, err := C.Client.ListBuckets(context.Background())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			s3backuplog.ErrorPrint("Failed to list buckets: %s", err.Error())
			io.WriteString(w, err.Error())
			return
		}

		datastores := make([]DataStore, 0)

		for _, b := range bckts {
			datastores = append(datastores, DataStore{Store: b.Name})
		}

		r := Response{Data: datastores}
		responsedata, _ := json.Marshal(r)
		w.Header().Add("Content-Type", "application/json")
		w.Write(responsedata)

	}

	if strings.HasPrefix(r.RequestURI, "//api2/json/backup") && auth {
		w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
		w.WriteHeader(http.StatusSwitchingProtocols)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack() //Here SSL/TCP connection is deowned from the HTTP1.1 server and passed to HTTP2 handler after sending headers telling the client that we are switching protocols
		ss := s3pmoxcommon.Snapshot{}
		ss.InitWithQuery(r.URL.Query())
		go s.backup(conn, C, r.URL.Query().Get("store"), ss)
	}

	if strings.HasPrefix(r.RequestURI, "//api2/json/reader") && auth {
		w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
		w.WriteHeader(http.StatusSwitchingProtocols)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack() //Here SSL/TCP connection is deowned from the HTTP1.1 server and passed to HTTP2 handler after sending headers telling the client that we are switching protocols
		ss := s3pmoxcommon.Snapshot{}
		ss.InitWithQuery(r.URL.Query())
		go s.restore(conn, C, r.URL.Query().Get("store"), ss)
	}

	if (r.RequestURI == "//api2/json/access/ticket" || r.RequestURI == "/api2/json/access/ticket") && r.Method == "POST" {
		req := AccessTicketRequest{}
		if r.Header.Get("Content-Type") == "application/json" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				s3backuplog.ErrorPrint(err.Error())
				return
			}
			json.Unmarshal(body, &req)
		} else {
			err := r.ParseForm()
			for name, values := range r.Header {
				// Loop over all values for the name.
				for _, value := range values {
					s3backuplog.DebugPrint("%s=%s", name, value)
				}
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				s3backuplog.ErrorPrint("Failed to parse form: %s", err.Error())
				return
			}
			req.Password = r.FormValue("password")
			req.Username = r.FormValue("username")
		}
		ticket := AuthTicketResponsePayload{
			CSRFPreventionToken: "35h235h23yh23", //Not used at all being that used only for API
			Ticket:              RandStringBytes(64),
			Username:            req.Username,
		}
		resp := Response{Data: ticket}

		username := strings.Split(req.Username, "@")[0]
		te := TicketEntry{
			AccessKeyID:     username,
			SecretAccessKey: req.Password,
			Endpoint:        s.S3Endpoint,
			Expire:          uint64(time.Now().Unix()) + s.TicketExpire,
		}
		_, ok := connectionList[username]
		if ok {
			s3backuplog.DebugPrint("Re-using minio connection for username: %s", username)
		} else {
			s3backuplog.DebugPrint("Setup NEW minio connection for username: %s", username)
			minioClient, err := minio.New(te.Endpoint, &minio.Options{
				Creds:  credentials.NewStaticV4(te.AccessKeyID, te.SecretAccessKey, ""),
				Secure: s.SecureFlag,
			})
			if err != nil {
				s3backuplog.ErrorPrint("Failed S3 Connection: %s", err.Error())
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(err.Error()))
			}
			connectionList[username] = minioClient
		}
		te.Client = connectionList[username]
		s.Auth[ticket.Ticket] = te

		respbody, _ := json.Marshal(resp)
		s3backuplog.DebugPrint(string(respbody))
		w.Header().Add("Content-Type", "application/json")
		//w.Header().Add("Connection", "Close")
		w.WriteHeader(http.StatusOK)
		w.Write(respbody)
	}
}
