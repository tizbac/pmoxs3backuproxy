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
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (S *Snapshot) initWithQuery(v url.Values) {
	S.BackupID = v.Get("backup-id")
	S.BackupTime, _ = strconv.ParseUint(v.Get("backup-time"), 10, 64)
	S.BackupType = v.Get("backup-type")
	S.Protected = false
}

func (S *Snapshot) S3Prefix() string {
	return fmt.Sprintf("backups/%s|%d|%s", S.BackupID, S.BackupTime, S.BackupType)
}

func main() {
	certFlag := flag.String("cert", "server.crt", "Server SSL certificate file")
	keyFlag := flag.String("key", "server.key", "Server SSL key file")
	endpointFlag := flag.String("endpoint", "", "S3 Endpoint without https/http , host:port")
	bindAddress := flag.String("bind", "127.0.0.1:8007", "PBS Protocol bind address, recommended 127.0.0.1:8007, use :8007 for all")

	debug := flag.Bool("debug", false, "Debug logging")
	flag.Parse()
	if *endpointFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	Gdebug = *debug
	srv := &http.Server{Addr: *bindAddress, Handler: &Server{Auth: make(map[string]TicketEntry), S3Endpoint: *endpointFlag}}
	srv.SetKeepAlivesEnabled(true)
	infoPrint("Starting PBS api server on %s , upstream: %s", *bindAddress, *endpointFlag)
	err := srv.ListenAndServeTLS(*certFlag, *keyFlag)
	if err != nil {
		panic(err)
	}
}

func (s *Server) listSnapshots(c minio.Client, datastore string) ([]Snapshot, error) {
	resparray := make([]Snapshot, 0)
	prefixMap := make(map[string]*Snapshot)
	ctx := context.Background()
	for object := range c.ListObjects(ctx, datastore, minio.ListObjectsOptions{Recursive: true, Prefix: "backups/"}) {
		//log.Println(object.Key)
		//The object name is backupid|unixtimestamp|type
		if strings.Count(object.Key, "/") == 2 {
			path := strings.Split(object.Key, "/")
			fields := strings.Split(path[1], "|")
			existing_S, ok := prefixMap[path[1]]
			if ok {
				//log.Println(path)
				if len(path) == 3 {
					existing_S.Files = append(existing_S.Files, path[2])
				}
				continue
			}
			backupid := fields[0]
			backuptime := fields[1]
			backuptype := fields[2]
			backuptimei, _ := strconv.ParseUint(backuptime, 10, 64)
			S := Snapshot{
				BackupID:   backupid,
				BackupTime: backuptimei,
				BackupType: backuptype,
				Files:      make([]string, 0),
			}
			if len(path) == 3 {
				S.Files = append(S.Files, path[2])
			}

			resparray = append(resparray, S)
			prefixMap[path[1]] = &resparray[len(resparray)-1]

		}
	}
	return resparray, ctx.Err()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cookiere := regexp.MustCompile(`PBSAuthCookie=([^;]+)`)
	matches := cookiere.FindStringSubmatch(r.Header.Get("Cookie"))
	auth := false

	C := TicketEntry{}
	if len(matches) >= 2 {
		C, auth = s.Auth[matches[1]]
	}

	debugPrint("Request:" + r.RequestURI)
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
			resparray, err := s.listSnapshots(*C.Client, ds)
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
		infoPrint("Handling get request for previous (%s)", r.URL.Query().Get("archive-name"))
		snapshots, err := s.listSnapshots(*s.H2Ticket.Client, *s.SelectedDataStore)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			errorPrint(err.Error())
			return
		}

		var mostRecent *Snapshot
		for _, s := range snapshots {
			if mostRecent == nil || s.BackupTime > mostRecent.BackupTime {
				mostRecent = &s
			}
		}

		if mostRecent == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		for _, f := range mostRecent.Files {
			if f == r.URL.Query().Get("archive-name") {
				obj, err := s.H2Ticket.Client.GetObject(
					context.Background(),
					*s.SelectedDataStore,
					mostRecent.S3Prefix()+"/"+f,
					minio.GetObjectOptions{},
				)
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					errorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f)
					io.WriteString(w, err.Error())
					return
				}
				s, err := obj.Stat()
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					errorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f)
					io.WriteString(w, err.Error())
					return
				}
				w.Header().Add("Content-Length", fmt.Sprintf("%d", s.Size))
				w.WriteHeader(http.StatusOK)

				io.Copy(w, obj)
				return
			}
		}

		warnPrint("File %s not found in snapshot %s (%s)", r.URL.Query().Get("archive-name"), mostRecent.S3Prefix(), mostRecent.Files)

		w.WriteHeader(http.StatusNotFound)
		return
	}
	if strings.HasPrefix(r.RequestURI, "/fixed_index?") && s.H2Ticket != nil && r.Method == "POST" {
		fidxname := r.URL.Query().Get("archive-name")
		reusecsum := r.URL.Query().Get("reuse-csum")
		size := r.URL.Query().Get("size")
		S, _ := strconv.ParseUint(size, 10, 64)
		infoPrint("Archive name : %s, size: %s\n", fidxname, size)
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
				errorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			outFile, err = io.ReadAll(obj)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				errorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			//Chunk size and size cannot be known since incremental may potentially upload 0 chunks, so we take them from reused index
			s.Writers[int32(wid)].Size = binary.LittleEndian.Uint64(outFile[64:72])
			s.Writers[int32(wid)].Chunksize = binary.NativeEndian.Uint64(outFile[72:80])

			debugPrint("Reusing old index")
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
					errorPrint("Backup failed because of hole in fixed index")
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
			errorPrint("%s failed to upload to S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
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
			errorPrint("%s failed to make a copy of the index on S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
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
			errorPrint("%s: Digest list and Offset list size does not match", s.Writers[req.Wid].FidxName)
			return
		}
		for i := 0; i < len(req.DigestList); i++ {
			if req.OffsetList[i]%s.Writers[req.Wid].Chunksize != 0 {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, "Chunk offset not at chunk-size boundary")
				errorPrint("%s: Chunk offset not at chunk-size boundary", s.Writers[req.Wid].FidxName)
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
			debugPrint("%s already in S3", digest)
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
			errorPrint("%s failed to upload blob to S3 bucket: %s", blobname, err.Error())
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
			errorPrint(err.Error() + " " + s.Snapshot.S3Prefix() + "/" + blobname)
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
			errorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
			return
		}
		st, err := obj.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			errorPrint(err.Error() + " " + s3name)
			io.WriteString(w, err.Error())
			errorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
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
		debugPrint("List buckets")
		bckts, err := C.Client.ListBuckets(context.Background())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			errorPrint("Failed to list buckets: %s", err.Error())
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
		ss := Snapshot{}
		ss.initWithQuery(r.URL.Query())
		go s.backup(conn, C, r.URL.Query().Get("store"), ss)
	}

	if strings.HasPrefix(r.RequestURI, "//api2/json/reader") && auth {
		w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
		w.WriteHeader(http.StatusSwitchingProtocols)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack() //Here SSL/TCP connection is deowned from the HTTP1.1 server and passed to HTTP2 handler after sending headers telling the client that we are switching protocols
		ss := Snapshot{}
		ss.initWithQuery(r.URL.Query())
		go s.restore(conn, C, r.URL.Query().Get("store"), ss)
	}

	if (r.RequestURI == "//api2/json/access/ticket" || r.RequestURI == "/api2/json/access/ticket") && r.Method == "POST" {
		req := AccessTicketRequest{}
		if r.Header.Get("Content-Type") == "application/json" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				errorPrint(err.Error())
				return
			}
			json.Unmarshal(body, &req)
		} else {
			err := r.ParseForm()
			for name, values := range r.Header {
				// Loop over all values for the name.
				for _, value := range values {
					debugPrint("%s=%s", name, value)
				}
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				errorPrint("Failed to parse form: %s", err.Error())
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

		te := TicketEntry{
			AccessKeyID:     strings.Split(req.Username, "@")[0],
			SecretAccessKey: req.Password,
			Endpoint:        s.S3Endpoint,
		}
		minioClient, err := minio.New(te.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(te.AccessKeyID, te.SecretAccessKey, ""),
			Secure: false,
		})
		if err != nil {
			//w.Header().Add("Connection", "Close")
			errorPrint(err.Error())
			warnPrint("Failed S3 Connection: %s", err.Error())
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(err.Error()))
		}

		te.Client = minioClient

		s.Auth[ticket.Ticket] = te

		respbody, _ := json.Marshal(resp)
		debugPrint(string(respbody))
		w.Header().Add("Content-Type", "application/json")
		//w.Header().Add("Connection", "Close")
		w.WriteHeader(http.StatusOK)
		w.Write(respbody)
	}
}
