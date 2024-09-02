package s3backuplog

import (
	"log"
	"os"
)

var flags = log.Ldate | log.Lshortfile
var logger = log.New(os.Stdout, "", flags)
var Gdebug = false

func EnableDebug() {
	Gdebug = true
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

func DebugPrint(fmt string, args ...interface{}) {
	if Gdebug {
		log.Printf("[\033[34;1m DEBUG \033[0m] "+fmt, args...)
	}
}

func InfoPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[37;1m  INFO \033[0m] "+fmt, args...)
}

func ErrorPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[31;1m ERROR \033[0m] "+fmt, args...)
}

func WarnPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[33;1mWARNING\033[0m] "+fmt, args...)
}

func FatalPrint(fmt string, args ...interface{}) {
	log.Fatalf("[\033[31;1m FATAL \033[0m] "+fmt, args...)
}
