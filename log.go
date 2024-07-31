package main

import (
	"log"
	"os"
)

var flags = log.Ldate | log.Lshortfile
var logger = log.New(os.Stdout, "", flags)
var Gdebug = false

func debugPrint(fmt string, args ...interface{}) {
	if Gdebug {
		log.Printf("[\033[34;1mDEBUG\033[0m] "+fmt, args...)
	}
}

func infoPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[37;1mINFO\033[0m] "+fmt, args...)
}

func errorPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[31;1mERROR\033[0m] "+fmt, args...)
}

func warnPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[33;1mWARNING\033[0m] "+fmt, args...)
}
