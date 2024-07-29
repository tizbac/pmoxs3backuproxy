package main

import (
	"log"
)

var Gdebug = false

func debugPrint(fmt string, args ...interface{}) {
	if Gdebug {
		log.Printf("[\033[34;1mDEBG\033[0m]"+fmt, args...)
	}
}

func infoPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[37;1mINFO\033[0m]"+fmt, args...)
}

func errorPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[31;1mERR \033[0m]"+fmt, args...)
}

func warnPrint(fmt string, args ...interface{}) {
	log.Printf("[\033[33;1mWARN\033[0m]"+fmt, args...)
}
