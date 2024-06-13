package main

import (
	"log"
	"time"
	_ "time/tzdata" //สำหรับ LoadLocation

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/routex"
	"gitlab.com/dohome-2020/go-servicex/mainx"
	// _ "github.com/denisenkom/go-mssqldb" // Register mssql driver.
)

func main() {

	// init mainx
	rx, ex := mainx.Init()
	if ex != nil {
		log.Fatalf(`main.Init: %s`, ex.Error())
	}
	time.Local = rx.Location

	// Cleanups
	defer mainx.Cleanup()

	// // kafkax
	// time.AfterFunc(time.Second*3, func() {
	// 	kafkax.Starter(config.GetServiceName(), runners.GetRunners())
	// })

	// // crons
	// time.AfterFunc(time.Second*4, func() {
	// 	crons.Starter()
	// })

	// routers
	routex.Routex()

}
