package main

import (
	"os"

	"github.com/inconshreveable/log15"
	"github.com/usrpro/dotpgx"
	"github.com/usrpro/pgxmgr"
)

func run() (err error) {
	db, err := dotpgx.InitDB(*Database, "")
	if err != nil {
		return
	}
	defer db.Close()

	if err = pgxmgr.Run(db, *Migrations); err != nil {
		return
	}
	return
}

func main() {
	if err := run(); err != nil {
		log15.Crit("Migrations failed", "err", err)
		os.Exit(2)
	}
}
