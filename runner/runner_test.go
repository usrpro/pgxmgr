package main

import (
	"os"
	"strings"
	"testing"

	"github.com/inconshreveable/log15"
	"github.com/usrpro/dotpgx"
)

func TestMain(m *testing.M) {
	// This connection is only for cleaning up later
	db, err := dotpgx.InitDB(*Database, "")
	if err != nil {
		log15.Crit("DB connect error", "err", err)
	}

	code := m.Run()

	if _, err := db.Pool.Exec("drop table peers;"); err != nil {
		log15.Crit("Cleanup error", "err", err)
		code++
	}
	if _, err := db.Pool.Exec("drop table schema_version;"); err != nil {
		log15.Crit("Cleanup error", "err", err)
		code++
	}

	db.Close()
	os.Exit(code)
}

func TestRun(t *testing.T) {
	exp := "No migration files loaded"
	err := run()
	if err == nil || err.Error() != exp {
		t.Error("Expected error:", exp, "Got", err)
	}

	host := Database.Host
	Database.Host = "foobar"
	exp = "no such host"
	err = run()
	Database.Host = host
	if err == nil || !strings.HasSuffix(err.Error(), exp) {
		t.Error("Expected error:", exp, "Got", err)
	}

	root.Migrations = "../tests/migrations1"
	if err = run(); err != nil {
		t.Fatal(err)
	}
}

func TestMainF(t *testing.T) {
	root.Migrations = "../tests/migrations1"
	main()
}
