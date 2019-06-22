package pgxmgr

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx"
	"github.com/usrpro/dotpgx"
	log "gopkg.in/inconshreveable/log15.v2"
)

var filesExp = []file{
	{
		name:  "tests/files/00-00-0000-file.sql",
		major: 0,
		minor: 0,
		fix:   0,
	},
	{
		name:  "tests/files/00-00-0001-file.sql",
		major: 0,
		minor: 0,
		fix:   1,
	},
	{
		name:  "tests/files/00-02-0000-file.sql",
		major: 0,
		minor: 2,
		fix:   0,
	},
	{
		name:  "tests/files/03-00-0000-file.sql",
		major: 3,
		minor: 0,
		fix:   0,
	},
	{
		name:  "tests/files/03-01-0000-file.sql",
		major: 3,
		minor: 1,
		fix:   0,
	},
}

func TestListFiles(t *testing.T) {
	_, err := listFiles("nothing")
	if err == nil {
		t.Fatal("Expected an error form listFiles")
	}
	got, err := listFiles("tests/files")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filesExp, got) {
		t.Fatal(
			"Files lists not equal\nExpected:\n",
			filesExp,
			"\nGot:\n",
			got,
		)
	}
}

func clean() {
	if _, err := db.Pool.Exec("drop table peers;"); err != nil {
		log.Crit("Cleanup error", "Error", err)
	}
	if _, err := db.Pool.Exec("drop table schema_version;"); err != nil {
		log.Crit("Cleanup error", "Error", err)
	}
}

func isApplied(f file) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	return f.skip(tx)
}

func dumpVersion() (files []file) {
	rows, _ := db.Pool.Query("select * from schema_version;")
	for rows.Next() {
		var f file
		if err := rows.Scan(&f.major, &f.minor, &f.fix); err != nil {
			log.Crit("dumpVersion", "error", err)
			return
		}
		files = append(files, f)
	}
	return
}

type peer struct {
	name  string
	email string
	nick  string
}

var conf = pgx.ConnPoolConfig{
	ConnConfig: pgx.ConnConfig{
		Host:     "/run/postgresql",
		User:     "postgres",
		Database: "migrate_test",
	},
	MaxConnections: 5,
}

var db *dotpgx.DB

var expMgr = []file{
	{
		major: 0,
		minor: 0,
		fix:   0,
	},
	{
		major: 0,
		minor: 0,
		fix:   1,
	},
	{
		major: 0,
		minor: 1,
		fix:   0,
	},
}

var expErr = file{
	major: 1,
	minor: 0,
	fix:   0,
}

func TestRun(t *testing.T) {
	var err error
	db, err = dotpgx.New(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer clean()
	if err = Run(db, "tests/migrations1"); err != nil {
		t.Fatal(err)
	}
	for _, f := range expMgr[:2] {
		if a, err := isApplied(f); !a || err != nil {
			t.Fatal(
				"Migration 1 not applied\nExpected:\n",
				expMgr[:2],
				"\nGot:\n",
				dumpVersion(),
			)
		}
	}
	if err = Run(db, "tests/migrations-err"); err == nil {
		t.Fatal("Expected an error")
	} else {
		log.Debug("Migration error test", "error", err)
	}
	if a, err := isApplied(expErr); err != nil {
		t.Fatal("Error in isApplied", err)
	} else if a {
		t.Fatal(
			"Errored migration applied\nExpected:\n",
			expMgr[:2],
			"\nGot:\n",
			dumpVersion(),
		)
	}
	if err = Run(db, "tests/migrations2"); err != nil {
		t.Fatal(err)
	}
	for _, f := range expMgr {
		if a, err := isApplied(f); !a || err != nil {
			t.Fatal(
				"Migration 2 not applied\nExpected:\n",
				expMgr,
				"\nGot:\n",
				dumpVersion(),
			)
		}
	}
	// Final check: attempt to insert & select on resulting table
	if err = db.ParseSQL(
		strings.NewReader(`
			--name: insert-peer
			insert into peers (name, email, nick) values ($1, $2, $3);
			--name: select-peer
			select name, email, nick from peers where nick = $1;
		`),
	); err != nil {
		t.Fatal(err)
	}
	exp := peer{
		name:  "Mickey Mouse",
		email: "mandm@disney.com",
		nick:  "mandm",
	}
	if _, err = db.Exec("insert-peer", exp.name, exp.email, exp.nick); err != nil {
		t.Fatal(err)
	}
	row, err := db.QueryRow("select-peer", "mandm")
	if err != nil {
		t.Fatal(err)
	}
	got := peer{}
	if err = row.Scan(&got.name, &got.email, &got.nick); err != nil {
		t.Fatal(err)
	}
	if exp != got {
		t.Fatal("Final check\nExpected:\n", exp, "\nGot:\n", got)
	}

}
