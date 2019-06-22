/*
Package pgxmgr loads and executes the migration scripts on a connection aquired from a pgx pool.
All migrations are run incrementaly in seperate transaction blocks.
Execution is terminated when ay migration fails and a rollback is performed to the beginning
of this failing migration. Any previous migrations will already be committed.
*/
package pgxmgr

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"

	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/jackc/pgx"

	"github.com/usrpro/dotpgx"
)

const createRevTable string = `
	CREATE TABLE IF NOT EXISTS schema_version (
		major int NOT NULL,
		minor int NOT NULL,
		fix int NOT NULL,
		CONSTRAINT schema_version_pkey PRIMARY KEY ( major , minor, fix)
	);
`

const insertRev string = `
	INSERT INTO schema_version (major, minor, fix)
	VALUES ( $1, $2, $3);
`

const checkRev string = `
	SELECT true::bool FROM schema_version
	WHERE
		major = $1
	AND
		minor = $2
	AND
		fix = $3
	;
`

// Run loads and executes the migrations.
// The first argument needs to be an instance of a configured pgx connection pool.
// The second argument should be directory where the migration files are loaded from.
// Files with the signature of ##-##-####-<name>.sql will be loaded and executed in order.
// The three number groups stand for major, minor and fix version.
func Run(db *dotpgx.DB, path string) (err error) {
	_, err = db.Pool.Exec(createRevTable)
	if err != nil {
		return
	}
	files, err := listFiles(path)
	if err != nil {
		return
	}
	// TODO: error in map was not empty.
	if err = db.ClearMap(); err != nil {
		return
	}
	for _, f := range files {
		err = exec(db, f)
		if err != nil {
			return
		}
	}
	return
}

type file struct {
	name  string
	major int
	minor int
	fix   int
}

func (f *file) skip(tx *dotpgx.Tx) (b bool, err error) {
	r := tx.Ptx.QueryRow(checkRev, f.major, f.minor, f.fix)
	if err = r.Scan(&b); err != nil {
		if err == pgx.ErrNoRows {
			return b, nil
		}
	}
	return
}

func listFiles(path string) (files []file, err error) {
	// TODO: improve glob pattern
	g := strings.Join([]string{path, "/*.sql"}, "")
	names, err := filepath.Glob(g)
	if err != nil {
		return
	}
	if len(names) == 0 {
		return nil, errors.New("No migration files loaded")
	}
	for _, name := range names {
		f := file{name: name}
		n := strings.Split(name, "/")
		v := strings.Split(n[len(n)-1], "-")
		if f.major, err = strconv.Atoi(v[0]); err != nil {
			return
		}
		if f.minor, err = strconv.Atoi(v[1]); err != nil {
			return
		}
		if f.fix, err = strconv.Atoi(v[2]); err != nil {
			return
		}
		files = append(files, f)
	}
	return
}

func exec(db *dotpgx.DB, f file) (err error) {
	log.Info("Migration exec", "parse", f)
	if err = db.ParseFiles(f.name); err != nil {
		return
	}
	defer db.ClearMap()
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	var skip bool
	if skip, err = f.skip(tx); skip || err != nil {
		log.Info("Migration exec", "skip", f)
		return
	}
	log.Info("Migration exec", "start", f)
	if _, err = tx.Ptx.Exec(insertRev, f.major, f.minor, f.fix); err != nil {
		return
	}
	for _, q := range db.List() {
		if _, err = tx.Exec(q); err != nil {
			return
		}
	}
	err = tx.Commit()
	return
}
