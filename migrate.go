/*
Package migrate loads and executes the migration scripts on a connection aquired from a pgx pool.
All migrations are run incrementaly in seperate transaction blocks.
Execution is terminated when ay migration fails and a rollback is performed to the beginning
of this failing migration. Any previous migrations will already be committed.
*/
package migrate

import (
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jackc/pgx"
)

const create_rev_table string = `
	CREATE TABLE IF NOT EXISTS public.schema_version (
		major int NOT NULL,
		minor int NOT NULL,
		fix int NOT NULL,
		CONSTRAINT schema_version_pkey PRIMARY KEY (major,minor,fix)
	);
`

const insert_rev string = `
	INSERT INTO schema_version (major, minor, fix)
	VALUES ( $1, $2, $3);
`

// Run loads and executes the migrations.
// The first argument needs to be an instance of a configured pgx connection pool.
// The second argument should be directory where the migration files are loaded from.
// Files with the signature of ##-##-####-<name>.sql will be loaded and executed in order.
// The three number groups stand for major, minor and fix version.
func Run(pool *pgx.ConnPool, path string) (err error) {
	queries, err := loadQueries(path)
	if err != nil {
		return
	}

	conn, err := pool.Acquire()
	defer pool.Release(conn)
	if err != nil {
		return
	}

	_, err = conn.Exec(create_rev_table)
	if err != nil {
		return
	}

	for _, q := range queries {
		err = exec(conn, q)
		if err != nil {
			return err
		}
	}
	return
}

type query struct {
	major  int
	minor  int
	fix    int
	script string
}

func loadQueries(path string) (queries []query, err error) {
	// TODO: improve glob pattern
	names, err := filepath.Glob(path + "/*.sql")
	if err != nil {
		return
	}
	for _, name := range names {
		v := strings.SplitN(name, "-", 3)
		major, err := strconv.Atoi(v[0])
		minor, err := strconv.Atoi(v[1])
		fix, err := strconv.Atoi(v[2])
		if err != nil {
			return nil, err
		}
		script, err := ioutil.ReadFile(name)
		if err != nil {
			return nil, err
		}
		q := query{
			major:  major,
			minor:  minor,
			fix:    fix,
			script: string(script),
		}
		queries = append(queries, q)
	}
	return
}

func exec(conn *pgx.Conn, q query) (err error) {
	tx, err := conn.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(insert_rev, q.major, q.minor, q.fix)
	if err != nil {
		return
	}

	_, err = tx.Exec(q.script)
	if err != nil {
		return
	}

	return tx.Commit()
}
