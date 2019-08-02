package main

import (
	"github.com/fulldump/goconfig"
	"github.com/inconshreveable/log15"
	"github.com/usrpro/dotpgx"
)

type config struct {
	Migrations string `usage:"Path to migrations scripts"`
	Database   dotpgx.Config
}

// Default config
var root = config{
	Migrations: "migrations",
	Database:   dotpgx.Default,
}

var (
	// Migrations script path
	Migrations = &root.Migrations
	// Database configuration
	Database = &root.Database
)

func init() {
	Database.Name = "migrate_test" //default for this package
	goconfig.Read(&root)
	log15.Info("Configuration loaded", "root", root)
}
