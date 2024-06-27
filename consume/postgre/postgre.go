package postgre

import (
	"database/sql"

	_ "github.com/lib/pq"
)

var PostgreDB *sql.DB

func Init() error {
	var err error
	PostgreDB, err = sql.Open("postgres", "host=192.168.2.188 port=5433 user=jpc password=123456 dbname=spider sslmode=disable")
	if err != nil {
		return err
	}
	return nil
}