package dbhelper

//mssql_db.go a way to load msi.Msi for MSSQL

import (
	"fmt"
	"strconv"

	"github.com/ws6/msi"
)

func getValueByPrefixKey(cfg map[string]string, key string) string {
	if prefix, ok := cfg[`msdb_prefix`]; ok {
		return cfg[fmt.Sprintf(`%s.%s`, prefix, key)]
	}

	return cfg[key]
}

func MakeConnectionString(cfg map[string]string) string {

	return fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s",
		getValueByPrefixKey(cfg, `host`),
		getValueByPrefixKey(cfg, `database`),
		getValueByPrefixKey(cfg, `user`),
		getValueByPrefixKey(cfg, `password`),
	)
}

func GetMSDB(cfg map[string]string) (*msi.Msi, error) {
	ret, err := msi.NewDb(msi.MSSQL, MakeConnectionString(cfg), getValueByPrefixKey(cfg, `database`), ``)
	if err != nil {
		return nil, err
	}
	maxConn := 2
	maxConnStr := getValueByPrefixKey(cfg, `max_connection`)
	if n, err := strconv.Atoi(maxConnStr); err == nil && n > 0 {
		maxConn = n
	}
	ret.Db.SetMaxOpenConns(maxConn)

	return ret, nil
}
