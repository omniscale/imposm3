package postgis

import (
	"database/sql"
	"fmt"
	"strings"
)

func schemasFromConnectionParams(params string) (string, string) {
	parts := strings.Fields(params)
	var schema, backupSchema string
	for _, p := range parts {
		if strings.HasPrefix(p, "schema=") {
			schema = strings.Replace(p, "schema=", "", 1)
		} else if strings.HasPrefix(p, "backupschema=") {
			backupSchema = strings.Replace(p, "backupschema=", "", 1)
		}
	}
	if schema == "" {
		schema = "import"
	}
	if backupSchema == "" {
		backupSchema = "backup"
	}
	return schema, backupSchema
}

func prefixFromConnectionParams(params string) string {
	parts := strings.Fields(params)
	var prefix string
	for _, p := range parts {
		if strings.HasPrefix(p, "prefix=") {
			prefix = strings.Replace(p, "prefix=", "", 1)
			break
		}
	}
	if prefix == "" {
		prefix = "osm_"
	}
	if prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}
	return prefix
}

func tableExists(tx *sql.Tx, schema, table string) (bool, error) {
	var exists bool
	sql := fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`,
		table, schema)
	row := tx.QueryRow(sql)
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func dropTableIfExists(tx *sql.Tx, schema, table string) error {
	sql := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, schema, table)
	_, err := tx.Exec(sql)
	return err
}

func rollbackIfTx(tx **sql.Tx) {
	if *tx != nil {
		if err := tx.Rollback(); err != nil {
			log.Fatal("rollback failed", err)
		}
	}
}
