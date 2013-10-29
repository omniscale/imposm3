package postgis

import (
	"fmt"
)

func (pg *PostGIS) rotate(source, dest, backup string) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Rotating tables")))

	if err := pg.createSchema(backup); err != nil {
		return err
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	for _, tableName := range pg.tableNames() {
		tableName = pg.Prefix + tableName

		log.Printf("Rotating %s from %s -> %s -> %s", tableName, source, dest, backup)

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		sourceExists, err := tableExists(tx, source, tableName)
		if err != nil {
			return err
		}
		destExists, err := tableExists(tx, dest, tableName)
		if err != nil {
			return err
		}

		if !sourceExists {
			log.Warnf("skipping rotate of %s, table does not exists in %s", tableName, source)
			continue
		}

		if destExists {
			log.Printf("backup of %s, to %s", tableName, backup)
			if backupExists {
				err = dropTableIfExists(tx, backup, tableName)
				if err != nil {
					return err
				}
			}
			sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" SET SCHEMA "%s"`, dest, tableName, backup)
			_, err = tx.Exec(sql)
			if err != nil {
				return err
			}
		}

		sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" SET SCHEMA "%s"`, source, tableName, dest)
		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

func (pg *PostGIS) Deploy() error {
	return pg.rotate(pg.Schema, "public", pg.BackupSchema)
}

func (pg *PostGIS) RevertDeploy() error {
	return pg.rotate(pg.BackupSchema, "public", pg.Schema)
}

func (pg *PostGIS) RemoveBackup() error {
	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	backup := pg.BackupSchema

	for _, tableName := range pg.tableNames() {
		tableName = pg.Prefix + tableName

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		if backupExists {
			log.Printf("removing backup of %s from %s", tableName, backup)
			err = dropTableIfExists(tx, backup, tableName)
			if err != nil {
				return err
			}

		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// tableNames returns a list of all tables (without prefix).
func (pg *PostGIS) tableNames() []string {
	var names []string
	for name, _ := range pg.Tables {
		names = append(names, name)
	}
	for name, _ := range pg.GeneralizedTables {
		names = append(names, name)
	}
	return names
}
