package sql

import (
	"fmt"
)

func (sdb *SQLDB) rotate(source, dest, backup string) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Rotating tables")))

	if err := sdb.createSchema(dest); err != nil {
		return err
	}

	if err := sdb.createSchema(backup); err != nil {
		return err
	}

	tx, err := sdb.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	for _, tableName := range sdb.tableNames() {
		tableName = sdb.Prefix + tableName

		log.Printf("Rotating %s from %s -> %s -> %s", tableName, source, dest, backup)

		backupExists, err := tableExists(tx, sdb.QB, backup, tableName)
		if err != nil {
			return err
		}
		sourceExists, err := tableExists(tx, sdb.QB, source, tableName)
		if err != nil {
			return err
		}
		destExists, err := tableExists(tx, sdb.QB, dest, tableName)
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
				err = dropTableIfExists(tx, sdb.QB, backup, tableName)
				if err != nil {
					return err
				}
			}
			sql := sdb.QB.ChangeTableSchemaSQL(dest, tableName, backup)
			_, err = tx.Exec(sql)
			if err != nil {
				return err
			}
		}

		sql := sdb.QB.ChangeTableSchemaSQL(source, tableName, dest)
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

func (sdb *SQLDB) Deploy() error {
	return sdb.rotate(sdb.Config.ImportSchema, sdb.Config.ProductionSchema, sdb.Config.BackupSchema)
}

func (sdb *SQLDB) RevertDeploy() error {
	return sdb.rotate(sdb.Config.BackupSchema, sdb.Config.ProductionSchema, sdb.Config.ImportSchema)
}

func (sdb *SQLDB) RemoveBackup() error {
	tx, err := sdb.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	backup := sdb.Config.BackupSchema

	for _, tableName := range sdb.tableNames() {
		tableName = sdb.Prefix + tableName

		backupExists, err := tableExists(tx, sdb.QB, backup, tableName)
		if err != nil {
			return err
		}
		if backupExists {
			log.Printf("removing backup of %s from %s", tableName, backup)
			err = dropTableIfExists(tx, sdb.QB, backup, tableName)
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
func (sdb *SQLDB) tableNames() []string {
	var names []string
	for name, _ := range sdb.Tables {
		names = append(names, name)
	}
	for name, _ := range sdb.GeneralizedTables {
		names = append(names, name)
	}
	return names
}

func (sdb *SQLDB) IsDeploymentSupported() bool {
	return sdb.DeploymentSupported
}
