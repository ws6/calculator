package dbhelper

//works with tables where has id as primary key with type bigint
import (
	"fmt"
	"strings"

	"github.com/ws6/msi"
)

func CreateIfNotExists(db *msi.Msi, tableName string, crit msi.M) (int64, error) {

	table := db.GetTable(tableName)
	if table == nil {
		return 0, fmt.Errorf(`no table %s`, tableName)
	}
	found, err := table.FindOne(crit)
	if err == nil {

		return msi.ToInt64(found[`id`])
	}
	if err != msi.NOT_FOUND {


		return 0, err
	}
	//create

	if err := table.Insert(crit); err != nil {
		if !strings.Contains(err.Error(), `duplicate`) {
			return 0, err
		}
	}

	return CreateIfNotExists(db, tableName, crit)
}
func CreateIfNotExistsThenUpdate(db *msi.Msi, tableName string, crit, updates msi.M) (int64, error) {
	ret, err := createIfNotExistsThenUpdate(db, tableName, crit, updates)
	if err != nil {
		// slack.Info(`crit = %+v`, crit)
		// slack.Info(`update = %+v`, updates)

		return ret, fmt.Errorf(`createIfNotExistsThenUpdate[%s] err:%s`, tableName, err.Error())
	}
	return ret, nil
}
func createIfNotExistsThenUpdate(db *msi.Msi, tableName string, crit, updates msi.M) (int64, error) {
	id, err := CreateIfNotExists(db, tableName, crit)
	if err != nil {
		return 0, err
	}
	table := db.GetTable(tableName)
	if table == nil {
		return 0, fmt.Errorf(`no table - %s`, err.Error())
	}
	if err := table.Update(msi.M{`id`: id}, updates); err != nil {
		return id, fmt.Errorf(`Update err:%s`, err.Error())
	}

	return id, nil
}
