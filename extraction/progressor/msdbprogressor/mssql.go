package msdbprogressor

//mssql.go using an existing table schema to store the progress.

import (
	"fmt"
	"time"

	"github.com/ws6/calculator/extraction/progressor"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/ws6/calculator/utils/config"
	"github.com/ws6/msi"
)

func init() {
	progressor.RegisterProgressor(new(MSdb))
}

type MSdb struct {
	db          *msi.Msi
	cfg         map[string]string
	dbnamespace string
	tablename   string
	getVal      func(string) string
	table       *msi.Table
}

func (self *MSdb) Close() error {
	if self.db != nil {
		fmt.Println(`MSdb Prog dB closed`)
		return self.db.Close()
	}
	return nil
}

func (self *MSdb) DriverName() string {
	return `MSDB`
}

func toprogress(m msi.M) (*progressor.Progress, error) {
	ret := new(progressor.Progress)

	if m[`time`] != nil {
		t, err := msi.ToTime(m[`time`])
		if err != nil {
			return nil, err
		}

		if t != nil && !t.IsZero() {
			ret.Timestamp = *t
		}
	}

	var err error
	if m[`number`] != nil {
		ret.Number, err = msi.ToInt64(m[`number`])
		if err != nil {
			return nil, err
		}
	}
	if m[`string`] != nil {
		ret.String, err = msi.ToString(m[`string`])
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (self *MSdb) GetProgress(name string) (*progressor.Progress, error) {
	found, err := self.table.FindOne(msi.M{`namespace`: self.dbnamespace, `k`: name})
	if err != nil {
		if err == msi.NOT_FOUND {
			return nil, progressor.NOT_FOUND_PROGRESS //!!!very important. other type error will stop the incref
		}
		return nil, err
	}
	return toprogress(found)
}

type MSdbconfig struct {
	Host, Database, User, Password string
}

func MakeConnectionString(cfg *MSdbconfig) string {

	return fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s",
		cfg.Host,
		cfg.Database,
		cfg.User,
		cfg.Password,
	)
}

func GetMSSQLSchema(cfg *MSdbconfig) (*msi.Msi, error) {
	schema, err := msi.NewDb(msi.MSSQL, MakeConnectionString(cfg), cfg.Database, ``)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (self *MSdb) NewProgressor(cfg config.Configer, sectionName string) (progressor.Progressor, error) {
	mscfg := new(MSdbconfig)
	makeKey := func(k string) string {
		return fmt.Sprintf(`%s::%s`, sectionName, k)
	}

	getVal := func(k string) string {
		if ret, err := cfg.String(makeKey(k)); err == nil {
			return ret
		}
		return ""
	}

	mscfg.Database = getVal(`database`)
	mscfg.Host = getVal(`host`)
	mscfg.User = getVal(`user`)
	mscfg.Password = getVal(`password`)
	ret := new(MSdb)
	var err error
	ret.db, err = GetMSSQLSchema(mscfg)
	if err != nil {
		fmt.Println(mscfg)
		return nil, err
	}
	ret.dbnamespace = getVal(`namespace`)

	if ret.dbnamespace == "" {
		ret.dbnamespace = `calculator`
	}

	ret.tablename = `system_config`
	if s := getVal(`tablename`); s != "" {
		ret.tablename = s
	}
	ret.getVal = getVal
	ret.table = ret.db.GetTable(ret.tablename)
	if ret.table == nil {
		return nil, fmt.Errorf(`no tablename:%s`, ret.tablename)
	}
	return ret, nil
}

func toUpdates(p *progressor.Progress) (int, msi.M) {
	tostore := msi.M{}
	updated := 0
	if p.Number != 0 {
		tostore[`number`] = p.Number
		updated++
	}
	if p.String != "" {
		tostore[`string`] = p.String
		updated++
	}
	if !p.Timestamp.IsZero() {
		tostore[`time`] = p.Timestamp
		updated++
	}

	return updated, tostore
}

//SaveProgress only store none-empty value
func (self *MSdb) SaveProgress(name string, p *progressor.Progress) error {
	updated, tostore := toUpdates(p)
	if updated == 0 {
		return nil //nothing to do
	}
	_, err := self.CreateIfNotExistRow(name, p)
	if err != nil {
		return err
	}
	tostore[`UpdatedAt`] = time.Now()
	return self.table.Update(
		msi.M{`namespace`: self.dbnamespace,
			`k`: name,
		},
		tostore,
	)

}

func (self *MSdb) CreateIfNotExistRow(name string, p *progressor.Progress) (int, error) {
	crit := msi.M{
		`namespace`: self.dbnamespace,
		`k`:         name,
	}
	found, err := self.table.FindOne(crit)

	if err != nil {
		if err != msi.NOT_FOUND {
			return 0, err
		}

		if err := self.table.Insert(crit); err != nil {
			return 0, err
		}

		return self.CreateIfNotExistRow(name, p)
	}
	return msi.ToInt(found[`id`])
}
