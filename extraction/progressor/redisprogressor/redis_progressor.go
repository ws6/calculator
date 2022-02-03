package redisprogressor

//redis_progressor.go implementation of progressor backen in redis without expire.

import (
	"calculator/extraction/progressor"
	"encoding/json"
	"fmt"
	"log"

	"sync"

	"strconv"
	"strings"

	"time"

	"github.com/beego/beego/v2/core/config"

	"github.com/gomodule/redigo/redis"
)

func init() {
	progressor.RegisterProgressor(new(Redis))
}

type Redis struct {
	lock     sync.Mutex
	provider *Provider
}

func (self *Redis) SaveProgress(k string, p *progressor.Progress) error {
	fn := self.provider.Set
	if self.provider.maxlifetime <= 0 {
		fn = self.provider.SetNoExpire
	}
	if err := fn(k, p); err != nil {
		log.Println(`SetNoExpire`, err.Error())
		return err
	}
	return nil
}

func (_self *Redis) NewProgressor(configer config.Configer, sectionName string) (progressor.Progressor, error) {
	self := new(Redis)
	self.provider = new(Provider)
	timeout, err := configer.Int64(fmt.Sprintf(`%s::timeout`, sectionName))
	if err != nil {
		return nil, err
	}
	savePath, err := configer.String(fmt.Sprintf(`%s::save_path`, sectionName))
	if err != nil {
		return nil, err
	}

	if err := self.provider.SessionInit(
		timeout,
		savePath,
	); err != nil {
		return nil, err
	}

	if flushall, _ := configer.Bool(fmt.Sprintf(`%s::flushall`, sectionName)); flushall {
		if err := self.provider.FlushAll(); err != nil {
			return nil, fmt.Errorf(`when FlushAll:%s`, err.Error())
		}
		fmt.Print(`redis-flushall`)
	}

	return self, nil
}

func (self *Redis) GetProgress(k string) (*progressor.Progress, error) {
	vStr, err := self.provider.SessionRead(k)
	fmt.Println(`SessionRead`, k, vStr)
	if err != nil {
		//!!!translating the error to be awared by progressor
		if err == redis.ErrNil {
			return nil, progressor.NOT_FOUND_PROGRESS
		}
		return nil, fmt.Errorf(`SessionRead:%s`, err.Error())
	}
	b := []byte(vStr)
	ret := new(progressor.Progress)
	if err := json.Unmarshal(b, ret); err != nil {
		fmt.Println(vStr)
		return nil, fmt.Errorf(`Unmarshal:%s`, err.Error())
	}
	return ret, nil
}

func (self *Redis) DriverName() string {
	return `REDIS_PROGRESSOR`
}

func (self *Redis) Close() error {

	return nil
}

// MaxPoolSize redis max pool size
var MaxPoolSize = 100

// Provider redis session provider
type Provider struct {
	maxlifetime int64
	savePath    string
	poolsize    int
	password    string
	dbNum       int
	poollist    *redis.Pool
}

// SessionInit init redis session
// savepath like redis server addr,pool size,password,dbnum,IdleTimeout second
// e.g. 127.0.0.1:6379,100,astaxie,0,30
func (rp *Provider) SessionInit(maxlifetime int64, savePath string) error {
	rp.maxlifetime = maxlifetime
	configs := strings.Split(savePath, ",")
	if len(configs) > 0 {
		rp.savePath = configs[0]
	}
	if len(configs) > 1 {
		poolsize, err := strconv.Atoi(configs[1])
		if err != nil || poolsize < 0 {
			rp.poolsize = MaxPoolSize
		} else {
			rp.poolsize = poolsize
		}
	} else {
		rp.poolsize = MaxPoolSize
	}
	if len(configs) > 2 {
		rp.password = configs[2]
	}
	if len(configs) > 3 {
		dbnum, err := strconv.Atoi(configs[3])
		if err != nil || dbnum < 0 {
			rp.dbNum = 0
		} else {
			rp.dbNum = dbnum
		}
	} else {
		rp.dbNum = 0
	}
	var idleTimeout time.Duration = 0
	if len(configs) > 4 {
		timeout, err := strconv.Atoi(configs[4])
		if err == nil && timeout > 0 {
			idleTimeout = time.Duration(timeout) * time.Second
		}
	}
	rp.poollist = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", rp.savePath)
			if err != nil {
				return nil, err
			}
			if rp.password != "" {
				if _, err = c.Do("AUTH", rp.password); err != nil {
					c.Close()
					return nil, err
				}
			}
			// some redis proxy such as twemproxy is not support select command
			if rp.dbNum > 0 {
				_, err = c.Do("SELECT", rp.dbNum)
				if err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		MaxIdle: rp.poolsize,
	}

	rp.poollist.IdleTimeout = idleTimeout

	return rp.poollist.Get().Err()
}

// SessionRead read redis session by sid
func (rp *Provider) SessionRead(sid string) (string, error) {
	c := rp.poollist.Get()
	defer c.Close()
	return redis.String(c.Do("GET", sid))

}
func (rp *Provider) FlushAll() error {
	c := rp.poollist.Get()
	defer c.Close()
	if _, err := c.Do(`flushall`); err != nil {
		return err
	}
	return nil

}

// SessionRegenerate generate new sid for redis session
func (rp *Provider) SessionRegenerate(oldsid, sid string) (string, error) {
	c := rp.poollist.Get()
	defer c.Close()

	if existed, _ := redis.Int(c.Do("EXISTS", oldsid)); existed == 0 {
		// oldsid doesn't exists, set the new sid directly
		// ignore error here, since if it return error
		// the existed value will be 0
		c.Do("SET", sid, "", "EX", rp.maxlifetime)
	} else {
		c.Do("RENAME", oldsid, sid)
		c.Do("EXPIRE", sid, rp.maxlifetime)
	}
	return rp.SessionRead(sid)
}

// SessionDestroy delete redis session by id
func (rp *Provider) SessionDestroy(sid string) error {
	c := rp.poollist.Get()
	defer c.Close()

	c.Do("DEL", sid)
	return nil
}

func (rp *Provider) Set(key string, value interface{}) error {

	c := rp.poollist.Get()
	defer c.Close()
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf(`Marshal:%s`, err.Error())
	}
	_, err = c.Do("SET", key, b, "EX", rp.maxlifetime)
	if err != nil {
		return err
	}
	return nil
}
func (rp *Provider) SetNoExpire(key string, value interface{}) error {

	c := rp.poollist.Get()
	defer c.Close()
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf(`Marshal:%s`, err.Error())
	}
	if _, err := c.Do("SET", key, b, "NX"); err != nil {

		return err
	}
	return nil
}
