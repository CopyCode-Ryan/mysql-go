package mysqlgo

import (
	"errors"

	"strings"

	"fmt"

	"time"
	
	"sync"
	
	"github.com/jmoiron/sqlx"
)

type dsn struct {
	HostName	string	
	HostPort	string	
	DBName		string
	UserName	string
	Password	string
	Charset		string	
	Prefix		string
}

func (d *dsn) getDSN() (string, error) {
	if d.UserName == "" {
		return "", errors.New("getDSN : UserName is nil")
	}
	if d.HostName == "" {
		return "", errors.New("getDSN : HostName is nil")
	}
	if d.HostPort == "" {
		return "", errors.New("getDSN : HostPort is nil")
	}
	if d.DBName == "" {
		return "", errors.New("getDSN : DBName is nil")
	}
	if d.Charset == "" {
		d.Charset = "utf8"
	}
	return fmt.Sprintf(
		"%s:%s@(%s:%s)/%s?charset=%s&parseTime=True&loc=Local",
		d.UserName,
		d.Password,
		d.HostName,
		d.HostPort,
		d.DBName,
		d.Charset,
	), nil
}

//Config is database connection configuration
type Config struct {
	Alias			string
	DSN 			dsn
	MaxOpenConns 	int
	MaxIdleConns 	int
	MaxLifetime		int	
	Enable			bool
}

type dbConfig struct {
	db 				*sqlx.DB
	dsn				string
	maxOpenConns	int
	maxIdleConns	int
	maxLifetime		int
	configMu		sync.RWMutex
	isClose			bool
}

func (service * dbConfig) getDB() *sqlx.DB {
	service.configMu.Lock()
	defer service.configMu.Unlock()
	if service.isClose {
		service.db, _ = sqlx.Open(driverName, service.dsn)
		service.isClose = false
	}
	return service.db
}

func (service *dbConfig) open()  {
	service.configMu.Lock()
	defer service.configMu.Unlock()
	if service.isClose {
		service.db, _ = sqlx.Open(driverName, service.dsn)
		service.isClose = false
	}
}

func (service *dbConfig) close() {
	service.configMu.Lock()
	defer service.configMu.Unlock()
	if !service.isClose {
		service.db.Close()
		service.isClose = true
	}
}

var driverName =  "mysql"

var dbConfigs = make(map[string]*dbConfig, 0)

var dbMu sync.RWMutex

//Connect 连接数据库并验证是否可以Ping
func Connect(configs ...*Config)(err error){
	var errs []string
	defer func(){
		if len(errs) > 0 {
			err = errors.New("\n [Config DB - Connect] : \n " + strings.Join(errs, "\n"))
		}
	}()

	if !(len(configs) > 0) {
		errs = append(errs, "The Config is Null of string")
		return 
	}

	for key, config := range configs {
		
		if config.Alias == "" {
			errs = append(errs, fmt.Sprintf("%d : The Alias is Null of string", key))
			continue
		}
		
		dsn, err := config.DSN.getDSN()
		
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		
		db, err := sqlx.Connect(driverName, dsn)
		
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}

		db.SetMaxOpenConns(config.MaxOpenConns)
		db.SetMaxIdleConns(config.MaxIdleConns)
		if config.MaxLifetime > 0 {
			db.SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
		}
		if d, ok := dbConfigs[config.Alias]; ok {
			//存在键值
			d.close()
			dbConfigs[config.Alias] = &dbConfig {
				db : db,
				maxIdleConns : config.MaxIdleConns,
				maxOpenConns : config.MaxOpenConns,
				maxLifetime  : config.MaxLifetime,
				dsn : dsn,
			}
		} else {
			//不存在键值
			dbConfigs[config.Alias] = &dbConfig {
				db : db,
				maxIdleConns : config.MaxIdleConns,
				maxOpenConns : config.MaxOpenConns,
				maxLifetime  : config.MaxLifetime,
				dsn : dsn,
			}
		}
	}

	return 
}

func getDBList() map[string]*dbConfig {
	return dbConfigs 
}

//getDB 获取指定别名的数据库
//如果不传入别名则默认获取别名为"default"的数据库
func getDB(alias ...string) (*sqlx.DB, error) {
	var errs []string
	dbName := "default"
	dbMu.Lock()
	defer dbMu.Unlock()
	if alias != nil {
		for _, value := range alias {
			dbc, ok := dbConfigs[value]
			if !ok {
				errs = append(errs, fmt.Sprintf("[Config DB]: The database link `%s` is not configured", value))
				continue
			} 
			if !dbc.isClose {
				return dbc.db, nil
			}
		}
	}
	dbc, ok := dbConfigs[dbName]
	if !ok {
		errs = append(errs, fmt.Sprintf("[Config DB]: The database link `%s` is not configured", dbName))
		return nil, fmt.Errorf("%s", strings.Join(errs, "\n"))
	} 
	return dbc.getDB(), nil
}

func closeDB(alias ...string) error {
	var errs []string
	for _, value := range alias {
		dbc, ok := dbConfigs[value]
		if !ok {
			errs = append(errs, fmt.Sprintf("[Config DB]: The database link `%s` is not configured", value))
			continue
		} 
		if dbc.isClose {
			continue
		}
		dbc.close()
	}
	return fmt.Errorf("%s", strings.Join(errs, "\n"))
}

func closeAllDB() {
	for _, config := range dbConfigs {
		if config.isClose {
			continue
		}
		config.close()
	}
}
