package starter

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)

// 存储数据库连接源切片 默认长度和容量都为1
var datasourceList = make(map[string]DbStruct, 1)

/**
 * @Author yNsLuHan
 * @Description:
 */
type DbStruct struct {
	Db   *sqlx.DB
	Gorm *gorm.DB
}

/*func main() {
	// 获取项目路径
	basePath, err := os.Getwd()
	//
	if err != nil {
		log.Fatal("ERROR get basePath error,", err)
	}
	// 拼接配置文件路径
	var configPath = path.Join(basePath, "application-dev.yaml")
	InitDataSources(configPath, "mysql.datasource")
}*/

/**
* @Author: yNsLuHan
* @Description:
* @File: MysqlPool
* @Version: 1.0.0
* @Date: 2021/8/23 12:45 下午
 */
func InitDataSources(configPath, datasourceName string) {
	// 读取配置文件 yaml
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	// 获取config  mysql配置
	var configMap = make(map[string]interface{}, 1)
	err = yaml.Unmarshal(yamlFile, &configMap)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	// 获取数据
	var nodeList = configMap[datasourceName]
	// 判断是否为空
	if nodeList == nil && nodeList.(map[string]interface{}) != nil {
		log.Fatal("ERROR 配置", datasourceName, "不存在")
	}

	// 进行遍历 获取到单个数据源
	// 列master,  因为每个数据的结构是 map[interface{}]interface{} 类型，所有需要遍历获取到 key  和 value
	for nodeName, value := range nodeList.(map[interface{}]interface{}) {
		// 进行数据库连接操作
		SetDatasource(datasourceList, value.(map[interface{}]interface{}), nodeName.(string), datasourceName)
	}

}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:21:11
 * @param datasourceList
 * @param m2
 * @param datasourceName
 * @return map[string]DbStruct
 */
func SetDatasource(datasourceList map[string]DbStruct, data map[interface{}]interface{}, nodeName, datasourceName string) {

	// 连接必要属性
	//log.Println("INFO 正在获取数据库连接参数..")
	host := GetStringMustOption(data, "host")
	port := GetIntMustOption(data, "port")
	user := GetStringMustOption(data, "user")
	password := GetStringMustOption(data, "password")
	database := GetStringMustOption(data, "database")
	url := GetStringMustOption(data, "url")
	// 选择性数据
	// 连接最大空闲数量
	interfaceMaxIdle := GetIntOption(data, "max-idle")
	// 连接池大小
	interfacePoolSize := GetIntOption(data, "max-pool-size")
	// 连接最大空闲时间
	interfaceIdleTimeout := GetIntOption(data, "idle-timeout")
	// 连接最大生存时间
	interfaceMaxLifetime := GetIntOption(data, "max-lifetime")
	// 是否初始化gorm
	loadGorm := GetBoolOption(data, "load-gorm")
	//
	//// db
	var db *sqlx.DB
	// 数据源url
	// var vds = "%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local"
	var vds = "%s:%s@tcp(%s:%s)/%s?%s"
	// 根据env获取host 根据env获取host
	var dataSource = fmt.Sprintf(vds, user, password, host, strconv.Itoa(port), database, url)
	//
	var err error
	//
	//log.Println("INFO 正在连接数据库..")
	// driverName: 驱动
	db, err = sqlx.Connect("mysql", dataSource)
	//
	if err != nil {
		log.Fatal("ERROR 数据master库连接失败： ", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal("ERROR 数据master库连接失败1： ", err)
	}
	// 设置最大空闲连接数
	if interfaceMaxIdle != 0 {
		db.SetMaxIdleConns(interfaceMaxIdle)
	}
	// 设置连接池大小
	if interfacePoolSize != 0 {
		db.SetMaxOpenConns(interfacePoolSize)
	}
	// 设置连接最大空闲时间
	if interfaceIdleTimeout != 0 {
		db.SetConnMaxIdleTime(time.Duration(interfaceIdleTimeout))
	}
	// 设置连接最大生存时间
	if interfaceIdleTimeout != 0 {
		db.SetConnMaxLifetime(time.Duration(interfaceMaxLifetime))
	}
	log.Printf("INFO MySQL connection %s successful：%s:%s/%s \n", datasourceName, host, port, database)
	// 进行gorm连接创建
	if loadGorm {
		initGormDb := InitGormDb(db, datasourceName)
		datasourceList[datasourceName] = DbStruct{Db: db, Gorm: initGormDb}
	} else {
		datasourceList[datasourceName] = DbStruct{Db: db}
	}
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:42:57
 * @param db
 * @param gormDb
 * @param datasourceName
 */
func InitGormDb(db *sqlx.DB, datasourceName string) *gorm.DB {
	//
	var err error
	// 创建gorm, 使用现有连接
	gormDb, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), &gorm.Config{})
	//
	if err != nil {
		log.Fatal("ERROR gorm", datasourceName, "create fail:", err)
	}
	log.Println("INFO Mysql Gorm", datasourceName, "init success.")
	//
	return gormDb
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:18:20
 * @return map[string]DbStruct
 */
//func GetDataSource() map[string]DbStruct {
//	return datasourceList
//}
func GetDataSource() func(datasourceName string) DbStruct {
	return func(datasourceName string) DbStruct {
		return datasourceList[datasourceName]
	}
}

/**
 * @Author yNsLuHan
 * @Description:  返回实体类
 */
type Res struct {
	AlterRow int
	Error    error
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 预处理，插入多条数据
 * @Time 2021-08-23 17:15:46
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func PrepareMany(db *sqlx.DB, sql string, args []interface{}) Res {
	prepare, err := db.Prepare(sql)
	//
	if err != nil {
		log.Println("ERROR sql failed:", err)
		return Res{Error: err}
	}
	exec, err := prepare.Exec(args...)
	//
	if err != nil {
		log.Println("ERROR select failed:", err)
		return Res{Error: err}
	}
	// 获取id
	id, err := exec.LastInsertId()
	//
	if err != nil {
		log.Println("ERROR GetId failed:", err)
		return Res{Error: err}
	}
	return Res{AlterRow: int(id), Error: nil}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：获取一个数据
 * @Time 2021-08-23 17:19:29
 * @param db
 * @param sql
 * @param o
 * @param args
 * @return Res
 */
func GetOne(db *sqlx.DB, sql string, o interface{}, args ...interface{}) Res {
	if args != nil {
		err := db.Get(o, sql, args...)
		if err != nil {
			log.Println("ERROR sql failed:", err)
			return Res{Error: err}
		}
		return Res{}
	} else {
		err := db.Get(o, sql)
		if err != nil {
			log.Println("ERROR select failed:", err)
			return Res{Error: err}
		}
		return Res{}
	}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：获取一个或多个对象
 * @Time 2021-08-23 17:19:48
 * @param db
 * @param sql
 * @param o
 * @param args
 */
func GetStruct(db *sqlx.DB, sql string, o interface{}, args ...interface{}) Res {
	if args != nil {
		err := db.Select(o, sql, args...)
		if err != nil {
			log.Println("ERROR sql failed:", err)
			return Res{Error: err}
		}
	} else {
		err := db.Select(o, sql)
		if err != nil {
			log.Println("ERROR select failed:", err)
			return Res{Error: err}
		}
	}
	return Res{}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：插入数据
 * @Time 2021-08-23 17:23:11
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func InsertStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR insert failed:", err)
		return Res{Error: err}
	}
	id, err := exec.LastInsertId()
	if err != nil {
		log.Println("ERROR Get id failed:", err)
		return Res{Error: err}
	}
	return Res{AlterRow: int(id)}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 修改
 * @Time 2021-06-08 15:14:19
 * @param db
 * @param sql
 * @param args
 * @return interface{}
 */
func UpdateStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR update failed:", err)
		return Res{Error: err}
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		log.Println("ERROR get Affected failed:", err)
		return Res{Error: err}
	}
	// 返回影响行数
	return Res{AlterRow: int(affected)}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 删除
 * @Time 2021-08-23 17:25:22
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func DeleteStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR update failed:", err)
		return Res{Error: err}
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		log.Println("ERROR get Affected failed:", err)
		return Res{Error: err}
	}
	// 返回影响行数
	return Res{AlterRow: int(affected)}
}

func GetStringMustOption(data map[interface{}]interface{}, key string) string {

	value := data[key]

	//
	if value == nil {
		log.Fatal("ERROR 数据库：", key, " 字段为空")
	}

	return value.(string)

}

func GetStringOption(data map[interface{}]interface{}, key string) string {
	value := data[key]
	//
	if value == nil {
		return ""
	}
	return value.(string)
}

func GetIntMustOption(data map[interface{}]interface{}, key string) int {
	value := data[key]
	//
	if value == nil {
		log.Fatal("ERROR 数据库：", key, " 字段为空")
	}
	return value.(int)
}
func GetIntOption(data map[interface{}]interface{}, key string) int {
	value := data[key]
	//
	if value == nil {
		return 0
	}
	return value.(int)
}
func GetBoolOption(data map[interface{}]interface{}, key string) bool {
	value := data[key]
	//
	if value == nil {
		return false
	}
	return true
}
