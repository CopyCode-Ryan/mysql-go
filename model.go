package mysqlgo

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

//Model 表模型
type Model struct{
	DBAlias		string
	TableName 	string
	Prefix		string
	err			[]string
	sql			string
	options		*option
	initLock	sync.RWMutex
}

//Table 数据表
type Table struct {
	Name 	string	//表名
	Alias	string	//别名
}

//Order 结果排序
type Order struct {
	Field 	string 	//排序字段
	Desc	bool	//排序方式: 默认false为asc排序，true为desc排序
}

//Limit 查询和操作的记录数量
type Limit struct {
	Offset	int		//起始位置，默认为0
	Length	int		//查询数量，默认为10	
}

//Join 表之间关系 
///Join语句，不用加join前缀
///Join类型，默认0
///0为INNER JOIN: 如果表中有至少一个匹配，则返回行，等同于 JOIN
///1为LEFT JOIN: 即使右表中没有匹配，也从左表返回所有的行
///2为RIGHT JOIN: 即使左表中没有匹配，也从右表返回所有的行
///3为FULL JOIN: 只要其中一个表中存在匹配，就返回行
type Join struct {
	Statement 	string	
	Type		int		
}

//Union 合并Select
type Union struct {
	SelectSQL 	[]string
	All			bool 
}

//Data 数据元素
type Data struct {
	Field	string
	Value	interface{}
}

type option struct{
	table		[]Table
	distinct	bool
	field		string
	join		[]Join
	where		string
	whereArgs	[]interface{}
	group		[]string
	having		string
	order		[]Order
	limit		Limit
	union		Union
	comment		string
	page		string
	force		string
	fetchSQL	bool
}

var exp = map[string]string {
	"EQ"		: "==",
	"NEQ" 		: "<>",
	"GT"		: ">",
	"NGT"		: ">=",
	"LT"		: "<",
	"ELT"		: "<=",
	"NLIKE"		: "NOT LIKE",
	"LIKE"		: "LIKE",
	"IN"		: "IN",
	"NOTIN"		: "NOT IN",
	"BETWEEN"	: "BETWEEN",
	"NBETWEEN"	: "NOT BETWEEN",
}

//ModelHook 模型操作前预处理数据钩子函数
type ModelHook interface {
	
}

var selectSQL = "SELECT%DISTINCT% %FIELD% FROM %TABLE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%%LIMIT% %UNION%%COMMENT%"
var insertSQL = "INSERT INTO %TABLE%(%FIELD%) VALUE(%MARK%)"
var updateSQL = "UPDATE %TABLE% SET %FIELD% WHERE %ARGS%"
var deleteSQL = "DELETE FORM %TABLE% WHERE %AGRS%"

//Table 指定当前的数据表
func (m *Model) Table(tables ...Table) *Model {
	m.initOption()
	m.options.table = append(m.options.table, tables...)
	return m
}

//Field 指定字段名
func (m *Model) Field(fields ...string) *Model {
	m.initOption()
	if len(fields) == 0 {
		m.options.field = ""
	} else {
		m.options.field = strings.Join(fields, ",")
	}
	return m
}

//Where 指定查询条件
func (m *Model) Where(where string, args ...interface{}) (model *Model) {
	m.initOption()
	if args != nil && where == "" {
		m.err = append(m.err, "[Model Where] : The Condition is nil")
		return 
	}
	if m.options.where == "" {
		m.options.where = where
	} else {
		m.options.where = fmt.Sprintf(" %s and %s ", m.options.where, where)
	}
	m.options.whereArgs = append(m.options.whereArgs, args...)
	return m
}

//Order 对操作的结果排序
func (m *Model) Order(orders ...Order) *Model {
	m.initOption()
	m.options.order = append(m.options.order, orders...)
	return m
}

//Limit 指定查询和操作的数量
func (m *Model) Limit(limit Limit) *Model {
	m.initOption()
	m.options.limit = limit
	return m
}

//Page 指定分页
///page 页数
///listRows 每页数量
func (m *Model) Page(page int, listRows int) *Model {
	m.initOption()
	m.options.page = fmt.Sprintf(" %d,%d ", page, listRows)
	return m
}

//Group 一个或多个列对结果集进行分组
func (m *Model) Group(fields ...string) *Model {
	m.initOption()
	m.options.group = append(m.options.group, fields...)
	return m
}

//Having 配合group方法完成从分组的结果中筛选
func (m *Model) Having(having string) *Model {
	m.initOption()
	m.options.having = having
	return m
}

//Distinct 用于返回唯一不同的值
func (m *Model) Distinct(distinct bool) *Model {
	m.initOption()
	m.options.distinct = distinct
	return m
}

//Join 用于根据两个或多个表中的列之间的关系，从这些表中查询数据
func (m *Model) Join(join Join) *Model {
	m.initOption()
	m.options.join = append(m.options.join, join)
	return m
}

//Union 用于合并两个或多个SELECT语句的结果集
func (m *Model) Union(selectSQL []string, all bool) *Model {
	m.initOption()
	if all {
		m.options.union.All = all
	}
	m.options.union.SelectSQL = append(m.options.union.SelectSQL, selectSQL...)
	return m
}

//LastSQL 最后执行生成的SQL语句
func (m *Model) LastSQL() string {
	return m.sql
}

//Error	执行过程中出现的所有错误
func (m *Model) Error() error {
	if len(m.err) > 0 {
		str := fmt.Sprintf("\n[Model Error]:\n %s \n", strings.Join(m.err, "\n"))
		return errors.New(str)
	}
	return nil
}

//Find 查找数据
func (m *Model) Find(dest interface{}) error {
	defer func(){
		m.options = nil
	}()
	m.Limit(Limit{
		Offset : 1,
	})
	m.sql = m.parseSelectSQL(selectSQL, m.options)
	if m.sql == "" {
		m.err = append(m.err, "[Model Find]:The SQL is null of string")
		return m.Error()
	}
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	if err = db.Get(dest, m.sql, m.options.whereArgs...); err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	return nil
}

//Select 查询数据
func (m *Model) Select(dest interface{}) error {
	defer func(){
		m.options = nil
	}()
	m.sql = m.parseSelectSQL(selectSQL, m.options)
	if m.sql == "" {
		m.err = append(m.err, "[Model Find]:The SQL is null of string")
		return m.Error()
	}
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	if err = db.Select(dest, m.sql, m.options.whereArgs...); err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	return nil
}


//Add 新增数据
func (m *Model) Add(datas ...Data) (int64, error) {
	defer func(){
		m.options = nil
	}()
	if len(datas) == 0 {
		m.err = append(m.err, "[Model Add]:The datas is null")
		return -1, m.Error()
	}
	var fields []string
	var values []interface{}
	for _, data := range datas {
		fields = append(fields, data.Field)
		values = append(values, data.Value)
	}
	m.sql = m.parseInsertSQL(insertSQL, fields...)
	
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	result ,err := db.Exec(m.sql, values...)
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	id, err := result.LastInsertId()
	if err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	return id, nil
}

//AddAll 新增多条数据
func (m *Model) AddAll(datas ...[]Data) error {
	defer func(){
		m.options = nil
	}()
	if len(datas) == 0 {
		m.err = append(m.err, "[Model AddAll]:The datas is null")
		return m.Error()
	}
	fields, err := m.verifyFiled(datas...)
	if err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	field, values := m.extractValue(fields, datas...)
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return m.Error()
	}
	m.sql = m.parseInsertSQL(insertSQL, field)
	tx := db.MustBegin()
	for _, value := range values {
		result := tx.MustExec(m.sql, value...)
		_, err := result.LastInsertId()
		if err != nil {
			m.err = append(m.err, err.Error())
			tx.Rollback()
			return m.Error()
		}
	}
	tx.Commit()
	return nil
}

//Update 更新数据
func (m *Model) Update(datas ...Data) (int64, error) {
	defer func(){
		m.options = nil
	}()
	if len(datas) == 0 {
		m.err = append(m.err, "[Model Update]: The datas is null")
		return -1, m.Error()
	}
	if m.options.where == "" && len(m.options.whereArgs) == 0 {
		m.err = append(m.err, "[Model Update]: The Condition is null")
		return -1, m.Error()
	}
	var args []interface{}
	var err error
	m.sql, args, err = m.parseUpdateSQL(updateSQL, m.options.where, m.options.whereArgs, datas...)
	if err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	result ,err := db.Exec(m.sql, args...)
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	id, err := result.RowsAffected()
	if err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	return id, nil
}

//Delete 删除数据
func (m *Model) Delete() (int64, error) {
	defer func(){
		m.options = nil
	}()
	if m.options.where == "" && len(m.options.whereArgs) == 0 {
		m.err = append(m.err, "[Model Delete]: The Condition is null")
		return -1, m.Error()
	}
	m.sql = m.parseDeleteSQL(deleteSQL, m.options.where)
	db, err := getDB(m.getDBAlias())
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	result ,err := db.Exec(m.sql, m.options.whereArgs...)
	if  err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	id, err := result.RowsAffected()
	if err != nil {
		m.err = append(m.err, err.Error())
		return -1, m.Error()
	}
	return id, nil
}

func (m *Model) getDBAlias() string {
	if m.DBAlias == "" {
		return "default"
	}
	return m.DBAlias
}

func (m *Model) getTableName() string {
	if m.TableName == "" {
		m.err = append(m.err, "[Model getTableName]: The TableName is nil")
	}
	return m.TableName
}

func (m *Model) initOption() {
	m.initLock.Lock()
	defer m.initLock.Unlock()
	if m.options == nil {
		m.options = &option{}
	}
}

func (m *Model) parseSelectSQL(sql string, options *option) string {
	sql = strings.Replace(sql, "%TABLE%", m.parseTable(m.options.table...), -1)
	sql = strings.Replace(sql, "%DISTINCT%", m.parseDistinct(options.distinct), -1)
	sql = strings.Replace(sql, "%FIELD%", m.parseField(options.field), -1)
	sql = strings.Replace(sql, "%JOIN%", m.parseJoin(options.join...), -1)
	sql = strings.Replace(sql, "%WHERE%", m.parseWhere(m.options.where), -1)
	sql = strings.Replace(sql, "%GROUP%", m.parseGroup(m.options.group...), -1)
	sql = strings.Replace(sql, "%HAVING%", m.parseHaving(m.options.having), -1)
	sql = strings.Replace(sql, "%ORDER%", m.parseOrder(m.options.order...), -1)
	sql = strings.Replace(sql, "%LIMIT%", m.parseLimit(m.options.limit), -1)
	sql = strings.Replace(sql, "%UNION%", m.parseUnion(m.options.union), -1)
	sql = strings.Replace(sql, "%COMMENT%", m.parseComment(m.options.comment), -1)
	return sql
}

func (m *Model) parseInsertSQL(sql string, fields ...string) string {
	sql = strings.Replace(sql, "%TABLE%", m.parseTable(m.options.table...), -1)
	sql = strings.Replace(sql, "%FIELD%", strings.Join(fields, ","), -1)
	var fieldMark []string
	for i := 0; i < len(fields); i++ {
		fieldMark = append(fieldMark, "?")
	}
	sql = strings.Replace(sql, "%MARK%", strings.Join(fieldMark, ","), -1)
	return sql
}

func (m *Model) parseUpdateSQL(sql, where string, whereArgs []interface{}, datas ...Data) (string, []interface{}, error)  {
	sql = strings.Replace(sql,  "%TABLE%", m.getTableName(), -1)
	var fields []string
	var values []interface{}
	vaild := make(map[string]bool, 0)
	for _, data := range datas {
		if !vaild[data.Field] {
			vaild[data.Field] = true
			fields = append(fields, fmt.Sprintf(" %s = ? ", data.Field))
			values = append(values, data.Value)
		} else {
			return "", nil, fmt.Errorf("[Model parseUpdateSQL]: Field '%s' is repeat", data.Field)
		}
	}
	sql = strings.Replace(sql, "%FIELD%", strings.Join(fields, ","), -1)
	sql = strings.Replace(sql, "%ARGS%", where, -1)
	if len(whereArgs) > 0 {
		values = append(values, whereArgs)
	}
	return sql, values, nil
}

func (m *Model) parseDeleteSQL(sql string, where string) string {
	sql = strings.Replace(sql, "%TABLE%", m.getTableName(), -1)
	sql = strings.Replace(sql, "%ARGS", where, -1)
	return sql
}

func (m *Model) verifyFiled(datas ...[]Data) ([]string, error) {
	fields := make(map[string]int, 0)
	var num = 0
	for i := 0; i < len(datas); i ++ {
		data := datas[i]
		if num == 0 {
			num = len(data)
		} else if num != len(data) {
			return nil, errors.New("[Model verifyFiled] : The Fields isn't same")
		}
		for _, d := range data {
			if d.Field != "" {
				fields[d.Field] ++
			}
		}
	}
	if len(fields) != num {
		return nil, errors.New("[Model verifyFiled] : The Fields isn't same")
	}
	var keys []string 
	for key, v := range fields {
		if v > 0 && key != "" {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *Model) extractValue(fields []string, datas ...[]Data) (string, [][]interface{}) {
	var values [][]interface{}
	for _, data := range datas {
		var value []interface{}
		dataMap := make(map[string]interface{}, 0)
		for _, d := range data {
			dataMap[d.Field] = d.Value
		}
		for _, field := range fields {
			value = append(value, dataMap[field])
		}
		values = append(values, value)
	}
	return strings.Join(fields, ",") , values
}

func (m *Model) parseTable(tables ...Table) string {
	if len(tables) > 0 {
		var table []string
		for _, Table := range tables {
			table = append(table, fmt.Sprintf(" %s %s ", Table.Name, Table.Alias))
		}
		return strings.Join(table, ",")
	}
	return m.getTableName()
}

func (m *Model) parseDistinct(distinct bool)string{
	if distinct {
		return "DISTINCT"
	} 
	return ""
}

func (m *Model) parseField(field string) string {
	if field == "" {
		return "*"
	} 
	return field
}

func (m *Model) parseJoin(joins ...Join) string {
	var join []string
	for _, value := range joins {
		if !(strings.Index(value.Statement, "JOIN") > -1 && strings.Index(value.Statement, "join") > -1) {
			value.Statement = fmt.Sprintf(" JOIN %s ", value.Statement)
		}
		switch value.Type {
		case 0:
			join = append(join, fmt.Sprintf(" INNER %s ", value.Statement))
			break
		case 1:
			join = append(join, fmt.Sprintf(" LEFT %s ", value.Statement))
			break
		case 2:
			join = append(join, fmt.Sprintf(" RIGHT %s ", value.Statement))
			break
		case 3:
			join = append(join, fmt.Sprintf(" FULL %s ", value.Statement))
			break
		default:
			join = append(join, fmt.Sprintf(" INNER %s ", value.Statement))
			break
		}

	}
	return strings.Join(join, ",")
}

func (m *Model) parseWhere(where string) string {
	if where != "" {
		return fmt.Sprintf(" WHERE %s " , where)
	}
	return ""
}

func (m *Model) parseGroup(group ...string) string {
	if len(group) > 0 {
		return fmt.Sprintf(" GROUP BY %s", strings.Join(group, ",")) 
	}
	return ""
}

func (m *Model) parseHaving(having string) string {
	if having != "" {
		return fmt.Sprintf(" HAVING %s", having) 
	}
	return ""
}

func (m *Model) parseComment(comment string) string {
	if comment != "" {
		return fmt.Sprintf(" /* %s */", comment)
	}
	return ""
}

func (m *Model) parseOrder(orders ...Order) string {
	if len(orders) > 0 {
		var orderStr []string
		for _, order := range orders {
			var str string
			if order.Desc {
				str = fmt.Sprintf(" %s desc", order.Field)
			} else {
				str = fmt.Sprintf(" %s asc", order.Field)
			}
			orderStr = append(orderStr, str)
		}
		return fmt.Sprintf(" ORDER BY %s ", strings.Join(orderStr, ","))
	}
	return ""
}

func (m *Model) parseLimit(limit Limit) string {
	if limit.Offset > 0 {
		if limit.Length > 0 {
			return fmt.Sprintf(" LIMIT %d, %d " , limit.Offset, limit.Length)
		}
		return fmt.Sprintf(" Limit %d " ,limit.Offset)
	}
	return ""
}

func (m *Model) parseUnion(union Union) string {
	if len(union.SelectSQL) > 0 {
		if union.All {
			return fmt.Sprintf(" UNION ALL %s ", strings.Join(union.SelectSQL, ","))
		}
		return fmt.Sprintf(" UNION %s ", strings.Join(union.SelectSQL, ","))
	}
	return ""
}