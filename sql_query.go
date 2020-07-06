package msql

import (
	"reflect"
	"strings"

	"fmt"

	"strconv"

	"errors"
)

var (
	ErrInvalidSQLStatement = errors.New("Invalid SQL Statement")
)

type SQLField map[string]interface{}

// Lt transform this field into lower than (default equal)
func (s SQLField) Lt() SQLField {
	if len(s) == 1 {
		s["operator"] = "<"
	}

	return s
}

//Gte transform this field into greater than equal (default equal)
func (s SQLField) Gte() SQLField {
	if len(s) == 1 {
		s["operator"] = ">="
	}

	return s
}

func (s SQLField) String(key string) string {
	switch v := s[key].(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return fmt.Sprintf("%v", v)
	default:
		return ""
	}
}

type SQLQuery struct {
	query        string
	args         []interface{}
	selectClause string
	fromClause   string
	whereClause  string
	insertClause string
	updateClause string
	setClause    string
	offsetClause string
	limitClause  string
}

func Count(alias string) *SQLQuery {
	sb := strings.Builder{}
	sb.WriteString("SELECT COUNT(*) AS ")
	sb.WriteString(alias)

	return &SQLQuery{
		selectClause: sb.String(),
		args:         []interface{}{},
	}
}

func Select(fields ...string) *SQLQuery {
	sb := strings.Builder{}
	selectFields := "*"
	sb.WriteString("SELECT ")
	if len(fields) > 0 {
		selectFields = strings.Join(fields, ", ")
	}
	sb.WriteString(selectFields)

	return &SQLQuery{
		selectClause: sb.String(),
		args:         []interface{}{},
	}
}

func Update(tableName string) *SQLQuery {
	return &SQLQuery{
		updateClause: fmt.Sprintf("UPDATE %v", tableName),
	}
}

//Insert will generate insert query with value placeholder
func Insert(tableName string, fields ...SQLField) *SQLQuery {
	if tableName == "" || len(fields) == 0 {
		return &SQLQuery{insertClause: ""}
	}

	args := make([]interface{}, len(fields))
	fieldNames := make([]string, len(fields))
	fieldValues := make([]string, len(fields))

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("INSERT INTO %v ", tableName))
	for i, f := range fields {
		for k, v := range f {
			fieldNames[i] = k
			args[i] = v
			fieldValues[i] = "?"
		}
	}

	sb.WriteString(fmt.Sprintf("( %v )", strings.Join(fieldNames, ", ")))
	sb.WriteString(" VALUES ")
	sb.WriteString(fmt.Sprintf("( %v )", strings.Join(fieldValues, ", ")))

	return &SQLQuery{
		insertClause: sb.String(),
		args:         args,
	}
}

//Set will generate set query with value placeholder
func (q *SQLQuery) Set(fields ...SQLField) *SQLQuery {
	if len(fields) == 0 {
		return q
	}

	sb := strings.Builder{}
	sb.WriteString("SET ")

	args := writePlaceholder(&sb, ", ", fields...)
	q.args = append(q.args, args...)
	q.setClause = sb.String()
	return q
}

func (q *SQLQuery) From(tableName string) *SQLQuery {
	if tableName == "" {
		return q
	}

	q.fromClause = fmt.Sprintf("FROM %v", tableName)
	return q
}

func (q *SQLQuery) Where(fields ...SQLField) *SQLQuery {
	if len(fields) == 0 {
		return q
	}

	sb := strings.Builder{}
	sb.WriteString("WHERE ")
	args := writePlaceholder(&sb, " AND ", fields...)
	q.args = append(q.args, args...)
	q.whereClause = sb.String()
	return q
}

func (q *SQLQuery) Offset(offset int) *SQLQuery {
	q.offsetClause = fmt.Sprintf("OFFSET %v", offset)
	return q
}

func (q *SQLQuery) Limit(limit int) *SQLQuery {
	q.limitClause = fmt.Sprintf("LIMIT %v", limit)
	return q
}

//Generate generates the query into string
func (q *SQLQuery) Generate() (string, []interface{}, error) {
	if q.insertClause != "" {
		return q.insertClause, q.args, nil
	}

	if q.updateClause != "" && q.setClause != "" {
		stmnt := fmt.Sprintf("%v %v", q.updateClause, q.setClause)
		if q.whereClause != "" {
			return fmt.Sprintf("%v %v", stmnt, q.whereClause), q.args, nil
		}
		return stmnt, q.args, nil
	}

	if q.selectClause != "" && q.fromClause != "" {
		stmnt := fmt.Sprintf("%v %v", q.selectClause, q.fromClause)
		if q.whereClause != "" {
			stmnt = fmt.Sprintf("%v %v", stmnt, q.whereClause)
		}

		if q.offsetClause != "" {
			stmnt = fmt.Sprintf("%v %v", stmnt, q.offsetClause)
		}

		if q.limitClause != "" {
			stmnt = fmt.Sprintf("%v %v", stmnt, q.limitClause)
		}
		return stmnt, q.args, nil
	}
	return "", nil, ErrInvalidSQLStatement
}

func writeValue(sb *strings.Builder, args *[]interface{}, field SQLField) {
	for k, v := range field {
		//skip meta information
		if k == "operator" {
			continue
		}

		vT := reflect.ValueOf(v)
		if vT.Kind() == reflect.Slice || vT.Kind() == reflect.Array {
			stmnt := fmt.Sprintf("%v IN ", k)
			sb.WriteString(stmnt)
			sb.WriteString("(")
			for i := 0; i < vT.Len(); i++ {
				*args = append(*args, vT.Index(i).Interface())
				sb.WriteString("?")
				if i < vT.Len()-1 {
					sb.WriteString(",")
				}
			}
			sb.WriteString(")")
			return
		}
		stmnt := fmt.Sprintf("%v = ?", k)
		if operator := field["operator"]; operator != nil {
			stmnt = fmt.Sprintf("%v %v ?", k, operator)
		}
		sb.WriteString(stmnt)
		*args = append(*args, v)
	}

}

func writePlaceholder(sb *strings.Builder, sep string, fields ...SQLField) []interface{} {
	whereArgs := make([]interface{}, 0)
	for i, f := range fields {
		writeValue(sb, &whereArgs, f)
		if i < len(fields)-1 {
			sb.WriteString(sep)
		}
	}

	return whereArgs
}
