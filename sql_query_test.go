package msql_test

import (
	"testing"

	"github.com/elanq/msql"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	cases := []struct {
		name         string
		expected     string
		expectedArgs []interface{}
		query        func() (string, []interface{}, error)
		shouldError  bool
	}{
		{
			"simple-INSERT",
			"INSERT INTO table_name ( col1, col2, col3 ) VALUES ( ?, ?, ? )",
			[]interface{}{"val1", "val2", "val3"},
			msql.Insert("table_name",
				msql.SQLField{"col1": "val1"},
				msql.SQLField{"col2": "val2"},
				msql.SQLField{"col3": "val3"},
			).Generate,
			false,
		},
		{
			"simple-UPDATE",
			"UPDATE table_name SET col1 = ?, col2 = ?",
			[]interface{}{"val1", "val2"},
			msql.Update("table_name").
				Set(
					msql.SQLField{"col1": "val1"},
					msql.SQLField{"col2": "val2"},
				).Generate,
			false,
		},
		{
			"UPDATE-with-WHERE",
			"UPDATE table_name SET col1 = ? WHERE col2 = ?",
			[]interface{}{"val1", "val2"},
			msql.Update("table_name").
				Set(
					msql.SQLField{"col1": "val1"},
				).
				Where(
					msql.SQLField{"col2": "val2"},
				).Generate,
			false,
		},
		{
			"simple-SELECT",
			"SELECT * FROM table_name",
			[]interface{}{},
			msql.Select().From("table_name").Generate,
			false,
		},
		{
			"SELECT-with-WHERE",
			"SELECT * FROM table_name WHERE col1 = ? AND col2 = ?",
			[]interface{}{"val1", "val2"},
			msql.Select().From("table_name").
				Where(
					msql.SQLField{"col1": "val1"},
					msql.SQLField{"col2": "val2"},
				).
				Generate,
			false,
		},
		{
			"SELECT-with-column-and-where",
			"SELECT col1, col2, col3 FROM table_name WHERE col1 = ? AND col2 = ?",
			[]interface{}{"val1", "val2"},
			msql.Select("col1", "col2", "col3").From("table_name").
				Where(
					msql.SQLField{"col1": "val1"},
					msql.SQLField{"col2": "val2"},
				).
				Generate,
			false,
		},
		{
			"SELECT-with-column-and-where-and-offset-limit",
			"SELECT col1, col2, col3 FROM table_name WHERE col1 = ? AND col2 = ? OFFSET 1 LIMIT 10",
			[]interface{}{"val1", "val2"},
			msql.Select("col1", "col2", "col3").From("table_name").
				Where(
					msql.SQLField{"col1": "val1"},
					msql.SQLField{"col2": "val2"},
				).
				Offset(1).Limit(10).
				Generate,
			false,
		},
		{
			"SELECT-with-column-and-multiple-where-value",
			"SELECT col1, col2, col3 FROM table_name WHERE col1 IN (?,?,?) OFFSET 1 LIMIT 10",
			[]interface{}{"val1", "val2", "val3"},
			msql.Select("col1", "col2", "col3").From("table_name").
				Where(
					msql.SQLField{"col1": []string{"val1", "val2", "val3"}},
				).
				Offset(1).Limit(10).
				Generate,
			false,
		},
		{
			"SELECT-COUNT-with-WHERE-value",
			"SELECT COUNT(*) AS alias FROM table_name WHERE col1 = ?",
			[]interface{}{"val1"},
			msql.Count("alias").From("table_name").
				Where(
					msql.SQLField{"col1": "val1"},
				).
				Generate,
			false,
		},
		{
			"SELECT-COUNT-with-multiple-WHERE-value",
			"SELECT COUNT(*) AS alias FROM table_name WHERE col1 = ? AND col2 IN (?,?,?)",
			[]interface{}{"val1", "val2", "val3", "val4"},
			msql.Count("alias").From("table_name").
				Where(
					msql.SQLField{"col1": "val1"},
					msql.SQLField{"col2": []string{"val2", "val3", "val4"}},
				).
				Generate,
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, args, err := c.query()
			if err != nil {
				assert.True(t, c.shouldError)
			} else {
				assert.Equal(t, c.expectedArgs, args)
				assert.Equal(t, c.expected, actual)
			}
		})
	}
}
