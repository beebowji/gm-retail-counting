package common

import (
	"gitlab.com/dohome-2020/go-servicex/dbx/rs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindShortReson() (*sqlx.Rows, error) {
	dxRedshift, ex := rs.DatalakeSqlx("hana")
	if ex != nil {
		return nil, ex
	}

	queryUser := `select code,description as desc, '' as type,zstatus,zaprove_sto,zreason_issue,type
	from ZDEL_DO_REASON `
	rows, ex := dxRedshift.QueryScan(queryUser)
	if ex != nil {
		return nil, ex
	}
	// for i := 0; i < len(rows.Rows); i++ {
	// 	if rows.Rows[i].String("zstatus") == "X" {
	// 		rows.Rows[i].Set(`type`, "01")
	// 	} else if rows.Rows[i].String("zaprove_sto") == "X" {
	// 		rows.Rows[i].Set(`type`, "02")
	// 	} else if rows.Rows[i].String("zreason_issue") == "X" {
	// 		rows.Rows[i].Set(`type`, "03")
	// 	}
	// }
	rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`code`)
	})

	return rows, nil
}
