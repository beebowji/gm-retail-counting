package common

import (
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindShortResonQc() (*sqlx.Rows, error) {
	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	queryUser := `SELECT creason_code, creason_desc, creason_type
	FROM qc_reason_type; `
	rows, ex := dx.QueryScan(queryUser)
	if ex != nil {
		return nil, ex
	}

	rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`creason_code`)
	})

	return rows, nil
}
