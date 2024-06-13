package common

import (
	"fmt"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindMainBin(ConCode string) (*sqlx.Rows, error) {
	//Connect
	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	query := fmt.Sprintf(`select id, product_id, bin_code, bin_default, site, sloc, product_unit, product_seqno, document_no
	from product_bin where (product_id,site,sloc) in (%s) and bin_default = 'X' `, ConCode)
	rows, ex := dx.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//build map
	rows.BuildMap(func(m *sqlx.Map) string {
		return fmt.Sprintf(`%v|%v|%v`, m.String(`product_id`), m.String(`site`), m.String(`sloc`))
	})

	return rows, nil
}
