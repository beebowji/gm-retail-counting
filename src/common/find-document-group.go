package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindDocumentGroup(documentGroup []string) (*sqlx.Rows, error) {

	// connect db
	// dxCompany, ex := pg.CompanyRead()
	// if ex != nil {
	// 	return nil, ex
	// }

	dxCompany, ex := sqlx.ConnectPostgresRO("dh_company")
	if ex != nil {
		return nil, ex
	}

	query := `select delivery_group,description
	from delivery_type_group 
	 where 1=1 `
	if len(query) != 0 {
		query += fmt.Sprintf(`and delivery_group in ('%s') `, strings.Join(documentGroup, `','`))
	}

	rows, ex := dxCompany.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapRows := rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`delivery_group`)
	})

	return mapRows, nil
}
