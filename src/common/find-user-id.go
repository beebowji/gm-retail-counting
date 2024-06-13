package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindUserId(personId []string) (*sqlx.Rows, error) {

	// connect db
	// dxCompany, ex := pg.CompanyRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxCompany, ex := sqlx.ConnectPostgresRO(dbs.DH_COMPANY)
	if ex != nil {
		return nil, ex
	}

	queryUser := fmt.Sprintf(`select e.person_id, e.first_name, e.last_name from employees e where person_id in ('%s')`, strings.Join(personId, `','`))
	rowUser, ex := dxCompany.QueryScan(queryUser)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapUser := rowUser.BuildMap(func(m *sqlx.Map) string {
		return m.String(`person_id`)
	})

	return mapUser, nil
}
