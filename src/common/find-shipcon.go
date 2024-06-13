package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindShipCon(shipConCode []string) (*sqlx.Rows, error) {

	//Connect
	// dxCompany, ex := pg.CompanyRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxCompany, ex := sqlx.ConnectPostgresRO(dbs.DH_COMPANY)
	if ex != nil {
		return nil, ex
	}

	queryShipCon := fmt.Sprintf(`select ship_con_code, ship_con_name from shipping_condition where ship_con_code in ('%s')`, strings.Join(shipConCode, `','`))
	rowShipCon, ex := dxCompany.QueryScan(queryShipCon)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapShipCon := rowShipCon.BuildMap(func(m *sqlx.Map) string {
		return m.String(`ship_con_code`)
	})

	return mapShipCon, nil
}
