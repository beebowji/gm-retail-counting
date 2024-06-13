package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

type DtoGetShipConMaster struct {
	ShipCon []string `json:"ship_con"`
}

func XGetShipConMaster(c *gwx.Context) (any, error) {

	var dto DtoGetShipConMaster
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	// dxCompany, ex := pg.CompanyRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxCompany, ex := sqlx.ConnectPostgresRO(dbs.DH_COMPANY)
	if ex != nil {
		return nil, ex
	}

	query := `select ship_con_code as code, ship_con_name as descr from shipping_condition`
	if len(dto.ShipCon) != 0 {
		query += fmt.Sprintf(` where ship_con_code in ('%s')`, strings.Join(dto.ShipCon, `','`))
	}
	row, ex := dxCompany.QueryScan(query)
	if ex != nil {
		return nil, ex
	}

	shipCon := []gmretailcounting.ResultsGetDataMaster{}
	if len(row.Rows) != 0 {
		for _, v := range row.Rows {
			shipCon = append(shipCon, gmretailcounting.ResultsGetDataMaster{
				Code: v.String(`code`),
				Desc: v.String(`descr`),
			})
		}
	}

	rto := gmretailcounting.RtoGetDataMaster{
		Results: shipCon,
	}

	return rto, nil
}
