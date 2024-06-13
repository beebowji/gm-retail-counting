package actions

import (
	"fmt"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func XGetDocumentTypeMaster(c *gwx.Context) (any, error) {

	type rto struct {
		Code string `json:"code"`
		Desc string `json:"desc"`
	}

	rtos := []rto{}

	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_COMPANY)
	if ex != nil {
		return nil, ex
	}

	query := `select dtg.delivery_group as code, dtg.description as desc from delivery_type_group dtg`
	deliRes, ex := dx.QueryScan(query)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}

	if len(deliRes.Rows) > 0 {
		for _, v := range deliRes.Rows {
			rtos = append(rtos, rto{
				Code: v.String("code"),
				Desc: v.String("desc"),
			})
		}
	}
	return rtos, nil
}
