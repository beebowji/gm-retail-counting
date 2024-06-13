package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

type DtoGetReasonConfirmChecklist struct {
	Type []string `json:"type"`
}

func XGetReasonConfirmChecklist(c *gwx.Context) (any, error) {

	var dto DtoGetReasonConfirmChecklist
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	// dxRtPicking, ex := pg.RetailPickingRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRtPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	query := `select creason_code as code, creason_desc as descr, creason_type as type from qc_reason_type`
	if len(dto.Type) != 0 {
		query += fmt.Sprintf(` where creason_type in ('%s')`, strings.Join(dto.Type, `','`))
	}
	row, ex := dxRtPicking.QueryScan(query)
	if ex != nil {
		return nil, ex
	}

	reason := []gmretailcounting.ResultsGetReasonConfirmChecklist{}
	if len(row.Rows) != 0 {
		for _, v := range row.Rows {
			reason = append(reason, gmretailcounting.ResultsGetReasonConfirmChecklist{
				Type: v.String(`type`),
				Code: v.String(`code`),
				Desc: v.String(`descr`),
			})
		}
	}

	rto := gmretailcounting.RtoGetReasonConfirmChecklist{
		Results: reason,
	}

	return rto, nil
}
