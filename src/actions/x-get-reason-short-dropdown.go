package actions

import (
	"github.com/gin-gonic/gin"
	"gitlab.com/dohome-2020/go-servicex/dbx/rs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/validx"
)

type DtoGetReasonShortDropdown struct {
	Type []string `json:"type"`
}

func XGetReasonShortDropdown(c *gwx.Context) (any, error) {

	//typex := c.Query(`type`)
	dto := DtoGetReasonShortDropdown{}
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}
	if len(dto.Type) < 1 {
		return nil, c.ErrorBadRequest("กรุณาใส่ type")
	}

	dxRedshift, ex := rs.DatalakeSqlx("hana")
	if ex != nil {
		return nil, ex
	}

	var zstatus, zaproveSto, zreasonIssue string
	for i := 0; i < len(dto.Type); i++ {
		if dto.Type[i] == "01" {
			zstatus = "X"
		}
		if dto.Type[i] == "02" {
			zaproveSto = "X"
		}
		if dto.Type[i] == "03" {
			zreasonIssue = "X"
		}
	}

	query := `select code,description as desc, '' as type,zstatus,zaprove_sto,zreason_issue
	from ZDEL_DO_REASON
	where 1=1 `
	if !validx.IsEmpty(zstatus) {
		query += ` and zstatus = 'X'`
	}
	if !validx.IsEmpty(zaproveSto) {
		query += ` and zaprove_sto = 'X'`
	}
	if !validx.IsEmpty(zreasonIssue) {
		query += ` and zreason_issue = 'X'`
	}

	rows, ex := dxRedshift.QueryScan(query)
	if ex != nil {
		return nil, ex
	}

	for i := 0; i < len(rows.Rows); i++ {
		if rows.Rows[i].String("zstatus") == "X" {
			rows.Rows[i].Set(`type`, "01")
		}
		if rows.Rows[i].String("zaprove_sto") == "X" {
			rows.Rows[i].Set(`type`, "02")
		}
		if rows.Rows[i].String("zreason_issue") == "X" {
			rows.Rows[i].Set(`type`, "03")
		}

	}

	return gin.H{`results`: rows.Rows}, nil
}
