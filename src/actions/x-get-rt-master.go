package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

type DtoGetRTMaster struct {
	RT []string `json:"rt"`
}

func XGetRTMaster(c *gwx.Context) (any, error) {

	var dto DtoGetRTMaster
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	// dxArticle, ex := pg.ArticleMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }

	dxArticle, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	query := `select seller_code as code, seller_name as descr from sales_representative`
	if len(dto.RT) != 0 {
		query += fmt.Sprintf(` where seller_code in ('%s')`, strings.Join(dto.RT, `','`))
	}
	row, ex := dxArticle.QueryScan(query)
	if ex != nil {
		return nil, ex
	}

	seller := []gmretailcounting.ResultsGetDataMaster{}
	if len(row.Rows) != 0 {
		for _, v := range row.Rows {
			seller = append(seller, gmretailcounting.ResultsGetDataMaster{
				Code: v.String(`code`),
				Desc: v.String(`descr`),
			})
		}
	}

	rto := gmretailcounting.RtoGetDataMaster{
		Results: seller,
	}

	return rto, nil
}
