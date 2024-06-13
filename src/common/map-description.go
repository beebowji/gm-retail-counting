package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func MapArticle(articleArr []string) (*sqlx.Rows, error) {

	// dx, ex := pg.ArticleMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	sql := fmt.Sprintf(`select pu.products_id, u.unit_code, u.name_th from product_units pu
	left join units u on pu.units_id = u.id
	where pu.products_id in ('%s')`, strings.Join(articleArr, `','`))
	rows, ex := dx.QueryScan(sql)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapArticleUnit := rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`products_id`)
	})

	return mapArticleUnit, nil
}

func MapToHeaderData(docNoArr []string) (*sqlx.Rows, error) {
	// dx, ex := pg.DocumentsRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_DOCUMENTS)
	if ex != nil {
		return nil, ex
	}

	sql := fmt.Sprintf(`select regmt_no, conf_ind from doc_to_toheaderdata where regmt_no in ('%s')`, strings.Join(docNoArr, `','`))
	rows, ex := dx.QueryScan(sql)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapToHeader := rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`regmt_no`)
	})
	return mapToHeader, nil
}
