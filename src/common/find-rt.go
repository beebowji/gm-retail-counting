package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindRt(sellerCode []string) (*sqlx.Rows, error) {

	// connect db
	// dxArticleMaster, ex := pg.ArticleMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxArticleMaster, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	query := `SELECT seller_code, seller_name
	FROM sales_representative
	 where 1=1 `
	if len(query) != 0 {
		query += fmt.Sprintf(`and seller_code in ('%s') `, strings.Join(sellerCode, `','`))
	}

	rows, ex := dxArticleMaster.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapRows := rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`seller_code`)
	})

	return mapRows, nil
}

func MapSellerByArticle(articleId []string, dxArticle *sqlx.DB) (*sqlx.Rows, error) {
	//หา seller RT
	query := fmt.Sprintf(`select article_id,zmm_seller,sl.seller_code,rem_shelf_life,tot_shelf_life,seller_name from products pd
    left join sales_representative sl on pd.zmm_seller = sl.id
    where pd.article_id in ('%s')`, strings.Join(articleId, `','`))
	rows, ex := dxArticle.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapArticleSeller := rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`article_id`)
	})
	return mapArticleSeller, nil
}
