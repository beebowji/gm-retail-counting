package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindProducts(articleCode []string) (*sqlx.Rows, error) {

	// connect db
	// dxArticleMaster, ex := pg.ArticleMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxArticleMaster, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	queryArticle := fmt.Sprintf(`select p.id, p.article_id, p.name_th from products p where article_id in ('%s')`, strings.Join(articleCode, `','`))
	rowArticle, ex := dxArticleMaster.QueryScan(queryArticle)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapArticle := rowArticle.BuildMap(func(m *sqlx.Map) string {
		return m.String(`article_id`)
	})

	return mapArticle, nil
}
