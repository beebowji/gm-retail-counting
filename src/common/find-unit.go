package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindUnit(unitCode []string) (*sqlx.Rows, error) {

	// connect db
	// dxArticleMaster, ex := pg.ArticleMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxArticleMaster, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	queryUnit := fmt.Sprintf(`select u.unit_code, u.name_th from units u where unit_code in ('%s')`, strings.Join(unitCode, `','`))
	rowUnit, ex := dxArticleMaster.QueryScan(queryUnit)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapUnit := rowUnit.BuildMap(func(m *sqlx.Map) string {
		return m.String(`unit_code`)
	})

	return mapUnit, nil
}
