package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gms"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/tox"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmarticlemaster "gitlab.com/dohome-2020/go-structx/gm-article-master"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

type DtoGetBarcodeMaster struct {
	Article []string `json:"article"`
}

func XGetBarcodeMaster(c *gwx.Context) (any, error) {

	var dto DtoGetBarcodeMaster
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	dxArticle, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	query := `select article_id, unit_code, barcode from product_barcodes`
	if len(dto.Article) != 0 {
		query += fmt.Sprintf(` where article_id in ('%s')`, strings.Join(dto.Article, `','`))
	}
	row, ex := dxArticle.QueryScan(query)
	if ex != nil {
		return nil, ex
	}

	//set data convert to base
	var rqbUnits gmarticlemaster.X_CONVERT_UNITS_RQB
	var rsbUnits gmarticlemaster.X_CONVERT_UNITS_RSB
	for _, v := range row.Rows {
		rqbUnits.Item = append(rqbUnits.Item, gmarticlemaster.X_CONVERT_UNITS_RQB_ITEM{
			ArticleId:  v.String(`article_id`),
			UnitCodeFr: v.String(`unit_code`),
			UnitCodeTo: `-`,
			UnitAmtFr:  1,
		})
	}
	//convert unit
	if len(rqbUnits.Item) > 0 {
		if ex := gms.GM_ARTICLE_MASTER.POST(c, `product-units/convert-units`, rqbUnits, &rsbUnits); ex != nil {
			return nil, ex
		}
	}

	master := []gmretailcounting.ResultsGetBarcodeMaster{}
	var chk []string
	if len(row.Rows) != 0 {
		for _, v := range row.Rows {
			//chk id ซ้ำ
			key := fmt.Sprintf(`%v|%v`, v.String(`article_id`), v.String(`unit_code`))
			if !validx.IsContains(chk, key) {

				//filter id เดียวกัน
				data := row.Filter(func(m *sqlx.Map) bool {
					return m.String(`article_id`) == v.String(`article_id`) &&
						m.String(`unit_code`) == v.String(`unit_code`)
				})

				//เก็บ barcode
				var barcode []string
				if len(data.Rows) != 0 {
					for _, b := range data.Rows {
						barcode = append(barcode, b.String(`barcode`))
					}
				}

				//หา base ที่ convert แล้ว
				var baseUnit string
				var baseQty float64
				for _, b := range rsbUnits.Item {
					textBase := fmt.Sprintf(`%v|%v`, b.ArticleId, b.UnitCodeFr)
					if textBase == key {
						baseUnit = b.UnitCodeTo
						baseQty = tox.Float(b.UnitAmtTo)
						break
					}
				}

				master = append(master, gmretailcounting.ResultsGetBarcodeMaster{
					Article:        v.String(`article_id`),
					Unit:           v.String(`unit_code`),
					BaseUnit:       baseUnit,
					ConvertBaseQty: baseQty,
					Barcode:        barcode,
				})

				chk = append(chk, key)
			}
		}
	}

	rto := gmretailcounting.RtoGetBarcodeMaster{
		Results: master,
	}

	return rto, nil
}
