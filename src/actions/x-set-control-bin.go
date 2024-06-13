package actions

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/jwtx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

func XSetControlBin(c *gwx.Context) (any, error) {

	// Incoming variable
	dto := []gmretailcounting.DtoSetControlBin{}

	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	// Initiate database(retail_picking) connection
	// dxRetailPicking, ex := pg.RetailPickingWrite()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRetailPicking, ex := sqlx.ConnectPostgresRW(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	dxDocuments, ex := sqlx.ConnectPostgresRO(dbs.DH_DOCUMENTS)
	if ex != nil {
		return nil, ex
	}

	// Get user
	userLogin, ex := jwtx.GetLoginInfo(c)
	if ex != nil {
		return nil, ex
	}
	userId := userLogin.UserInfo.UserID

	// Update all doc(s) by incoming co_item_id
	var itemIdArrOrder, itemIdArrOrderNoCbin []string
	//itemIdArr := ``
	itemConfig := ``
	tableConfirmCbin, ex := dxRetailPicking.TableEmpty("qc_confirm_cbin")
	if ex != nil {
		return nil, ex
	}
	//

	for _, v := range dto {

		if !validx.IsEmpty(v.Cbin) && !validx.IsEmpty(v.Csite) && !validx.IsEmpty(v.Csloc) {
			if len(itemConfig) == 0 {
				itemConfig += fmt.Sprintf(`('%s','%s','%s')`, v.Cbin, v.Csite, v.Csloc)
			} else {
				itemConfig += fmt.Sprintf(`,('%s','%s','%s')`, v.Cbin, v.Csite, v.Csloc)
			}
		}

		// if len(itemIdArr) == 0 {
		// 	itemIdArr += fmt.Sprintf(`('%s','%s')`, v.CoItemId, v.Cbin)
		// } else {
		// 	itemIdArr += fmt.Sprintf(`,('%s','%s')`, v.CoItemId, v.Cbin)
		// }

		if !validx.IsContains(itemIdArrOrder, v.CoItemId) {
			if !validx.IsEmpty(v.Cbin) {
				itemIdArrOrder = append(itemIdArrOrder, v.CoItemId)
			} else {
				itemIdArrOrderNoCbin = append(itemIdArrOrderNoCbin, v.CoItemId)
			}
		}
		if !validx.IsEmpty(v.Cbin) {
			tableConfirmCbin.Rows = append(tableConfirmCbin.Rows, sqlx.Map{
				`co_cbin_item_id`: uuid.New(),
				`co_item_id`:      v.CoItemId,
				`cbin_code`:       v.Cbin,
				`csite_code`:      v.Csite,
				`csloc_code`:      v.Csloc,
				`entry_dtm`:       time.Now(),
				`entry_by`:        userId,
			})
		}
		//  else {
		// 	deleteItemOrder = append(deleteItemOrder, v.CoItemId)
		// }
		// query := fmt.Sprintf(`UPDATE qc_confirm_cbin set cbin_code = '%s' WHERE co_item_id = '%s'`, v.Cbin, v.CoItemId)
		// _, ex = dxRetailPicking.Exec(query)
		// if ex != nil {
		// 	return nil, ex
		// }
	}

	// Query data for add transaction
	// Get table
	logTable, ex := dxRetailPicking.TableEmpty("qc_control_transaction_log")
	if ex != nil {
		return nil, ex
	}
	//
	var qcRows *sqlx.Rows
	if len(itemIdArrOrder) > 0 || len(itemIdArrOrderNoCbin) > 0 {

		doc := []string{}
		doc = append(doc, itemIdArrOrder...)
		doc = append(doc, itemIdArrOrderNoCbin...)

		sqlStatement := fmt.Sprintf(`SELECT qcoi.doc_no,
	qcoi.co_item_id,
	qcoi.site_code,
	qcoi.sloc_code,
	qcc.co_cbin_item_id,
	qcoi.doc_type,
	qcoi.doc_no,
	qcoi.doc_item,
	qcoi.article_code,
	qcoi.unit_code,
	qcoi.qty_pick,
	qcoi.qty_count,
	qcoi.is_short,
	qcoi.short_reason,
	qcc.csite_code,
	qcc.csloc_code,
	qcc.cbin_code
	FROM qc_control_order_item qcoi 
	LEFT JOIN qc_confirm_cbin qcc ON qcoi.co_item_id = qcc.co_item_id WHERE qcoi.co_item_id IN ('%s')`, strings.Join(doc, `','`))
		qcRows, ex = dxRetailPicking.QueryScan(sqlStatement)
		if ex != nil {
			return nil, ex
		}

		if len(itemIdArrOrder) > 0 {
			//เก็บ doc_no ไปหา ship_con
			docNo := []string{}
			if qcRows != nil {
				for i := 0; i < len(qcRows.Rows); i++ {
					for j := 0; j < len(itemIdArrOrder); j++ {
						if qcRows.Rows[i].String("co_item_id") == itemIdArrOrder[j] {
							docNo = append(docNo, qcRows.Rows[i].String("doc_no"))
							break
						}
					}
				}
			}

			//หาข้อมูลฝั่ง doc
			sqlDoc := fmt.Sprintf(`select doc_do_pk, shipping_cond from doc_do_header ddh 
	where doc_do_pk in ('%s')`, strings.Join(docNo, `','`))
			docRows, ex := dxDocuments.QueryScan(sqlDoc)
			if ex != nil {
				return nil, ex
			}

			//ดึงข้อมูล config
			if len(itemConfig) != 0 {
				sqlCon := fmt.Sprintf(`SELECT cb.cbin_id,sh.ship_con 
	FROM public.qc_control_bin cb
	left join public.qc_control_bin_shipcon sh on cb.cbin_id  = sh.cbin_id  
	where (cb.cbin_code,cb.csite_code,cb.csloc_code) in (%s) `, itemConfig)
				consRows, ex := dxRetailPicking.QueryScan(sqlCon)
				if ex != nil {
					return nil, ex
				}
				consRows.BuildMap(func(m *sqlx.Map) string {
					return m.String(`ship_con`)
				})

				//เช็ค
				for i := 0; i < len(docRows.Rows); i++ {
					chk := consRows.FindMap(docRows.Rows[i].String("shipping_cond"))
					if chk == nil {
						return nil, fmt.Errorf("กำหนดจุดวางไม่ได้เนื่องจากประเภทส่องมอบต่างกัน")
					}
				}
			}
		}
		//
		for _, v := range qcRows.Rows {

			//map co_item_id
			var toSite, toSloc, toCbin, transtype string
			for i := 0; i < len(dto); i++ {
				if dto[i].CoItemId == v.String("co_item_id") {
					toSite = dto[i].Csite
					toSloc = dto[i].Csloc
					toCbin = dto[i].Cbin

					if dto[i].Action == "CCB" {
						transtype = "CCB"
					} else if dto[i].Action == "CFB" {
						if validx.IsEmpty(v.String("co_cbin_item_id")) {
							transtype = "CFB"
						} else {
							//มี
							transtype = "MVB"
						}
					}
				}
			}

			logTable.Rows = append(logTable.Rows, sqlx.Map{
				`ctrans_id`:    uuid.New(),
				`site_code`:    v.String(`site_code`),
				`sloc_code`:    v.String(`sloc_code`),
				`doc_type`:     v.String(`doc_type`),
				`doc_no`:       v.String(`doc_no`),
				`doc_item`:     v.String(`doc_item`),
				`article_code`: v.String(`article_code`),
				`unit_code`:    v.String(`unit_code`),
				`qty_pick`:     v.Float(`qty_pick`),
				`qty_count`:    v.Float(`qty_count`),
				`is_short`:     v.Bool(`is_short`),
				`short_reason`: v.String(`short_reason`),
				`trans_type`:   transtype,
				`fr_site_code`: v.String("csite_code"),
				`fr_sloc_code`: v.String("csloc_code"),
				`fr_bin_code`:  v.String("cbin_code"),
				`to_site_code`: toSite,
				`to_sloc_code`: toSloc,
				`to_bin_code`:  toCbin,
				`entry_dtm`:    time.Now(),
				`entry_by`:     userId,
			})
		}
	}

	if ex := dxRetailPicking.Transaction(func(t *sqlx.Tx) error {

		if len(itemIdArrOrderNoCbin) > 0 {
			query := fmt.Sprintf(`delete from qc_confirm_cbin where co_item_id in ('%s') `, strings.Join(itemIdArrOrderNoCbin, `','`))
			_, ex = dxRetailPicking.Exec(query)
			if ex != nil {
				return ex
			}
		}

		if len(itemIdArrOrder) > 0 {
			query := fmt.Sprintf(`delete from qc_confirm_cbin where co_item_id in ('%s') `, strings.Join(itemIdArrOrder, `','`))
			_, ex = dxRetailPicking.Exec(query)
			if ex != nil {
				return ex
			}

			if tableConfirmCbin != nil && len(tableConfirmCbin.Rows) > 0 {
				_, ex = t.InsertCreateBatches(`qc_confirm_cbin`, tableConfirmCbin, 100)
				if ex != nil {
					return ex
				}
			}
		}

		// if len(deleteItemOrder) > 0 {
		// 	query := fmt.Sprintf(`delete from qc_confirm_cbin where co_item_id in ('%s')`, strings.Join(deleteItemOrder, `','`))
		// 	_, ex = dxRetailPicking.Exec(query)
		// 	if ex != nil {
		// 		return ex
		// 	}
		// }

		colsConflict := []string{`ctrans_id`}
		_, ex = t.InsertUpdateBatches(`qc_control_transaction_log`, logTable, colsConflict, 100)
		if ex != nil {
			return ex
		}

		return nil
	}); ex != nil {
		return nil, ex
	}

	return nil, nil
}
