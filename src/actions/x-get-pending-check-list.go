package actions

import (
	"fmt"
	"strings"
	"time"

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/common"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/timex"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

func XGetPendingCheckList(c *gwx.Context) (any, error) {

	// Incoming variable
	dto := gmretailcounting.DtoGetPendingCheckList{}
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	// Outgoing variable
	rto := gmretailcounting.RtoGetPendingCheckList{}
	rto.Results = make([]gmretailcounting.GetPendingCheckListResult, 0)

	// Initiate database(retail_picking) connection
	// dxRetailPicking, ex := pg.RetailPickingRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRetailPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	// Initiate database(documents) connection
	// dxDocuments, ex := pg.DocumentsRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxDocuments, ex := sqlx.ConnectPostgresRO(dbs.DH_DOCUMENTS)
	if ex != nil {
		return nil, ex
	}

	dxArticleMaster, ex := sqlx.ConnectPostgresRO(dbs.DH_ARTICLE_MASTER)
	if ex != nil {
		return nil, ex
	}

	// Fixed start/end date if = ""
	if dto.StartDtm == "" && dto.EndDtm == "" {
		dto.StartDtm = time.Now().AddDate(0, 0, -30).Format(timex.YYYYMMDD)
		dto.EndDtm = time.Now().Format(timex.YYYYMMDD)
	}

	siteSlocs := ``
	for i := 0; i < len(dto.Slocs); i++ {
		if len(siteSlocs) == 0 {
			siteSlocs += fmt.Sprintf(`('%s','%s')`, dto.Slocs[i].Site, dto.Slocs[i].Sloc)
		} else {
			siteSlocs += fmt.Sprintf(`,('%s','%s')`, dto.Slocs[i].Site, dto.Slocs[i].Sloc)
		}
	}
	// Find in ControlOrder
	sqlStatement := fmt.Sprintf(`select rpd.pick_actual_total,rpd.pick_volume ,qcoi.co_item_id, qcoi.doc_type, rpd.document_no as doc_no,rpd.product_seqno as doc_item, qcoi.rt, qcoi.article_code, qcoi.qty_count, qcoi.qty_pick, qcoi.status_rec, qcoi.entry_dtm,
	qcoi.entry_by, qcoi.base_qty_count, qcoi.base_qty_pick, qcoi.base_unit_code, qcc.cbin_code, qcc.csite_code, qcc.csloc_code,qcoi.short_reason
	from retail_picking_detail rpd
	left join qc_control_order_item qcoi on rpd.document_no  = qcoi.doc_no and rpd.product_seqno = qcoi.doc_item 
	left join qc_confirm_cbin qcc on qcoi.co_item_id = qcc.co_item_id
	where qcoi.status_rec in (0,1,2,3)
	and (to_char(qcoi.entry_dtm, 'yyyy-mm-dd') between '%s' and '%s') `, dto.StartDtm, dto.EndDtm)
	// Add do query condition
	if len(dto.DoNo) > 0 {
		sqlStatement += fmt.Sprintf(`and qcoi.doc_no in ('%s')`, strings.Join(dto.DoNo, "','"))
	}
	// Add picker query condition
	if len(dto.Picker) > 0 {
		sqlStatement += fmt.Sprintf(`and qcoi.entry_by in ('%s')`, strings.Join(dto.Picker, "','"))
	}
	// Add rt query condition
	if len(dto.RtCode) > 0 {
		sqlStatement += fmt.Sprintf(`and qcoi.rt in ('%s')`, strings.Join(dto.RtCode, "','"))
	}
	if len(siteSlocs) > 0 {
		sqlStatement += fmt.Sprintf(`and (rpd.site,rpd.sloc) in (%s)`, siteSlocs)
	}
	qcRows, ex := dxRetailPicking.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}

	// Get DO from all item in QC
	doItemRows := make(map[string]sqlx.Map)
	resItemRows := make(map[string]sqlx.Map)
	var doNoArr, doNoItemArr, resNoArr, resNoItemArr []string
	for _, v := range qcRows.Rows {

		//rt = append(rt, v.String("rt"))

		if v.String(`doc_type`) == "DO" && !validx.IsContains(doNoArr, v.String(`doc_no`)) {
			doNoArr = append(doNoArr, v.String(`doc_no`))
		} else if v.String(`doc_type`) == "RS" && !validx.IsContains(resNoArr, v.String(`doc_no`)) {
			resNoArr = append(resNoArr, v.String(`doc_no`))
		}

		keyDocitem := fmt.Sprintf(`%v|%v`, v.String(`doc_no`), v.String(`doc_item`))
		if v.String(`doc_type`) == "DO" && !validx.IsContains(doNoItemArr, keyDocitem) {
			doNoItemArr = append(doNoItemArr, keyDocitem)
			doItemRows[keyDocitem] = v
		} else if v.String(`doc_type`) == "RS" && !validx.IsContains(resNoItemArr, keyDocitem) {
			resNoItemArr = append(resNoItemArr, keyDocitem)
			resItemRows[keyDocitem] = v
		}
	}

	var mapToHeaderData *sqlx.Rows
	if len(resNoArr) != 0 {
		//mapToHeaderData, _ = common.MapToHeaderData(resNoArr)
		sql := fmt.Sprintf(`SELECT product_seqno, document_no,i_pick_status FROM public.retail_picking_detail where document_no in ('%s') and i_pick_status = 'C' `, strings.Join(resNoArr, `','`))
		mapToHeaderData, ex = dxRetailPicking.QueryScan(sql)
		if ex != nil {
			return nil, ex
		}
		mapToHeaderData.BuildMap(func(m *sqlx.Map) string {
			return fmt.Sprintf(`%v|%v`, m.String(`document_no`), m.String(`product_seqno`))
		})
		//
	}

	// Find DO in doc_do_header+doc_do_items
	if len(doNoArr) > 0 {
		sqlStatement = fmt.Sprintf(`select ddh.doc_do_pk, ddh.text1, ddi.article_no, ddi.gm_status, ddi.pick_status, ddi.site, ddi.sloc, ddi.delivery_item, ddi.batch, ddh.shipping_cond,ddi.quantity,ddi.sun
		from doc_do_header ddh left join doc_do_items ddi on ddh.doc_do_pk = ddi.doc_do_pk
		where ddh.doc_do_pk in ('%s')`, strings.Join(doNoArr, `','`))
		if len(siteSlocs) > 0 {
			sqlStatement += fmt.Sprintf(`and (ddi.site,ddi.sloc) in (%s)`, siteSlocs)
		}
		sqlStatement += `and exists (
			select doc_do_pk  from doc_do_items where pick_status  <> 'C' and doc_do_pk = ddh.doc_do_pk 
			)`

		doRows, ex := dxDocuments.QueryScan(sqlStatement)
		if ex != nil {
			return nil, ex
		}
		//build map
		mapDocDo := doRows.BuildMap(func(m *sqlx.Map) string {
			return m.String(`doc_do_pk`)
		})

		var articleArr, unitArr, shipCon, docHeaderChk []string
		for _, v := range doRows.Rows {
			artNo := strings.TrimLeft(v.String(`article_no`), "0")
			if !validx.IsContains(articleArr, artNo) {
				articleArr = append(articleArr, artNo)
			}
			if !validx.IsContains(unitArr, v.String("sun")) {
				unitArr = append(unitArr, v.String("sun"))
			}

		}

		//find article data
		mapArticle, ex := common.FindProducts(articleArr)
		if ex != nil {
			return nil, ex
		}
		// if len(mapArticle.Rows) != 0 {
		// 	for _, v := range mapArticle.Rows {
		// 		arrId = append(arrId, v.String(`id`))
		// 	}
		// }

		//unit
		mapUnit, ex := common.FindUnit(unitArr)
		if ex != nil {
			return nil, ex
		}
		//find unit by product id
		// var mapUnit *sqlx.Rows
		// if len(arrId) != 0 {
		// 	mapUnit, _ = common.MapArticle(arrId)
		// }

		for _, d := range doRows.Rows {
			shipCon = append(shipCon, d.String(`shipping_cond`))
		}
		mapShipCon, _ := common.FindShipCon(shipCon)

		// seller, ex := common.FindRt(rt)
		// if ex != nil {
		// 	return nil, ex
		// }

		seller, ex := common.MapSellerByArticle(articleArr, dxArticleMaster)
		if ex != nil {
			return nil, ex
		}

		if len(doRows.Rows) != 0 {
			for _, v := range doRows.Rows {
				if !validx.IsContains(docHeaderChk, v.String(`doc_do_pk`)) {

					//find shipcon
					var shipconName string
					shipConMap := mapShipCon.FindMap(v.String(`shipping_cond`))
					if shipConMap != nil {
						shipconName = shipConMap.String(`ship_con_name`)
					}

					rto.Results = append(rto.Results, gmretailcounting.GetPendingCheckListResult{
						DoNo:          v.String(`doc_do_pk`),
						Site:          v.String(`site`),
						Sloc:          v.String(`sloc`),
						ShipCon:       v.String(`shipping_cond`),
						ShipConName:   shipconName,
						CustomerQueue: v.String(`text1`),
					})

					//find item
					docHeaderMap := mapDocDo.FilterMap(v.String(`doc_do_pk`))
					if len(docHeaderMap.Rows) != 0 {
						for _, item := range docHeaderMap.Rows {
							key := fmt.Sprintf(`%v|%v`, item.String(`doc_do_pk`), item.String(`delivery_item`))
							qcItemData := doItemRows[key] //มองที่ retail

							var statusRec, statusPicking, statusFinished int
							var coItemId string
							var countQty, orderQty float64

							if len(qcItemData) != 0 {
								coItemId = qcItemData.String(`co_item_id`)

								statusRec = qcItemData.Int(`status_rec`)

								//
								countQty = qcItemData.Float("pick_actual_total")
								orderQty = qcItemData.Float("pick_volume")

								if !validx.IsEmpty(qcItemData.String("co_item_id")) {
									countQty = qcItemData.Float("qty_count")
									orderQty = qcItemData.Float("qty_pick")
								}

								//
								rto.Results[len(rto.Results)-1].DocType = qcItemData.String("doc_type")
							}

							if item.String(`gm_status`) == "C" {
								statusFinished = 91
							} else if item.String(`pick_status`) == "C" {
								statusFinished = 92
							}

							if countQty == 0 && orderQty == 0 {
								countQty = 0
								orderQty = item.Float("quantity")
								if v.String(`gm_status`) == "C" {
									statusFinished = 91
								} else if v.String(`pick_status`) == "C" {
									statusFinished = 92
								}
							}

							if validx.IsContains(doNoItemArr, key) {
								statusPicking = 1
							}

							//find article & unit
							var articleName, unitName, unitCode string
							articleId := strings.TrimLeft(item.String(`article_no`), "0")
							articleMap := mapArticle.FindMap(articleId)
							if articleMap != nil {
								articleName = articleMap.String(`name_th`)
								//productId = articleMap.String(`id`)
							}

							UnitMap := mapUnit.FindMap(item.String("sun"))
							//UnitMap := mapUnit.FindMap(productId)
							if UnitMap != nil {
								unitCode = UnitMap.String(`unit_code`)
								unitName = UnitMap.String(`name_th`)
							}

							//ถ้าไม่มีมองที่ Do แทน
							//CountQty:       qcItemData.Float(`qty_count`), qty_count : pick_actual_total : 0
							//OrderQty:       qcItemData.Float(`qty_pick`),  qty_pick : pick_volume :  qutity

							var rtCode, rtDes string
							rt := seller.FindMap(articleId)
							if rt != nil {
								rtDes = rt.String("seller_name")
								rtCode = rt.String("seller_code")
							}

							rto.Results[len(rto.Results)-1].Items = append(rto.Results[len(rto.Results)-1].Items, gmretailcounting.GetPendingCheckListItem{
								CoItemId:       coItemId,
								DoItem:         item.String(`delivery_item`),
								Batch:          v.String(`batch`),
								Article:        articleId,
								ArticleName:    articleName,
								Unit:           unitCode,
								UnitName:       unitName,
								Rt:             rtCode,
								RtName:         rtDes,
								Cbin:           qcItemData.String(`cbin_code`),
								Csite:          qcItemData.String(`csite_code`),
								Csloc:          qcItemData.String(`csloc_code`),
								CountQty:       countQty,
								OrderQty:       orderQty,
								BaseCountQty:   qcItemData.Float(`base_qty_count`),
								BaseOrderQty:   qcItemData.Float(`base_qty_pick`),
								BaseUnit:       qcItemData.String(`base_unit_code`),
								StatusRec:      statusRec,
								StatusPicking:  statusPicking,
								StatusFinished: statusFinished,
								ShortReason:    qcItemData.String(`short_reason`),
							})
						}
					}
					docHeaderChk = append(docHeaderChk, v.String(`doc_do_pk`))
				}
			}
		}
	}

	// Find RS in doc_reserv
	if len(resNoArr) > 0 {
		sqlStatement = fmt.Sprintf(`select dr.doc_reserv_pk, dri.res_item, dri.material, dri.plant, dri.store_loc, dri.batch, dri.withdrawn,dri.quantity,dri.unit
		from doc_reserv dr 
		left join doc_reserv_item dri ON dr.doc_reserv_pk = dri.doc_reserv_pk 
		where dr.doc_reserv_pk IN ('%s')`, strings.Join(resNoArr, `','`))
		if len(siteSlocs) > 0 {
			sqlStatement += fmt.Sprintf(`and (dri.plant,dri.store_loc) in (%s)`, siteSlocs)
		}

		resRows, ex := dxDocuments.QueryScan(sqlStatement)
		if ex != nil {
			return nil, ex
		}
		//build map
		mapResRow := resRows.BuildMap(func(m *sqlx.Map) string {
			return m.String(`doc_reserv_pk`)
		})

		var articleArr, unitArr, docHeaderChk []string
		for _, v := range resRows.Rows {
			artNo := strings.TrimLeft(v.String(`material`), "0")
			if !validx.IsContains(articleArr, artNo) {
				articleArr = append(articleArr, artNo)
			}
			if !validx.IsContains(unitArr, v.String("unit")) {
				unitArr = append(articleArr, v.String("unit"))
			}
		}

		//find article data
		mapArticle, ex := common.FindProducts(articleArr)
		if ex != nil {
			return nil, ex
		}
		// if len(mapArticle.Rows) != 0 {
		// 	for _, v := range mapArticle.Rows {
		// 		arrId = append(arrId, v.String(`id`))
		// 	}
		// }

		//unit
		mapUnit, ex := common.FindUnit(unitArr)
		if ex != nil {
			return nil, ex
		}
		//find unit by product id
		// var mapUnit *sqlx.Rows
		// if len(arrId) != 0 {
		// 	mapUnit, _ = common.MapArticle(arrId)
		// }

		// seller, ex := common.FindRt(rt)
		// if ex != nil {
		// 	return nil, ex
		// }

		seller, ex := common.MapSellerByArticle(articleArr, dxArticleMaster)
		if ex != nil {
			return nil, ex
		}

		if len(resRows.Rows) != 0 {
			for _, v := range resRows.Rows {
				if !validx.IsContains(docHeaderChk, v.String(`doc_reserv_pk`)) {

					chkHeader := gmretailcounting.GetPendingCheckListResult{
						DoNo:          v.String(`doc_reserv_pk`),
						Site:          v.String(`plant`),
						Sloc:          v.String(`store_loc`),
						CustomerQueue: "",
					}

					//map head
					// rto.Results = append(rto.Results, gmretailcounting.GetPendingCheckListResult{
					// 	DoNo:          v.String(`doc_reserv_pk`),
					// 	Site:          v.String(`plant`),
					// 	Sloc:          v.String(`store_loc`),
					// 	CustomerQueue: "",
					// })

					//find item
					docHeaderMap := mapResRow.FilterMap(v.String(`doc_reserv_pk`))
					headerPick := "C"
					if len(docHeaderMap.Rows) != 0 {
						//	for _, item := range docHeaderMap.Rows {
						for i := 0; i < len(docHeaderMap.Rows); i++ {

							item := docHeaderMap.Rows[i]
							key := fmt.Sprintf(`%v|%v`, item.String(`doc_reserv_pk`), item.String(`res_item`))
							qcItemData := resItemRows[key]

							//pack status
							var coItemId, packStatus string
							//var statusRec, statusPicking, statusFinished, indCount int
							var statusRec, statusPicking, statusFinished int

							//key = fmt.Sprintf(`%v|%v`, v.String(`doc_reserv_pk`), v.String(``))
							if mapToHeaderData != nil {
								toHeaderDataMap := mapToHeaderData.FindMap(key)
								if toHeaderDataMap != nil {
									packStatus = "C"
								}
							}

							//status
							var rtCode, rtDes string
							var countQty, orderQty float64

							if len(qcItemData) != 0 {
								//
								countQty = qcItemData.Float("pick_actual_total")
								orderQty = qcItemData.Float("pick_volume")

								if !validx.IsEmpty(qcItemData.String("co_item_id")) {
									countQty = qcItemData.Float("qty_count")
									orderQty = qcItemData.Float("qty_pick")
								}

								coItemId = qcItemData.String(`co_item_id`)

								if item.String(`withdrawn`) == "X" {
									statusFinished = 91
								} else if packStatus == "C" {
									statusFinished = 92
								}
								statusRec = qcItemData.Int(`status_rec`)

								//rto.Results[len(rto.Results)-1].DocType = qcItemData.String("doc_type")
								chkHeader.DocType = qcItemData.String("doc_type")

							}

							if countQty == 0 && orderQty == 0 {
								countQty = 0
								orderQty = item.Float("quantity")
								// if v.String(`gm_status`) == "C" {
								// 	statusFinished = 91
								// } else if v.String(`pack_status`) == "C" {
								// 	statusFinished = 92
								// }
								if item.String(`withdrawn`) == "X" {
									statusFinished = 91
								} else if packStatus == "C" {
									statusFinished = 92
								}
							}

							if validx.IsContains(resNoItemArr, key) {
								statusPicking = 1
							}

							//find article & unit
							var articleName, unitName, unitCode string
							articleId := strings.TrimLeft(item.String(`material`), "0")
							articleMap := mapArticle.FindMap(articleId)
							if articleMap != nil {
								articleName = articleMap.String(`name_th`)
								//productId = articleMap.String(`id`)
							}

							UnitMap := mapUnit.FindMap(item.String("unit"))
							//UnitMap := mapUnit.FindMap(item.String("unit"))
							if UnitMap != nil {
								unitCode = UnitMap.String(`unit_code`)
								unitName = UnitMap.String(`name_th`)
							}

							rt := seller.FindMap(articleId)
							if rt != nil {
								rtDes = rt.String("seller_name")
								rtCode = rt.String("seller_code")
							}

							chkHeader.Items = append(chkHeader.Items, gmretailcounting.GetPendingCheckListItem{
								CoItemId:       coItemId,
								DoItem:         item.String(`res_item`),
								Batch:          item.String(`batch`),
								Article:        articleId,
								ArticleName:    articleName,
								Unit:           unitCode,
								UnitName:       unitName,
								Cbin:           qcItemData.String(`cbin_code`),
								Csite:          qcItemData.String(`csite_code`),
								Csloc:          qcItemData.String(`csloc_code`),
								CountQty:       countQty,
								OrderQty:       orderQty,
								StatusRec:      statusRec,
								StatusPicking:  statusPicking,
								StatusFinished: statusFinished,
								Rt:             rtCode,
								RtName:         rtDes,
								BaseCountQty:   qcItemData.Float(`base_qty_count`),
								BaseOrderQty:   qcItemData.Float(`base_qty_pick`),
								BaseUnit:       qcItemData.String(`base_unit_code`),
							})
							if chkHeader.Items[len(chkHeader.Items)-1].StatusFinished == 0 {
								headerPick = "A"
							}
						}
					}
					docHeaderChk = append(docHeaderChk, v.String(`doc_reserv_pk`))
					if headerPick != "C" {
						rto.Results = append(rto.Results, chkHeader)
					}
				}
			}
		}
	}

	return rto, nil
}
