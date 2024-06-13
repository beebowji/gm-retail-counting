package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/common"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/timex"
	"gitlab.com/dohome-2020/go-servicex/tox"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

func XGetControlOrderByCbinReport(c *gwx.Context) (any, error) {

	var dto gmretailcounting.DtoGetByCbinReport
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//validate
	if ex := c.Empty(dto.StartDtm, `Invalid Start Date`); ex != nil {
		return nil, ex
	}
	if ex := c.Empty(dto.EndDtm, `Invalid End Date`); ex != nil {
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

	// dxCsMaster, ex := pg.CustomerMasterRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxCsMaster, ex := sqlx.ConnectPostgresRO(dbs.DH_CUSTOMER_MASTER)
	if ex != nil {
		return nil, ex
	}

	dxDocument, ex := sqlx.ConnectPostgresRO("dh_documents")
	if ex != nil {
		return nil, ex
	}

	//คิวรี่ qc แล้วเอา doc ไปหา document where ด้วย pack_status = C , gm_status = C
	//เอา doc_no,item  not in ใน qc
	var cbins []string
	if len(dto.Cbins) != 0 {
		for _, v := range dto.Cbins {
			text := fmt.Sprintf(`('%v','%v','%v')`, v.Csite, v.Csloc, v.Cbin)
			cbins = append(cbins, text)
		}
	}

	//เก็บ site & sloc
	var siteSloc []string
	if len(dto.Slocs) != 0 {
		for _, v := range dto.Slocs {
			text := fmt.Sprintf(`('%v','%v')`, v.Site, v.Sloc)
			siteSloc = append(siteSloc, text)
		}
	}

	queryControlOrder := `select qcoi.*, qcc.cbin_code, qcc.csite_code, qcc.csloc_code 
	from qc_control_order_item qcoi 
	inner join qc_confirm_cbin qcc on qcoi.co_item_id = qcc.co_item_id where 1=1 and cbin_code is not null `
	if len(cbins) != 0 {
		queryControlOrder += fmt.Sprintf(`and (qcc.csite_code,qcc.csloc_code,qcc.cbin_code) in (%s) `, strings.Join(cbins, `,`))
	}
	if len(siteSloc) != 0 {
		queryControlOrder += fmt.Sprintf(`and (qcoi.site_code,qcoi.sloc_code) in (%s) `, strings.Join(siteSloc, `,`))
	}
	if len(dto.DoNo) != 0 {
		queryControlOrder += fmt.Sprintf(`and qcoi.doc_no in ('%s') `, strings.Join(dto.DoNo, `','`))
	}

	if dto.StartDtm != nil && dto.EndDtm != nil {
		startDate := dto.StartDtm.UTC().Format(timex.YYYYMMDD)
		endDate := dto.EndDtm.UTC().Format(timex.YYYYMMDD)
		queryControlOrder += fmt.Sprintf(`and to_char(qcoi.entry_dtm, 'yyyy-mm-dd') between '%v' and '%v'`, startDate, endDate)
	}

	rowControlOrder, ex := dxRtPicking.QueryScan(queryControlOrder)
	if ex != nil {
		return nil, ex
	}

	var docfindGi []string
	//ไปหา document where ด้วย pack_status = C , gm_status = C
	for i := 0; i < len(rowControlOrder.Rows); i++ {
		docfindGi = append(docfindGi, rowControlOrder.Rows[i].String("doc_no"))
	}
	//----------- หาข้อมูลที่ GI แล้ว
	docGiList := ``
	if len(docfindGi) > 0 {
		res, err := findDocGi(docfindGi, dxDocument, dxRtPicking)
		if err != nil {
			return nil, ex
		}
		if res != nil && !validx.IsEmpty(tox.String(res)) {
			docGiList = tox.String(res)
		}
	}
	//-----------

	if len(dto.CustomerQueue) > 0 || len(dto.DocRef) > 0 {
		rowsDo, ex := common.FindDoc(dto.DoNo, dto.CustomerQueue, dto.DocRef)
		if ex != nil {
			return nil, fmt.Errorf(ex.Error())
		}
		if len(rowsDo.Rows) < 1 {
			return []gmretailcounting.RtoGetControlOrderByCbinReport{}, nil
		} else {
			dto.DoNo = nil
		}
		for i := 0; i < len(rowsDo.Rows); i++ {
			dto.DoNo = append(dto.DoNo, rowsDo.Rows[i].String("doc_do_pk"))
		}
	}

	// var cbins []string
	// if len(dto.Cbins) != 0 {
	// 	for _, v := range dto.Cbins {
	// 		text := fmt.Sprintf(`('%v','%v','%v')`, v.Csite, v.Csloc, v.Cbin)
	// 		cbins = append(cbins, text)
	// 	}
	// }

	// //เก็บ site & sloc
	// var siteSloc []string
	// if len(dto.Slocs) != 0 {
	// 	for _, v := range dto.Slocs {
	// 		text := fmt.Sprintf(`('%v','%v')`, v.Site, v.Sloc)
	// 		siteSloc = append(siteSloc, text)
	// 	}
	// }

	//เพิ่ม  not in doc gi
	queryControlOrder = `select qcoi.*, qcc.cbin_code, qcc.csite_code, qcc.csloc_code 
	from qc_control_order_item qcoi 
	inner join qc_confirm_cbin qcc on qcoi.co_item_id = qcc.co_item_id where 1=1 and cbin_code is not null `
	if len(cbins) != 0 {
		queryControlOrder += fmt.Sprintf(`and (qcc.csite_code,qcc.csloc_code,qcc.cbin_code) in (%s) `, strings.Join(cbins, `,`))
	}
	if len(siteSloc) != 0 {
		queryControlOrder += fmt.Sprintf(`and (qcoi.site_code,qcoi.sloc_code) in (%s) `, strings.Join(siteSloc, `,`))
	}
	if len(dto.DoNo) != 0 {
		queryControlOrder += fmt.Sprintf(`and qcoi.doc_no in ('%s') `, strings.Join(dto.DoNo, `','`))
	}
	if dto.StartDtm != nil && dto.EndDtm != nil {
		startDate := dto.StartDtm.UTC().Format(timex.YYYYMMDD)
		endDate := dto.EndDtm.UTC().Format(timex.YYYYMMDD)
		queryControlOrder += fmt.Sprintf(`and to_char(qcoi.entry_dtm, 'yyyy-mm-dd') between '%v' and '%v'`, startDate, endDate)
	}
	if len(docGiList) > 0 {
		queryControlOrder += fmt.Sprintf(`and (qcoi.doc_no,qcoi.doc_item) not in (%s) `, docGiList)
	}

	rowControlOrder, ex = dxRtPicking.QueryScan(queryControlOrder)
	if ex != nil {
		return nil, ex
	}
	if len(rowControlOrder.Rows) < 1 {
		return []gmretailcounting.RtoGetControlOrderByCbinReport{}, nil
	}
	var articleCode, UnitCode, shipTo []string
	rowControlOrder.BuildMap(func(m *sqlx.Map) string {
		articleCode = append(articleCode, m.String(`article_code`))
		UnitCode = append(UnitCode, m.String(`unit_code`))
		return m.String(`doc_no`)
	})

	docDo := []string{}
	for i := 0; i < len(rowControlOrder.Rows); i++ {
		docDo = append(docDo, rowControlOrder.Rows[i].String("doc_no"))
	}

	//query DocDo
	rowsDo, ex := common.FindDoc(docDo, dto.CustomerQueue, dto.DocRef)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}
	if len(rowsDo.Rows) < 1 {
		return []gmretailcounting.RtoGetControlOrderByCbinReport{}, nil
	}

	for _, v := range rowsDo.Rows {
		shipTo = append(shipTo, strings.TrimLeft(v.String(`ship_to`), "0"))
	}
	//query article master
	var rowArticle, rowUnit *sqlx.Rows
	if len(articleCode) != 0 {
		rowArticle, ex = common.FindProducts(articleCode)
		if ex != nil {
			return nil, fmt.Errorf(ex.Error())
		}
	}
	if len(UnitCode) != 0 {
		rowUnit, ex = common.FindUnit(UnitCode)
		if ex != nil {
			return nil, fmt.Errorf(ex.Error())
		}
	}

	queryShipTo := fmt.Sprintf(`select sap_shipto, first_name, last_name from customer_shipto where sap_shipto in ('%s')`, strings.Join(shipTo, `','`))
	rowShipTo, ex := dxCsMaster.QueryScan(queryShipTo)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapShipTo := rowShipTo.BuildMap(func(m *sqlx.Map) string {
		return m.String(`sap_shipto`)
	})

	rto := []gmretailcounting.RtoGetControlOrderByCbinReport{}
	//export
	rowReport, ex := dxRtPicking.QueryScan(`select null as " "`)
	if ex != nil {
		return nil, c.Error(ex)
	}
	for _, v := range rowControlOrder.Rows {
		var customerQ, articleName, unitName, docRef, deliDate, shipTo, shipName string

		doc := rowsDo.FindMap(v.String(`doc_no`))
		article := rowArticle.FindMap(v.String(`article_code`))
		unit := rowUnit.FindMap(v.String(`unit_code`))

		if article != nil {
			articleName = article.String(`name_th`)
		}
		if unit != nil {
			unitName = unit.String(`name_th`)
		}
		if doc != nil {
			customerQ = doc.String(`text1`)
			docRef = doc.String(`doc_ref`)
			deliDate = doc.String(`deliver_date`)
			shipTo = strings.TrimLeft(doc.String(`ship_to`), "0")

			ship := mapShipTo.FindMap(shipTo)
			shipName = ship.String(`first_name`) + " " + ship.String(`last_name`)
		}

		if dto.IsExport {
			rowReport.Rows = append(rowReport.Rows, sqlx.Map{
				`Site`:        v.String(`site_code`),
				`Sloc`:        v.String(`sloc_code`),
				`เลขที่คิว`:   customerQ,
				`เลขที่ใบจัด`: v.String(`doc_no`),
				`เอกสารอ้างอิง`:                 docRef,
				`รหัสสินค้า`:                    v.String(`article_code`),
				`ชื่อสินค้า`:                    articleName,
				`จำนวน`:                         v.Int(`qty_pick`),
				`หน่วย`:                         v.String(`unit_code`),
				`จุดวางสินค้า`:                  v.String(`cbin_code`),
				`Deliver Date (วันที่นัดรับ)`:   deliDate,
				`รหัสลูกค้า`:                    shipTo,
				`ชื่อลูกค้า`:                    shipName,
				`วันเวลานำสินค้าเข้า Stateging`: v.TimePtr(`entry_dtm`),
			})
		} else {
			rto = append(rto, gmretailcounting.RtoGetControlOrderByCbinReport{
				Site:            v.String(`site_code`),
				Sloc:            v.String(`sloc_code`),
				CustomerQueue:   customerQ,
				DoNo:            v.String(`doc_no`),
				DocRef:          docRef,
				Article:         v.String(`article_code`),
				ArticleName:     articleName,
				Qty:             v.Int(`qty_pick`),
				Unit:            v.String(`unit_code`),
				UnitName:        unitName,
				Cbin:            v.String(`cbin_code`),
				Csite:           v.String(`csite_code`),
				Csloc:           v.String(`csloc_code`),
				DeliveryDate:    deliDate,
				Shipto:          shipTo,
				ShiptoName:      shipName,
				ControlOrderDtm: v.TimePtr(`entry_dtm`),
			})
		}

	}

	if dto.IsExport {
		rowReport.Columns = append(rowReport.Columns,
			`Site`,
			`Sloc`,
			`เลขที่คิว`,
			`เลขที่ใบจัด`,
			`เอกสารอ้างอิง`,
			`รหัสสินค้า`,
			`ชื่อสินค้า`,
			`จำนวน`,
			`หน่วย`,
			`จุดวางสินค้า`,
			`Deliver Date (วันที่นัดรับ)`,
			`รหัสลูกค้า`,
			`ชื่อลูกค้า`,
			`วันเวลานำสินค้าเข้า Stateging`,
		)

		rowReport.RemoveIndex(0)
		rx, err := common.CreateFileReportfunc(rowReport, 1, `retail-counting-control-order-by-cbin-report`, `retail-counting`)
		if err != nil {
			return nil, err
		}

		fileExport := common.RtoFileExport{
			Name: rx.Name,
			URL:  rx.URL,
		}
		return fileExport, nil
	}

	return rto, nil
}

func findDocGi(docfindGi []string, dxDocument, dxRtPicking *sqlx.DB) (*string, error) {

	docGiList := ``
	dubkey := []string{}
	queryDocDo := fmt.Sprintf(`select ddh.doc_do_pk,ddi.delivery_item,ddi.gm_status,ddi.pack_status 
	from doc_do_header ddh 
		left join doc_do_items ddi on ddh.doc_do_pk = ddi.doc_do_pk 
		where ddh.doc_do_pk in ('%s')
		and (ddi.gm_status  = 'C'  or ddi.pack_status = 'C')
		 `, strings.Join(docfindGi, `','`))
	rowDoc, ex := dxDocument.QueryScan(queryDocDo)
	if ex != nil {
		return nil, ex
	}
	for i := 0; i < len(rowDoc.Rows); i++ {
		key := fmt.Sprintf(`%s|%s`, rowDoc.Rows[i].String("doc_do_pk"), rowDoc.Rows[i].String("delivery_item"))
		if !validx.IsContains(dubkey, key) {
			if len(docGiList) == 0 {
				docGiList += fmt.Sprintf(`('%s','%s')`, rowDoc.Rows[i].String("doc_do_pk"), rowDoc.Rows[i].String("delivery_item"))
				dubkey = append(dubkey, key)
			} else {
				docGiList += fmt.Sprintf(`,('%s','%s')`, rowDoc.Rows[i].String("doc_do_pk"), rowDoc.Rows[i].String("delivery_item"))
				dubkey = append(dubkey, key)
			}
		}
	}

	//reserv
	queryDocDo = fmt.Sprintf(`select doc_reserv_pk, res_no, res_item, withdrawn
	from doc_reserv_item where withdrawn = 'X'
	and doc_reserv_pk in ('%s')	
		 `, strings.Join(docfindGi, `','`))
	rowDoc, ex = dxDocument.QueryScan(queryDocDo)
	if ex != nil {
		return nil, ex
	}
	for i := 0; i < len(rowDoc.Rows); i++ {
		key := fmt.Sprintf(`%s|%s`, rowDoc.Rows[i].String("doc_reserv_pk"), rowDoc.Rows[i].String("res_item"))
		if !validx.IsContains(dubkey, key) {
			if len(docGiList) == 0 {
				docGiList += fmt.Sprintf(`('%s','%s')`, rowDoc.Rows[i].String("doc_reserv_pk"), rowDoc.Rows[i].String("res_item"))
			} else {
				docGiList += fmt.Sprintf(`,('%s','%s')`, rowDoc.Rows[i].String("doc_reserv_pk"), rowDoc.Rows[i].String("res_item"))
			}
		}
	}

	//หาที่ pack_detail
	queryDocDo = fmt.Sprintf(`select document_no,product_seqno,pack_balance from retail_packing_detail
	where document_no in ('%s') and pack_balance = 0 `, strings.Join(docfindGi, `','`))
	rowDoc, ex = dxRtPicking.QueryScan(queryDocDo)
	if ex != nil {
		return nil, ex
	}
	for i := 0; i < len(rowDoc.Rows); i++ {
		key := fmt.Sprintf(`%s|%s`, rowDoc.Rows[i].String("document_no"), rowDoc.Rows[i].String("product_seqno"))
		if !validx.IsContains(dubkey, key) {
			if len(docGiList) == 0 {
				docGiList += fmt.Sprintf(`('%s','%s')`, rowDoc.Rows[i].String("document_no"), rowDoc.Rows[i].String("product_seqno"))
			} else {
				docGiList += fmt.Sprintf(`,('%s','%s')`, rowDoc.Rows[i].String("document_no"), rowDoc.Rows[i].String("product_seqno"))
			}
		}
	}

	if len(docGiList) < 1 {
		return nil, nil
	}
	return &docGiList, nil
}
