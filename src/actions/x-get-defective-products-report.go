package actions

import (
	"fmt"
	"strings"
	"time"

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/common"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/tablex"
	"gitlab.com/dohome-2020/go-servicex/timex"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

type DtoGetDefectiveProductsReport struct {
	StartCreateDtm  *time.Time `json:"start_create_dtm"`
	EndCreateDtm    *time.Time `json:"end_create_dtm"`
	StartPickingDtm *time.Time `json:"start_picking_dtm"`
	EndPickingDtm   *time.Time `json:"end_picking_dtm"`
	Slocs           []struct {
		Site string `json:"site"`
		Sloc string `json:"sloc"`
	} `json:"slocs"`
	DocType       []string `json:"doc_type"`
	Article       []string `json:"article"`
	DocDo         []string `json:"doc_do"`
	PickingReason []string `json:"picking_reason"`
	QcReason      []string `json:"qc_reason"`
	Rt            []string `json:"rt"`
	IsExport      bool     `json:"is_export"`
}

func XGetDefectiveProductsReport(c *gwx.Context) (any, error) {

	var dto DtoGetDefectiveProductsReport
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//connect
	dxRtPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	query := `select rpd.site as site_code
    ,rpd.sloc as sloc_code
	,rp.document_date, rp.last_pick_dtm
	, rp.document_group as doc_type
	,'' as doc_type_name
	,rpd.seller_id as rt
	,'' as rt_name
	,rpd.document_no as doc_do
	,rpd.product_id as article
	,'' as article_name
	--,rpd.pick_actual_total as pick_qty
	,rpd.pick_volume as pick_qty
	--,rpd.damaged_volume
	--,rpd.pick_actual_total
	--,rpd.pick_volume - (rpd.damaged_volume -  rpd.pick_actual_total) as short_pick_qty
	,rpd.pick_volume - rpd.pick_actual_total as short_pick_qty
	,rpd.product_unit as pick_unit 
	,'' as pick_unit_name
	, rpd.short_reason as rpd_short_reason 
	,'' as rpd_short_reason_name
	,rpd.pick_user_id as pick_user
	,'' as pick_user_name
	,qcoi.qty_count as qc_qty
	,qcoi.qty_pick  - qcoi.qty_count as short_qc_qty  
	,qcoi.unit_code as qc_unit
	,'' as qc_unit_name
	,qcoi.entry_by as qc_user
	,'' as qc_user_name
	, qcoi.short_reason  as qc_short_reason 
	,'' as qc_short_reason_name
	from retail_picking_detail rpd
	left join retail_picking rp ON rpd.document_no = rp.document_no
	left join  qc_control_order_item qcoi on rpd.document_no = qcoi.doc_no and rpd.product_seqno  = qcoi.doc_item
	where 1=1 `
	if dto.StartCreateDtm != nil && dto.EndCreateDtm != nil {
		query += fmt.Sprintf(`and rp.document_date::date   between '%s' and '%s'`, dto.StartCreateDtm.Format(timex.YYYYMMDD), dto.EndCreateDtm.Format(timex.YYYYMMDD))
	}
	if dto.StartPickingDtm != nil && dto.EndPickingDtm != nil {
		query += fmt.Sprintf(`and rpd.pick_dtm::date between  '%s' and '%s'`, dto.StartPickingDtm.Format(timex.YYYYMMDD), dto.EndPickingDtm.Format(timex.YYYYMMDD))
	}
	queryIn := ``
	for i := 0; i < len(dto.Slocs); i++ {
		if len(queryIn) == 0 {
			queryIn += fmt.Sprintf(`('%s','%s')`, dto.Slocs[i].Site, dto.Slocs[i].Sloc)
		} else {
			queryIn += fmt.Sprintf(`,('%s','%s')`, dto.Slocs[i].Site, dto.Slocs[i].Sloc)
		}
	}
	if !validx.IsEmpty(queryIn) {
		query += fmt.Sprintf(` and (rpd.site,rpd.sloc)  in (%s)`, queryIn)
	}
	if len(dto.DocType) > 0 {
		query += fmt.Sprintf(` and rp.document_group in ('%s')`, strings.Join(dto.DocType, `','`))
	}
	if len(dto.Article) > 0 {
		query += fmt.Sprintf(` and rpd.product_id in ('%s') `, strings.Join(dto.Article, `','`))
	}
	if len(dto.DocDo) > 0 {
		query += fmt.Sprintf(` and rpd.document_no  in ('%s')`, strings.Join(dto.DocDo, `','`))
	}
	if len(dto.PickingReason) > 0 {
		query += fmt.Sprintf(` and rpd.short_reason in ('%s')`, strings.Join(dto.PickingReason, `','`))
	}
	if len(dto.QcReason) > 0 {
		query += fmt.Sprintf(` and qcoi.short_reason in ('%s')`, strings.Join(dto.QcReason, `','`))
	}
	if len(dto.Rt) > 0 {
		query += fmt.Sprintf(` and rpd.seller_id in ('%s') `, strings.Join(dto.Rt, `','`))
	}
	table := fmt.Sprintf(`(%v ) as t`, query)
	resp, ex := tablex.ExReport(c, dxRtPicking, table, ``)
	if ex != nil {
		return nil, ex
	}

	//set data
	var deliveryCode, rt, article, pickUnit, pickUser, qcUnit, qcUser []string
	for i := 0; i < len(resp.Rows); i++ {
		deliveryCode = append(deliveryCode, resp.Rows[i].String("doc_type"))
		rt = append(rt, resp.Rows[i].String("rt"))
		article = append(article, resp.Rows[i].String("article"))
		pickUnit = append(pickUnit, resp.Rows[i].String("pick_unit"))
		pickUser = append(pickUser, resp.Rows[i].String("pick_user"))
		qcUser = append(qcUser, resp.Rows[i].String("qc_user"))

		if !validx.IsEmpty(resp.Rows[i].String("qc_unit")) {
			qcUnit = append(qcUnit, resp.Rows[i].String("qc_unit"))
		}
		//shortReson = append(shortReson, resp.Rows[i].String("rpd_short_reason"))
		//shortReson = append(shortReson, resp.Rows[i].String("qc_short_reason"))
	}

	delivery, ex := common.FindDocumentGroup(deliveryCode)
	if ex != nil {
		return nil, ex
	}

	seller, ex := common.FindRt(rt)
	if ex != nil {
		return nil, ex
	}

	articleDest, ex := common.FindProducts(article)
	if ex != nil {
		return nil, ex
	}

	unitPick, ex := common.FindUnit(pickUnit)
	if ex != nil {
		return nil, ex
	}

	pickUserDest, ex := common.FindUserId(pickUser)
	if ex != nil {
		return nil, ex
	}

	var qcUnitDest *sqlx.Rows
	if len(qcUnit) != 0 {
		qcUnitDest, ex = common.FindUnit(qcUnit)
		if ex != nil {
			return nil, ex
		}
	}

	qcUserDest, ex := common.FindUserId(qcUser)
	if ex != nil {
		return nil, ex
	}

	RpdShortReson, ex := common.FindShortReson()
	if ex != nil {
		return nil, ex
	}

	qcShortReson, ex := common.FindShortResonQc()
	if ex != nil {
		return nil, ex
	}

	//export
	rowReport, ex := dxRtPicking.QueryScan(`select null as " "`)
	if ex != nil {
		return nil, c.Error(ex)
	}

	//update rtos
	rto := []gmretailcounting.RtoGetDefectiveProductsReport{}
	for _, v := range resp.Rows {

		//find data
		delivery := delivery.FindMap(v.String("doc_type"))
		seller := seller.FindMap(v.String("rt"))
		articleDest := articleDest.FindMap(v.String("article"))
		unitPick := unitPick.FindMap(v.String("pick_unit"))
		pickUserDest := pickUserDest.FindMap(v.String("pick_user"))
		qcUserDest := qcUserDest.FindMap(v.String("qc_user"))

		var qcUnitDestData, RpdShortResonData, qcShortResonData *sqlx.Map
		if qcUnitDest != nil {
			qcUnitDestData = qcUnitDest.FindMap(v.String("qc_unit"))
		}
		if !validx.IsEmpty(v.String("rpd_short_reason")) {
			RpdShortResonData = RpdShortReson.FindMap(v.String("rpd_short_reason"))
		}
		if !validx.IsEmpty(v.String("qc_short_reason")) {
			qcShortResonData = qcShortReson.FindMap(v.String("qc_short_reason"))
		}

		var docTypeName, rtName, articleName, pickUnitName, pickUserName, qcUnitName, qcUserName, rpdShortReasonName, qcShortReasonName string
		if delivery != nil {
			docTypeName = delivery.String("description")
		}
		if seller != nil {
			rtName = seller.String("seller_name")
		}
		if articleDest != nil {
			articleName = articleDest.String("name_th")
		}
		if unitPick != nil {
			pickUnitName = unitPick.String("name_th")
		}
		if pickUserDest != nil {
			pickUserName = fmt.Sprintf(`%v %v`, pickUserDest.String("first_name"), pickUserDest.String("last_name"))
		}
		if qcUnitDestData != nil {
			qcUnitName = qcUnitDestData.String("name_th")
		}
		if qcUserDest != nil {
			qcUserName = fmt.Sprintf(`%v %v`, qcUserDest.String("first_name"), qcUserDest.String("last_name"))
		}
		if RpdShortResonData != nil {
			rpdShortReasonName = RpdShortResonData.String("desc")
		}
		if qcShortResonData != nil {
			qcShortReasonName = qcShortResonData.String("creason_desc")
		}

		if dto.IsExport {
			rowReport.Rows = append(rowReport.Rows, sqlx.Map{
				`Site`: v.String(`site_code`),
				`Sloc`: v.String(`sloc_code`),
				`วันที่สร้างเอกสาร`:          v.String(`document_date`),
				`วันที่จัด`:                  v.String(`last_pick_dtm`),
				`ประเภทเอกสาร`:               docTypeName,
				`แผนกขาย`:                    v.String(`rt`),
				`ชื่อแผนกขาย`:                rtName,
				`เลขที่ใบจัด`:                v.String(`doc_do`),
				`รหัสสินค้า`:                 v.String(`article`),
				`ชื่อสินค้า`:                 articleName,
				`ปริมาณจำนวนทั้งหมด`:         v.Int(`pick_qty`),
				`ปริมาณที่จัดไม่ครบ`:         v.Int(`short_pick_qty`),
				`หน่วยจัด`:                   pickUnitName,
				`รหัสสาเหตุที่จัดไม่ครบ`:     v.String(`rpd_short_reason`),
				`ชื่อสาเหตุที่จัดไม่ครบ`:     rpdShortReasonName,
				`รหัสพนักงาน(ผู้ดำเนินการ)`:  v.String(`pick_user`),
				`ชื่อพนักงาน(ผู้ดำเนินการ)`:  pickUserName,
				`ปริมาณที่ตรวจสอบไม่ครบ`:     v.Int(`short_qc_qty`),
				`หน่วยนับ`:                   qcUnitName,
				`รหัสสาเหตุที่ตรวจสอบไม่ครบ`: v.String(`qc_short_reason`),
				`ชื่อสาเหตุที่ตรวจสอบไม่ครบ`: qcShortReasonName,
				`รหัสพนักงาน(ผู้ตรวจสอบ)`:    v.String(`qc_user`),
				`ชื่อพนักงาน(ผู้ตรวจสอบ)`:    qcUserName,
			})
		} else {
			rto = append(rto, gmretailcounting.RtoGetDefectiveProductsReport{
				Article:            v.String(`article`),
				ArticleName:        articleName,
				DocDo:              v.String(`doc_do`),
				DocType:            v.String(`doc_type`),
				DocTypeName:        docTypeName,
				DocumentDate:       v.Time(`document_date`),
				LastPickDtm:        v.Time(`last_pick_dtm`),
				PickQty:            v.Int(`pick_qty`),
				PickUnit:           v.String(`pick_unit`),
				PickUnitName:       pickUnitName,
				PickUser:           v.String(`pick_user`),
				PickUserName:       pickUserName,
				QcQty:              v.Int(`qc_qty`),
				QcShortReason:      v.String(`qc_short_reason`),
				QcShortReasonName:  qcShortReasonName,
				QcUnit:             v.String(`qc_unit`),
				QcUnitName:         qcUnitName,
				QcUser:             v.String(`qc_user`),
				QcUserName:         qcUserName,
				RpdShortReason:     v.String(`rpd_short_reason`),
				RpdShortReasonName: rpdShortReasonName,
				Rt:                 v.String(`rt`),
				RtName:             rtName,
				ShortPickQty:       v.Int(`short_pick_qty`),
				ShortQcQty:         v.Int(`short_qc_qty`),
				SiteCode:           v.String(`site_code`),
				SlocCode:           v.String(`sloc_code`),
			})
		}
	}

	if dto.IsExport {
		rowReport.Columns = append(rowReport.Columns,
			`Site`,
			`Sloc`,
			`วันที่สร้างเอกสาร`,
			`วันที่จัด`,
			`ประเภทเอกสาร`,
			`แผนกขาย`,
			`ชื่อแผนกขาย`,
			`เลขที่ใบจัด`,
			`รหัสสินค้า`,
			`ชื่อสินค้า`,
			`ปริมาณจำนวนทั้งหมด`,
			`ปริมาณที่จัดไม่ครบ`,
			`หน่วยจัด`,
			`รหัสสาเหตุที่จัดไม่ครบ`,
			`ชื่อสาเหตุที่จัดไม่ครบ`,
			`รหัสพนักงาน(ผู้ดำเนินการ)`,
			`ชื่อพนักงาน(ผู้ดำเนินการ)`,
			`ปริมาณที่ตรวจสอบไม่ครบ`,
			`หน่วยนับ`,
			`รหัสสาเหตุที่ตรวจสอบไม่ครบ`,
			`ชื่อสาเหตุที่ตรวจสอบไม่ครบ`,
			`รหัสพนักงาน(ผู้ตรวจสอบ)`,
			`ชื่อพนักงาน(ผู้ตรวจสอบ)`,
		)

		rowReport.RemoveIndex(0)
		rx, err := common.CreateFileReportfunc(rowReport, 1, `retail-counting-get-defective-products-report`, `retail-counting`)
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
