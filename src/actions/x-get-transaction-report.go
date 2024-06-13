package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/common"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/timex"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

func XGetTransactionReport(c *gwx.Context) (any, error) {

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
	dxRtPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	//query DocDo
	// docDo, ex := common.FindDoc(dto.DoNo, dto.CustomerQueue, dto.DocRef)
	// if ex != nil {
	// 	return nil, fmt.Errorf(ex.Error())
	// }
	if len(dto.CustomerQueue) > 0 || len(dto.DocRef) > 0 {
		rowsDo, ex := common.FindDoc(dto.DoNo, dto.CustomerQueue, dto.DocRef)
		if ex != nil {
			return nil, fmt.Errorf(ex.Error())
		}
		if len(rowsDo.Rows) < 1 {
			return []gmretailcounting.RtoGetTransactionReport{}, nil
		} else {
			dto.DoNo = nil
		}
		for i := 0; i < len(rowsDo.Rows); i++ {
			dto.DoNo = append(dto.DoNo, rowsDo.Rows[i].String("doc_do_pk"))
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

	var cbins []string
	if len(dto.Cbins) != 0 {
		for _, v := range dto.Cbins {
			text := fmt.Sprintf(`('%v','%v','%v')`, v.Csite, v.Csloc, v.Cbin)
			cbins = append(cbins, text)
		}
	}

	//query transaction log
	queryTransaction := `select * from qc_control_transaction_log qct where 1=1 `
	if len(cbins) != 0 {
		queryTransaction += fmt.Sprintf(`and (qct.to_site_code,qct.to_sloc_code,qct.to_bin_code) in (%s) `, strings.Join(cbins, `,`))
	}
	if len(siteSloc) != 0 {
		queryTransaction += fmt.Sprintf(`and (qct.site_code,qct.sloc_code) in (%s) `, strings.Join(siteSloc, `,`))
	}
	if len(dto.DoNo) != 0 {
		queryTransaction += fmt.Sprintf(`and qct.doc_no in ('%s') `, strings.Join(dto.DoNo, `','`))
	}

	if dto.StartDtm != nil && dto.EndDtm != nil {
		startDate := dto.StartDtm.Local().Format(timex.YYYYMMDD)
		endDate := dto.EndDtm.Local().Format(timex.YYYYMMDD)
		queryTransaction += fmt.Sprintf(`and to_char(qct.entry_dtm, 'yyyy-mm-dd') between '%v' and '%v'`, startDate, endDate)
	}
	rowTransaction, ex := dxRtPicking.QueryScan(queryTransaction)
	if ex != nil {
		return nil, ex
	}

	//เก็บ articleCode & UnitCode & user
	var articleCode, UnitCode, personId, docDo []string
	if len(rowTransaction.Rows) != 0 {
		for _, v := range rowTransaction.Rows {
			articleCode = append(articleCode, v.String(`article_code`))
			UnitCode = append(UnitCode, v.String(`unit_code`))
			personId = append(personId, v.String(`entry_by`))

			docDo = append(docDo, v.String("doc_no"))
		}
	}

	//query article master
	var rowArticle, rowUnit, rowUser *sqlx.Rows
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
	//query employee on company
	if len(personId) != 0 {
		rowUser, ex = common.FindUserId(personId)
		if ex != nil {
			return nil, fmt.Errorf(ex.Error())
		}
	}

	//query DocDo
	rowsDo, ex := common.FindDoc(docDo, dto.CustomerQueue, dto.DocRef)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}
	if len(rowsDo.Rows) < 1 {
		return []gmretailcounting.RtoGetTransactionReport{}, nil
	}

	//export
	rowReport, ex := dxRtPicking.QueryScan(`select null as " "`)
	if ex != nil {
		return nil, c.Error(ex)
	}

	rto := []gmretailcounting.RtoGetTransactionReport{}
	if len(rowTransaction.Rows) != 0 {
		for _, v := range rowTransaction.Rows {

			//filter data
			doc := rowsDo.FindMap(v.String(`doc_no`))
			article := rowArticle.FindMap(v.String(`article_code`))
			unit := rowUnit.FindMap(v.String(`unit_code`))
			user := rowUser.FindMap(v.String(`entry_by`))

			var customerQ, articleName, unitName, userName, docRef string
			if doc != nil {
				customerQ = doc.String(`text1`)
				docRef = doc.String(`doc_ref`)
			}
			if article != nil {
				articleName = article.String(`name_th`)
			}
			if unit != nil {
				unitName = unit.String(`name_th`)
			}
			if user != nil {
				userName = user.String(`first_name`) + " " + user.String(`last_name`)
			}

			if dto.IsExport {
				rowReport.Rows = append(rowReport.Rows, sqlx.Map{
					`Site`:        v.String(`site_code`),
					`Sloc`:        v.String(`sloc_code`),
					`เลขที่คิว`:   customerQ,
					`เลขที่ใบจัด`: v.String(`doc_no`),
					`เอกสารอ้างอิง`:        docRef,
					`รหัสสินค้า`:           v.String(`article_code`),
					`ชื่อสินค้า`:           articleName,
					`จำนวน`:                v.Int(`qty_pick`),
					`หน่วย`:                v.String(`unit_code`),
					`จุดวางต้นทาง`:         v.String(`fr_bin_code`),
					`จุดวางปลายทาง`:        v.String(`to_bin_code`),
					`วันเวลาที่เคลือนย้าย`: v.TimePtr(`entry_dtm`),
					`รหัสพนักงาน`:          v.String(`entry_by`),
					`ชื่อพนักงาน`:          userName,
					`Movement type`:        v.String(`trans_type`),
				})
			} else {
				rto = append(rto, gmretailcounting.RtoGetTransactionReport{
					Site:          v.String(`site_code`),
					Sloc:          v.String(`sloc_code`),
					CustomerQueue: customerQ,
					DoNo:          v.String(`doc_no`),
					DocRef:        docRef,
					Article:       v.String(`article_code`),
					ArticleName:   articleName,
					Qty:           v.Int(`qty_pick`),
					Unit:          v.String(`unit_code`),
					UnitName:      unitName,
					FrCbin:        v.String(`fr_bin_code`),
					FrCsite:       v.String(`fr_site_code`),
					FrCsloc:       v.String(`fr_sloc_code`),
					ToCbin:        v.String(`to_bin_code`),
					ToCsite:       v.String(`to_site_code`),
					ToCsloc:       v.String(`to_sloc_code`),
					TransDtm:      v.TimePtr(`entry_dtm`),
					TransUser:     v.String(`entry_by`),
					TransUserName: userName,
					TransType:     v.String(`trans_type`),
				})
			}
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
			`จุดวางต้นทาง`,
			`จุดวางปลายทาง`,
			`วันเวลาที่เคลือนย้าย`,
			`รหัสพนักงาน`,
			`ชื่อพนักงาน`,
			`Movement type`,
		)

		rowReport.RemoveIndex(0)
		rx, err := common.CreateFileReportfunc(rowReport, 1, `retail-counting-transaction-report`, `retail-counting`)
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
