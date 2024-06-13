package actions

import (
	"fmt"
	"sort"
	"strings"

	"gitlab.com/dohome-2020/gm-retail-counting.git/src/common"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/validx"
)

// type DtoGetReasonShortDropdown struct {
// 	Type string `json:"type"`
// }

type RtoGetPendingQc struct {
	Results []RtoGetPendingQcResults `json:"results"`
}

type RtoGetPendingQcResults struct {
	DocNo   string                        `json:"doc_no"`
	DocType string                        `json:"doc_type"`
	Items   []RtoGetPendingQcResultsItems `json:"items"`
}

type RtoGetPendingQcResultsItems struct {
	DocItem       string  `json:"doc_item"`
	Article       string  `json:"article"`
	ArticleName   string  `json:"article_name"`
	MainBin       string  `json:"main_bin"`
	ShipCon       string  `json:"ship_con"`
	ShipConName   string  `json:"ship_con_name"`
	ReceiveDate   string  `json:"receive_date"`
	OrderCountQty float64 `json:"order_count_qty"`
	OrderMaxQty   float64 `json:"order_max_qty"`
	OrderUnit     string  `json:"order_unit"`
	StatusItem    string  `json:"status_item"`
	IsBatch       bool    `json:"is_batch"`
	IsExp         bool    `json:"is_exp"`
	IsSerial      bool    `json:"is_serial"`
}

func XGetPendingQc(c *gwx.Context) (any, error) {

	userId := c.Query(`user_id`)

	//validate
	if ex := c.Empty(userId, `Invalid user_id`); ex != nil {
		return nil, ex
	}

	dx, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	query := fmt.Sprintf(`select 
	qco.doc_no,
	qco.doc_type,
	qco.doc_item,
	qco.article_code,
	rpd.ship_con,
	qco.article_code,
	qco.site_code,
	qco.sloc_code,
	rpd.batch,
	rpd.serial_type,
	rpd.exp_rem,
	rpd.exp_total,
	rpd.pick_actual_total,
	rpd.pick_volume,
	rpd.product_unit from public.retail_picking_detail rpd
	inner join public.qc_control_order_item qco on rpd.document_no  = qco.doc_no and rpd.product_seqno  = qco.doc_item 
	where qco.entry_by =  '%v' and status_rec = 0 and rpd.i_pick_status <> 'C' `, userId)
	rows, ex := dx.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//เรียงข้อมูล
	queryMainBinIn := ``
	var articleId, docNo, reservNo, shipCound, queryMainBinInchk []string
	sort.Slice(rows.Rows, func(i, j int) bool {
		return rows.Rows[i].String("doc_type")+rows.Rows[i].String("doc_no") < rows.Rows[j].String("doc_type")+rows.Rows[j].String("doc_no")
	})

	for i := 0; i < len(rows.Rows); i++ {
		if !validx.IsContains(articleId, rows.Rows[i].String("article_code")) {
			articleId = append(articleId, rows.Rows[i].String("article_code"))
		}
		if rows.Rows[i].String("doc_type") == "DO" && !validx.IsContains(docNo, rows.Rows[i].String("doc_no")) {
			docNo = append(docNo, rows.Rows[i].String("doc_no"))
		} else if rows.Rows[i].String("doc_type") == "RS" && !validx.IsContains(reservNo, rows.Rows[i].String("doc_no")) {
			reservNo = append(reservNo, rows.Rows[i].String("doc_no"))
		}
		if !validx.IsContains(shipCound, rows.Rows[i].String("ship_con")) {
			shipCound = append(shipCound, rows.Rows[i].String("ship_con"))
		}
		key := fmt.Sprintf(`%s|%s|%s`, rows.Rows[i].String("article_code"), rows.Rows[i].String("site_code"), rows.Rows[i].String("sloc_code"))
		if !validx.IsContains(queryMainBinInchk, key) {
			if len(queryMainBinIn) == 0 {
				queryMainBinIn += fmt.Sprintf(`('%s','%s','%s')`, rows.Rows[i].String("article_code"), rows.Rows[i].String("site_code"), rows.Rows[i].String("sloc_code"))

			} else {
				queryMainBinIn += fmt.Sprintf(`,('%s','%s','%s')`, rows.Rows[i].String("article_code"), rows.Rows[i].String("site_code"), rows.Rows[i].String("sloc_code"))
			}
		}
	}
	//ArticleName หา articlemaster
	article, ex := common.FindProducts(articleId)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}
	//หา mainbin article site sloc หาใน company bin-location ถ้ามันเจอ x ให้เป็น mainbin

	//ReceiveDate มาจาก do = gm_date ใน doccuments resev = req_date
	docDo, ex := common.FindDoc(docNo, nil, nil)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}

	docReserv, ex := common.FindReservDoc(reservNo)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}

	shipCoundx, ex := common.FindShipCon(shipCound)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}

	mainBinRows, ex := common.FindMainBin(queryMainBinIn)
	if ex != nil {
		return nil, fmt.Errorf(ex.Error())
	}
	var docHeaderChk []string
	rtos := RtoGetPendingQc{}
	for i := 0; i < len(rows.Rows); i++ {
		var articleName, mainBin, shipConName, receiveDate string

		status := ``
		if rows.Rows[i].String("doc_type") == "DO" {
			res := docDo.FindMap(rows.Rows[i].String("doc_no"))
			// ไม่เอาข้อมูลที่ pick_status = C
			if res != nil {
				if res.String("pick_status") == "C" {
					status = "C"
				}
				receiveDate = res.String("gm_date")
			}
		} else if rows.Rows[i].String("doc_type") == "RS" {
			res := docReserv.FindMap(rows.Rows[i].String("doc_no"))

			receiveDate = res.String("req_date")
		}

		if status != `C` {
			key := fmt.Sprintf(`%v|%v`, rows.Rows[i].String("doc_no"), rows.Rows[i].String("doc_type"))
			if !validx.IsContains(docHeaderChk, key) {
				rtos.Results = append(rtos.Results, RtoGetPendingQcResults{
					DocNo:   strings.TrimLeft(rows.Rows[i].String(`doc_no`), "0"),
					DocType: rows.Rows[i].String(`doc_type`),
				})
				docHeaderChk = append(docHeaderChk, key)
			}

			res := shipCoundx.FindMap(rows.Rows[i].String("ship_con"))
			if res != nil {
				shipConName = res.String("ship_con_name")
			}

			res = article.FindMap(rows.Rows[i].String("article_code"))
			if res != nil {
				articleName = res.String("name_th")
			}

			keyBin := fmt.Sprintf(`%v|%v|%v`, rows.Rows[i].String("article_code"), rows.Rows[i].String("site_code"), rows.Rows[i].String("sloc_code"))
			mainBinx := mainBinRows.FindMap(keyBin)
			if mainBinx != nil {
				mainBin = mainBinx.String("bin_code")
			}

			var isBatch, IsSerial, isExp bool
			if rows.Rows[i].String("batch") == "X" {
				isBatch = true
			}
			if rows.Rows[i].String("serial_type") == "ZSD1" || rows.Rows[i].String("serial_type") == "ZSR2" {
				IsSerial = true
			}
			if rows.Rows[i].Float("exp_rem") > 0 && rows.Rows[i].Float("exp_total") > 0 {
				isExp = true
			}

			//set items
			rtos.Results[len(rtos.Results)-1].Items = append(rtos.Results[len(rtos.Results)-1].Items, RtoGetPendingQcResultsItems{
				DocItem:       rows.Rows[i].String("doc_item"),
				Article:       rows.Rows[i].String("article_code"),
				ArticleName:   articleName,
				MainBin:       mainBin,
				ShipCon:       rows.Rows[i].String("ship_con"),
				ShipConName:   shipConName,
				ReceiveDate:   receiveDate,
				OrderCountQty: rows.Rows[i].Float("pick_actual_total"),
				OrderMaxQty:   rows.Rows[i].Float("pick_volume"),
				OrderUnit:     rows.Rows[i].String("product_unit"),
				StatusItem:    "P", //ถามเหงี่ยนไอซ์ status_rec = 0  ==== P
				IsBatch:       isBatch,
				IsExp:         isExp,
				IsSerial:      IsSerial,
			})
			status = ``
		}
	}

	return rtos, nil
}
