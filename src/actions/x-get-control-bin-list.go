package actions

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/validx"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

func XGetControlBinList(c *gwx.Context) (any, error) {

	// Incoming variable
	dto := gmretailcounting.DtoGetControlBinList{}
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	for i := 0; i < len(dto.CoItemId); i++ {
		if validx.IsEmpty(dto.CoItemId[i]) {
			return nil, c.ErrorBadRequest("กรุณาตรวจสอบรายการที่ยังไม่ยืนยันจัด")
		}
	}

	// Outgoing variable
	rto := gmretailcounting.RtoControlBinList{}

	// Initiate database(retail_picking) connection
	// dxRetailPicking, ex := pg.RetailPickingWrite()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRetailPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	// dxDocuments, ex := pg.DocumentsRead()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxDocuments, ex := sqlx.ConnectPostgresRO("dh_documents")
	if ex != nil {
		return nil, ex
	}

	// Get
	var itemIdArr []string
	for _, v := range dto.CoItemId {
		if !validx.IsContains(itemIdArr, v) {
			itemIdArr = append(itemIdArr, v)
		}
	}

	// Find in ControlOrder
	chkRs := ``
	sqlStatement := fmt.Sprintf(`select qcoi.co_item_id, qcoi.doc_no, qcc.csite_code, qcc.csloc_code,qcoi.doc_type 
	from qc_control_order_item qcoi
	left join qc_confirm_cbin qcc on qcoi.co_item_id = qcc.co_item_id 
	where qcoi.co_item_id in ('%s')`, strings.Join(itemIdArr, "','"))
	qcRows, ex := dxRetailPicking.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}

	// Use do+item to find bin of each item
	doItemIdArr := []string{}
	for _, v := range qcRows.Rows {
		//chk id ที่ไม่มี
		if !validx.IsContains(itemIdArr, v.String(`co_item_id`)) {
			return nil, fmt.Errorf(`ไม่พบ ID ที่ต้องการ`)
		}
		//เก็บ DO
		if !validx.IsContains(doItemIdArr, v.String(`doc_no`)) {
			doItemIdArr = append(doItemIdArr, v.String(`doc_no`))
		}
		if v.String("doc_type") == "RS" {
			chkRs = "X"
		}
	}

	// Find ship_con by do
	sqlStatement = fmt.Sprintf(`select shipping_cond from doc_do_header ddh where ddh.doc_do_pk in ('%s')`, strings.Join(doItemIdArr, `','`))
	doRows, ex := dxDocuments.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}
	//
	if !validx.IsEmpty(chkRs) {
		doRows.Rows = append(doRows.Rows, sqlx.Map{
			`shipping_cond`: "X",
		})
	}

	//เก็บ ship_con จาก doc_do
	var doShipCon []string
	if len(doRows.Rows) != 0 {
		for _, v := range doRows.Rows {
			if !validx.IsContains(doShipCon, v.String(`shipping_cond`)) {
				doShipCon = append(doShipCon, v.String(`shipping_cond`))
			}
		}
	}

	// Find shipcon by csite csloc
	sqlStatement = fmt.Sprintf(`select ship_con, csite_code, csloc_code, cbin_code
	from qc_control_bin qcb 
	left join qc_control_bin_shipcon qcbs on qcb.cbin_id = qcbs.cbin_id
	where (qcb.csite_code,qcb.csloc_code) in (('%v','%v'))`, dto.Site, dto.Sloc)
	binRows, ex := dxRetailPicking.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}
	binMap := binRows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`cbin_code`)
	})

	//เก็บ shipcon ที่ไม่ได้ผูก
	var notUseShip []string
Exit:
	for i := 0; i < len(doShipCon); i++ {
		for _, v := range binMap.Rows {
			if v.String(`ship_con`) == doShipCon[i] {
				continue Exit
			}
		}
		notUseShip = append(notUseShip, doShipCon[i])
	}

	var cbinCode []string
	for _, v := range binMap.Rows {
		//chk cbin
		if !validx.IsContains(cbinCode, v.String(`cbin_code`)) {
			//filter cbin_code เดียวกัน
			data := binMap.FilterMap(v.String(`cbin_code`))

			//chk shicon ว่าง
			var shipChk []string
			for _, v := range data.Rows {
				if !validx.IsEmpty(v.String(`ship_con`)) {
					shipChk = append(shipChk, v.String(`ship_con`))
				} else {
					if len(notUseShip) != 0 {
						shipChk = append(shipChk, notUseShip...)
					}
				}
			}

			//chk ship_con ถ้ามีให้เป็น true ต้องมีทุกตัว
			ress := true
			for _, ship := range doShipCon {
				if !validx.IsContains(shipChk, ship) {
					ress = false
					break
				}
			}

			if ress {
				rto.Results = append(rto.Results, gmretailcounting.ControlBinListResult{
					Csite: v.String(`csite_code`),
					Csloc: v.String(`csloc_code`),
					Cbin:  v.String(`cbin_code`),
				})
			}
			cbinCode = append(cbinCode, v.String(`cbin_code`))
		}
	}

	return rto, nil
}
