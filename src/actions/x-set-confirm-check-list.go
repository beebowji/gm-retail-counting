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

func XSetConfirmCheckList(c *gwx.Context) (any, error) {

	// Incoming variable
	dto := []gmretailcounting.DtoSetConfirmCheckList{}

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

	// Outgoing variable
	rto := gmretailcounting.RtoSetCountCheckList{}
	// Map data by co_item_id and get distinct co_item_id
	itemArr := map[string]gmretailcounting.DtoSetConfirmCheckList{}
	coItemArr := []string{}
	for _, v := range dto {
		if !validx.IsContains(coItemArr, v.CoItemId) {
			coItemArr = append(coItemArr, v.CoItemId)
		}
		itemArr[v.CoItemId] = v
	}

	// qc_control_order_item table empty
	qcoiTable, ex := dxRetailPicking.TableEmpty("qc_control_order_item")
	if ex != nil {
		return nil, ex
	}

	// Query in qc_confirm_cbin by array of co_item_id
	sqlStatement := fmt.Sprintf(`SELECT * FROM qc_control_order_item qcoi WHERE qcoi.co_item_id in ('%s') and qcoi.status_rec IN (0,1,2)`, strings.Join(coItemArr, "','"))
	doRows, ex := dxRetailPicking.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}
	for _, v := range doRows.Rows {
		coItemData := itemArr[v.String(`co_item_id`)]
		isShort := false
		ErrMsg := "Success"
		if coItemData.Qty < v.Float(`qty_pick`) {
			isShort = true
		}
		if coItemData.Qty > v.Float(`qty_pick`) {
			ErrMsg = "Overpick"
		} else {
			qcoiTable.Rows = append(qcoiTable.Rows, sqlx.Map{
				`co_item_id`:     v.String(`co_item_id`),
				`site_code`:      v.String(`site_code`),
				`sloc_code`:      v.String(`sloc_code`),
				`doc_type`:       v.String(`doc_type`),
				`doc_no`:         v.String(`doc_no`),
				`doc_item`:       v.String(`doc_item`),
				`rt`:             v.String(`rt`),
				`article_code`:   v.String(`article_code`),
				`qty_count`:      coItemData.Qty,
				`qty_pick`:       v.Float(`qty_pick`),
				`unit_code`:      coItemData.Unit,
				`is_short`:       isShort,
				`short_reason`:   coItemData.Reason,
				`status_rec`:     2,
				`entry_dtm`:      v.TimePtr(`entry_dtm`),
				`entry_by`:       v.String(`entry_by`),
				`update_dtm`:     v.TimePtr(`update_dtm`),
				`update_by`:      v.String(`update_by`),
				`base_qty_count`: coItemData.BaseQty,
			})
		}

		// Append outgoing object
		rto.Results = append(rto.Results, gmretailcounting.SetCountCheckListResult{
			CoItemId:   v.String(`co_item_id`),
			QtyNeed:    coItemData.Qty,
			QtyRequire: v.Float(`qty_pick`),
			QtyCount:   v.Float(`qty_count`),
			ErrorMsg:   ErrMsg,
			DoNo:       v.String(`doc_no`),
			DoItem:     v.String(`doc_item`),
			Article:    v.String(`article_code`),
			Unit:       coItemData.Unit,
		})
	}
	if ex := dxRetailPicking.Transaction(func(t *sqlx.Tx) error {

		colsConflict := []string{`co_item_id`}
		_, ex = t.InsertUpdateBatches(`qc_control_order_item`, qcoiTable, colsConflict, 100)
		if ex != nil {
			return ex
		}
		return nil
	}); ex != nil {
		return nil, ex
	}

	return rto, nil
}
