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

func XSetCancelCheckList(c *gwx.Context) (any, error) {

	// Incoming variable
	dto := gmretailcounting.DtoSetCancelCheckList{}
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	// Get user
	userLogin, ex := jwtx.GetLoginInfo(c)
	if ex != nil {
		return nil, ex
	}
	userId := userLogin.UserInfo.UserID

	// Initiate database(retail_picking) connection
	// dxRetailPicking, ex := pg.RetailPickingWrite()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRetailPicking, ex := sqlx.ConnectPostgresRW(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	// Delete all doc(s) by incoming co_item_id
	itemIdArr := []string{}
	for _, v := range dto.CoItemId {
		if !validx.IsContains(itemIdArr, v) {
			itemIdArr = append(itemIdArr, v)
		}
	}

	itemArrRetail := ``
	// Query data for add transaction
	// Get table
	logTable, ex := dxRetailPicking.TableEmpty("qc_control_transaction_log")
	if ex != nil {
		return nil, ex
	}
	sqlStatement := fmt.Sprintf(`SELECT * FROM qc_control_order_item qcoi LEFT JOIN qc_confirm_cbin qcc ON qcoi.co_item_id = qcc.co_item_id WHERE qcoi.co_item_id IN ('%s')`, strings.Join(itemIdArr, `','`))
	qcRows, ex := dxRetailPicking.QueryScan(sqlStatement)
	if ex != nil {
		return nil, ex
	}

	for _, v := range qcRows.Rows {
		if len(itemArrRetail) == 0 {
			itemArrRetail += fmt.Sprintf(`('%s','%s')`, v.String(`doc_no`), v.String(`doc_item`))
		} else {
			itemArrRetail += fmt.Sprintf(`,('%s','%s')`, v.String(`doc_no`), v.String(`doc_item`))
		}

		logTable.Rows = append(logTable.Rows, sqlx.Map{
			`ctrans_id`:     uuid.New(),
			`site_code`:     v.String(`site_code`),
			`sloc_code`:     v.String(`sloc_code`),
			`doc_type`:      v.String(`doc_type`),
			`doc_no`:        v.String(`doc_no`),
			`doc_item`:      v.String(`doc_item`),
			`article_code`:  v.String(`article_code`),
			`unit_code`:     v.String(`unit_code`),
			`qty_pick`:      v.Float(`qty_pick`),
			`qty_count`:     v.Float(`qty_count`),
			`is_short`:      v.Bool(`is_short`),
			`short_reason`:  v.String(`short_reason`),
			`trans_type`:    "CCC",
			`fr_site_code`:  v.String(`site_code`),
			`fr_sloc_code`:  v.String(`sloc_code`),
			`fr_cbin_code`:  v.String(`cbin_code`),
			`to_csite_code`: "",
			`to_csloc_code`: "",
			`to_cbin_code`:  "",
			`entry_dtm`:     time.Now(),
			`entry_by`:      userId,
		})
	}

	if ex := dxRetailPicking.Transaction(func(t *sqlx.Tx) error {

		if len(itemIdArr) != 0 {
			// Remove from qc_confirm_cbin
			delete := fmt.Sprintf(`delete from qc_confirm_cbin
			where co_item_id in ('%v')`, strings.Join(itemIdArr, `','`))
			_, ex = t.Exec(delete)
			if ex != nil {
				return ex
			}

			// Remove from qc_control_order_item
			if len(itemIdArr) > 0 {
				delete = fmt.Sprintf(`delete from qc_control_order_item
				where co_item_id in ('%v')`, strings.Join(itemIdArr, `','`))
				_, ex = t.Exec(delete)
				if ex != nil {
					return ex
				}
			}

			if len(itemArrRetail) > 0 {
				delete = fmt.Sprintf(`update retail_picking_detail
				set is_cancel_by_qc = true
				where (document_no,product_seqno) in (%v) `, itemArrRetail)
				_, ex = t.Exec(delete)
				if ex != nil {
					return ex
				}
			}

		}

		// Add transaction(s)
		if len(logTable.Rows) != 0 {
			colsConflict := []string{`ctrans_id`}
			_, ex := t.InsertUpdateBatches(`qc_control_transaction_log`, logTable, colsConflict, 100)
			if ex != nil {
				return ex
			}
		}

		return nil
	}); ex != nil {
		return nil, ex
	}

	return nil, nil
}
