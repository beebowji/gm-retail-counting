package actions

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gitlab.com/dohome-2020/go-servicex/dbs"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/tox"
	gmretailcounting "gitlab.com/dohome-2020/go-structx/gm-retail-counting"
)

// เพิ่ม A: add  U:อับเดท D:ลบ
func XSetCbinMaster(c *gwx.Context) (any, error) {

	var dto []gmretailcounting.CBinMaster
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	// dxRtPicking, ex := pg.RetailPickingWrite()
	// if ex != nil {
	// 	return nil, ex
	// }
	dxRtPicking, ex := sqlx.ConnectPostgresRW(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	//get table
	rowsCbin, ex := dxRtPicking.TableEmpty(`qc_control_bin`)
	if ex != nil {
		return nil, ex
	}
	rowsShipCon, ex := dxRtPicking.TableEmpty(`qc_control_bin_shipcon`)
	if ex != nil {
		return nil, ex
	}

	var delete []string
	chkDub := ``
	for _, v := range dto {
		action := strings.ToLower(v.Action)
		if action == "u" {
			//======== insert
			if v.CbinId == nil {
				//ถ้าไม่เจอเลยให้เพิ่ม
				newId := uuid.New()
				rowsCbin.Rows = append(rowsCbin.Rows, sqlx.Map{
					`cbin_id`:    newId,
					`csite_code`: v.Csite,
					`csloc_code`: v.Csloc,
					`cbin_code`:  v.Cbin,
				})
				//
				if len(chkDub) == 0 {
					chkDub += fmt.Sprintf(`('%s','%s','%s')`, v.Cbin, v.Csite, v.Csloc)
				} else {
					chkDub += fmt.Sprintf(`,('%s','%s','%s')`, v.Cbin, v.Csite, v.Csloc)
				}

				for _, shipCon := range v.ShipCons {
					rowsShipCon.Rows = append(rowsShipCon.Rows, sqlx.Map{
						`cbin_id`:  newId,
						`ship_con`: shipCon.Code,
					})
				}
			} else {
				//ถ้าไม่เจอเลยให้เพิ่ม
				//newId := uuid.New()
				newId := uuid.New()

				rowsCbin.Rows = append(rowsCbin.Rows, sqlx.Map{
					`cbin_id`:    newId,
					`csite_code`: v.Csite,
					`csloc_code`: v.Csloc,
					`cbin_code`:  v.Cbin,
				})

				for _, shipCon := range v.ShipCons {
					rowsShipCon.Rows = append(rowsShipCon.Rows, sqlx.Map{
						`cbin_id`:  newId,
						`ship_con`: shipCon.Code,
					})
				}
				delete = append(delete, tox.String(v.CbinId))
			}

			//========
		} else if action == "d" && v.CbinId != nil {
			delete = append(delete, tox.String(v.CbinId))
		}
	}

	//
	if len(chkDub) > 0 {
		queryUser := fmt.Sprintf(`select * from qc_control_bin where (cbin_code,csite_code,csloc_code) in (%s)`, chkDub)
		rows, ex := dxRtPicking.QueryScan(queryUser)
		if ex != nil {
			return nil, ex
		}
		if len(rows.Rows) > 0 {
			return nil, fmt.Errorf("พบข้อมูลอยู่แล้ว")
		}
	}

	if ex = dxRtPicking.Transaction(func(t *sqlx.Tx) error {

		if len(delete) != 0 {
			//ลบ table ลูกก่อน
			deleteQuery := fmt.Sprintf(`delete from qc_control_bin_shipcon
			where cbin_id in ('%v')`, strings.Join(delete, `,`))
			_, ex = t.Exec(deleteQuery)
			if ex != nil {
				return ex
			}
			deleteQuery = fmt.Sprintf(`delete from qc_control_bin
			where cbin_id in ('%v')`, strings.Join(delete, `,`))
			_, ex = t.Exec(deleteQuery)
			if ex != nil {
				return ex
			}
		}

		if len(rowsCbin.Rows) != 0 {
			//colsConflict := []string{`csite_code`, `csloc_code`, `cbin_code`}
			_, ex := t.InsertCreateBatches(`qc_control_bin`, rowsCbin, 100)
			if ex != nil {
				return ex
			}
		}

		if len(rowsShipCon.Rows) != 0 {
			//colsConflict := []string{`cbin_id`, `ship_con`}
			_, ex := t.InsertCreateBatches(`qc_control_bin_shipcon`, rowsShipCon, 100)
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
