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

func XGetCbinMaster(c *gwx.Context) (any, error) {

	var dto gmretailcounting.DtoGetCBinMaster
	if ex := c.ShouldBindJSON(&dto); ex != nil {
		return nil, ex
	}

	//Connect
	dxRtPicking, ex := sqlx.ConnectPostgresRO(dbs.DH_RETAIL_PICKING)
	if ex != nil {
		return nil, ex
	}

	//เก็บ key
	var qryKey, setKey []string
	for _, v := range dto.Cbins {
		keyQ := fmt.Sprintf(`('%v','%v','%v')`, v.Csite, v.Csloc, v.Cbin)
		qryKey = append(qryKey, keyQ)

		text := fmt.Sprintf(`%v|%v|%v`, v.Csite, v.Csloc, v.Cbin)
		setKey = append(setKey, text)
	}

	var slocKey []string
	for _, v := range dto.Csloc {
		keyQ := fmt.Sprintf(`('%v','%v')`, v.Csite, v.Csloc)
		slocKey = append(slocKey, keyQ)

		text := fmt.Sprintf(`%v|%v`, v.Csite, v.Csloc)
		setKey = append(setKey, text)
	}

	rto := []gmretailcounting.RtoGetCBinMaster{}
	if dto.IsShipcon {
		query := `select qcb.cbin_id, csite_code, csloc_code, cbin_code, qcbs.ship_con as ship_con
		from qc_control_bin qcb
		left join qc_control_bin_shipcon qcbs on qcb.cbin_id = qcbs.cbin_id
		where 1=1`
		if len(qryKey) != 0 {
			query += fmt.Sprintf(` and (csite_code,csloc_code,cbin_code) in (%v)`, strings.Join(qryKey, `,`))
		}
		if len(slocKey) != 0 {
			query += fmt.Sprintf(` and (csite_code,csloc_code) in (%v)`, strings.Join(slocKey, `,`))
		}
		row, ex := dxRtPicking.QueryScan(query)
		if ex != nil {
			return nil, ex
		}

		var rowChk []string
		if len(row.Rows) != 0 {
			for _, v := range row.Rows {

				key := fmt.Sprintf(`%v|%v|%v`, v.String(`csite_code`), v.String(`csloc_code`), v.String(`cbin_code`))

				if !validx.IsContains(rowChk, key) {
					data := row.Filter(func(m *sqlx.Map) bool {
						return m.String(`csite_code`) == v.String(`csite_code`) &&
							m.String(`csloc_code`) == v.String(`csloc_code`) &&
							m.String(`cbin_code`) == v.String(`cbin_code`)
					})

					code := []gmretailcounting.ResultsGetDataMaster{}
					if len(data.Rows) != 0 {
						for _, d := range data.Rows {
							code = append(code, gmretailcounting.ResultsGetDataMaster{
								Code: d.String(`ship_con`),
							})
						}
					}

					rto = append(rto, gmretailcounting.RtoGetCBinMaster{
						CbinId:   v.UUID(`cbin_id`),
						Csite:    v.String(`csite_code`),
						Csloc:    v.String(`csloc_code`),
						Cbin:     v.String(`cbin_code`),
						ShipCons: code,
					})

					rowChk = append(rowChk, key)
				}
			}
		}
	} else {

		for _, v := range setKey {
			textSplit := strings.Split(v, `|`)
			cbin := ""
			csite := ""
			csloc := ""
			if len(textSplit) != 0 {
				csite = textSplit[0]
				csloc = textSplit[1]
				if len(textSplit) == 3 {
					cbin = textSplit[2]
				}
			}
			rto = append(rto, gmretailcounting.RtoGetCBinMaster{
				Csite: csite,
				Csloc: csloc,
				Cbin:  cbin,
			})
		}
	}

	return rto, nil
}
