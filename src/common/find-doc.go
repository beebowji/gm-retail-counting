package common

import (
	"fmt"
	"strings"

	"gitlab.com/dohome-2020/go-servicex/sqlx"
)

func FindDoc(doNo []string, customerQ []string, docRef []string) (*sqlx.Rows, error) {
	// connect db
	dxDocument, ex := sqlx.ConnectPostgresRO("dh_documents")
	if ex != nil {
		return nil, ex
	}

	queryDocDo := `select ddh.doc_do_pk, ddh.text1, ddh.deliver_date, ddh.ship_to, ddi.doc_ref,ddi.article_no as article_code,ddi.sun as unit_code,ddh.gm_date,ddi.pick_status
	from doc_do_header ddh 
	left join doc_do_items ddi on ddh.doc_do_pk = ddi.doc_do_pk where 1=1 `
	if len(doNo) != 0 {
		queryDocDo += fmt.Sprintf(`and ddh.doc_do_pk in ('%s') `, strings.Join(doNo, `','`))
	}
	if len(customerQ) != 0 {
		queryDocDo += fmt.Sprintf(`and text1 in ('%s') `, strings.Join(customerQ, `','`))
	}
	if len(docRef) != 0 {
		queryDocDo += fmt.Sprintf(`and doc_ref in ('%s') `, strings.Join(docRef, `','`))
	}

	queryDocDo += ` order by  ddi.pick_status asc`
	rowDocDo, ex := dxDocument.QueryScan(queryDocDo)
	if ex != nil {
		return nil, ex
	}
	//build map
	mapDocDo := rowDocDo.BuildMap(func(m *sqlx.Map) string {
		return m.String(`doc_do_pk`)
	})

	return mapDocDo, nil
}

func FindReservDoc(reservNo []string) (*sqlx.Rows, error) {
	// connect db
	dxDocument, ex := sqlx.ConnectPostgresRO("dh_documents")
	if ex != nil {
		return nil, ex
	}
	query := fmt.Sprintf(`select dr.doc_reserv_pk, dri.res_item, dri.material, dri.plant, dri.store_loc, dri.batch, dri.withdrawn, dri.req_date  
	from doc_reserv dr 
	left join doc_reserv_item dri ON dr.doc_reserv_pk = dri.doc_reserv_pk 
	where dr.doc_reserv_pk IN ('%s')`, strings.Join(reservNo, `','`))

	rows, ex := dxDocument.QueryScan(query)
	if ex != nil {
		return nil, ex
	}
	//build map
	rows.BuildMap(func(m *sqlx.Map) string {
		return m.String(`doc_reserv_pk`)
	})

	return rows, nil
}
