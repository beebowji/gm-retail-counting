package common

import (
	"fmt"
	"os"
	"time"

	"gitlab.com/dohome-2020/go-servicex/excelx"
	"gitlab.com/dohome-2020/go-servicex/filex"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/reportx"
	"gitlab.com/dohome-2020/go-servicex/sqlx"
	"gitlab.com/dohome-2020/go-servicex/timex"
)

type RtoFileExport struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func CreateFileReportfunc(data *sqlx.Rows, countFile int, nameFile, reportCode string) (*RtoFileExport, error) {

	fileExport := RtoFileExport{}
	loadDate := time.Now().Local().Format(`20060102150405`)

	shareName := fmt.Sprintf(`%s-%v-%v.xlsx`, nameFile, loadDate, countFile)
	pathFile := fmt.Sprintf(`%s%s`, filex.TEMP_PATH, shareName)

	// write rows to xlsx
	ex := excelx.RowsExcel2(data, shareName)
	if ex != nil {
		return nil, ex
	}
	// upload s3 and share
	loadDate = time.Now().Local().Format(timex.SYMD)
	rx, ex := reportx.ReportLoaderUpload(loadDate, reportCode, pathFile, shareName)
	if ex != nil {
		return nil, ex
	}
	_ = os.Remove(pathFile)
	if rx == nil {
		return nil, gwx.Error500("ไม่สามารถ export file ได้")
	}

	fileExport = RtoFileExport{
		Name: shareName,
		URL:  rx.ShareLink,
	}

	return &fileExport, nil

}
