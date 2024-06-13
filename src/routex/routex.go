package routex

import (
	"github.com/gin-gonic/gin"
	"gitlab.com/dohome-2020/gm-retail-counting.git/src/actions"
	"gitlab.com/dohome-2020/go-servicex/gms"
	"gitlab.com/dohome-2020/go-servicex/gwx"
	"gitlab.com/dohome-2020/go-servicex/jwtx"
)

func Routex() {

	// // https://hoohoo.top/blog/20220320172715-go-websocket/
	// hub := wss.CreateHub()
	// go hub.Run()

	// connect
	gms.GM_RETAIL_COUNTING.Connect(func(g *gwx.GX, rg *gin.RouterGroup) {
		// rGuest := rg.Group(``)
		// {
		// 	// routex.GET(rGuest, `ws/:branch/:macaddr`, func(c *gin.Context) {
		// 	// 	wss.ServeWS(hub, c)
		// 	// })
		// 	// Example>> g.POST(rGuest, `actions/kick-me`, actions.XKickMe)
		// }
		rGuard := rg.Group(``, jwtx.Guard())
		{
			g.POST(rGuard, `actions/get-ship-con-master`, actions.XGetShipConMaster)
			g.POST(rGuard, `actions/get-rt-master`, actions.XGetRTMaster)
			g.POST(rGuard, `actions/get-barcode-master`, actions.XGetBarcodeMaster)
			g.POST(rGuard, `actions/get-reason-confirm-checklist`, actions.XGetReasonConfirmChecklist)
			g.POST(rGuard, `actions/get-cbin-master`, actions.XGetCbinMaster)
			g.POST(rGuard, `actions/set-cbin-master`, actions.XSetCbinMaster)
			g.POST(rGuard, `actions/get-transaction-report`, actions.XGetTransactionReport)                  //export is true
			g.POST(rGuard, `actions/get-control-order-by-cbin-report`, actions.XGetControlOrderByCbinReport) //export is true

			g.POST(rGuard, `actions/get-pending-check-list`, actions.XGetPendingCheckList)
			g.POST(rGuard, `actions/set-confirm-check-list`, actions.XSetConfirmCheckList)
			g.POST(rGuard, `actions/set-count-check-list`, actions.XSetCountCheckList)
			g.POST(rGuard, `actions/set-control-bin`, actions.XSetControlBin)
			g.POST(rGuard, `actions/get-control-bin-list`, actions.XGetControlBinList)
			g.POST(rGuard, `actions/set-cancel-check-list`, actions.XSetCancelCheckList)
			g.POST(rGuard, `actions/get-reason-short-dropdown`, actions.XGetReasonShortDropdown)
			g.GET(rGuard, `actions/get-document-type-master`, actions.XGetDocumentTypeMaster)
			g.POST(rGuard, `actions/get-defective-products-report`, actions.XGetDefectiveProductsReport) //export is true

			g.GET(rGuard, `actions/get-pending-qc`, actions.XGetPendingQc)

		}
	})
}
