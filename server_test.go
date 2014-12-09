package eventsource

import (
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testResponder struct {
	*httptest.ResponseRecorder
}

func (t testResponder) CloseNotify() <-chan bool {
	return make(<-chan bool)
}

func TestServer(t *testing.T) {
	Convey("The EventSource server", t, func() {
		closed := false
		srv := NewServer()
		Reset(func() {
			if !closed {
				srv.Close()
			}
		})
		var channel = "test"
		var channels = []string{"test"}
		Convey("when shutdown", func() {
			r, _ := http.NewRequest("GET", "/test", nil)
			var w testResponder
			w.ResponseRecorder = httptest.NewRecorder()
			Convey("closes active connections", func() {
				time.AfterFunc(time.Microsecond, func() {
					closed = true
					srv.Close()
				})
				// This blocks while the channel is open
				srv.Handler(channel)(w, r)
				So(closed, ShouldBeTrue)
			})
			Convey("denies new connections", func() {
				closed = true
				srv.Close()
				// This shouldn't block
				srv.Handler(channel)(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				// So(w.Code, ShouldEqual, http.StatusGone)
			})
			Convey("panics when used", func() {
				closed = true
				srv.Close()
				So(func() { srv.Close() }, ShouldPanic)
				So(func() { srv.Register(channel, nil) }, ShouldPanic)
				So(func() { srv.Publish(channels, nil) }, ShouldPanic)
				So(func() { srv.PublishComment(channels, "test") }, ShouldPanic)
			})
		})
		Convey("when closing a channel", func() {
			r, _ := http.NewRequest("GET", "/test", nil)
			var w testResponder
			w.ResponseRecorder = httptest.NewRecorder()
			Convey("all connections on that channel are closed", func() {
				var chanClosed = false
				time.AfterFunc(time.Microsecond, func() {
					chanClosed = true
					srv.CloseChannel(channel)
				})
				// This blocks while the channel is open
				srv.Handler(channel)(w, r)
				So(chanClosed, ShouldBeTrue)
			})
			Convey("new connections are denied", func() {
				srv.CloseChannel(channel)
				// This shouldn't block
				srv.Handler(channel)(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				// So(w.Code, ShouldEqual, http.StatusNoContent)
			})
			Convey("subsequent publishes do nothing", func() {
				srv.CloseChannel(channel)
				var ev Event = &publication{}
				So(func() { srv.Publish(channels, ev) }, ShouldNotPanic)
			})
		})

		// TODO MOAR
	})
}
