package eventsource

import (
	"log"
	"net/http"
)

type subscription struct {
	channel     string
	lastEventId string
	out         chan Event
	cout        chan string
}

func (s *subscription) destroy() {
	close(s.out)
	close(s.cout)
}

type outbound struct {
	channels []string
	event    Event
}
type registration struct {
	channel    string
	repository Repository
}
type outComment struct {
	channels []string
	comment  string
}

type Server struct {
	AllowCORS     bool // Enable all handlers to be accessible from any origin
	ReplayAll     bool // Replay repository even if there's no Last-Event-Id specified
	BufferSize    int  // How many messages do we let the client get behind before disconnecting
	registrations chan *registration
	pub           chan *outbound
	subs          chan *subscription
	comments      chan *outComment
	unregister    chan *subscription
	quit          chan bool
	kill          chan string
	deadChannels  map[string]bool
	dead          bool
}

// Create a new Server ready for handler creation and publishing events
func NewServer() *Server {
	srv := &Server{
		registrations: make(chan *registration),
		pub:           make(chan *outbound),
		comments:      make(chan *outComment),
		subs:          make(chan *subscription),
		unregister:    make(chan *subscription, 2),
		quit:          make(chan bool),
		kill:          make(chan string),
		deadChannels:  make(map[string]bool),
		BufferSize:    128,
	}
	go srv.run()
	return srv
}

// Stop handling publishing
func (srv *Server) Close() {
	srv.quit <- true
}

// Create a new handler for serving a specified channel
func (srv *Server) Handler(channel string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if srv.dead {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(200)
			w.Write([]byte("This event source is no longer available"))
			// http.Error(w, "This event source is no longer available", http.StatusGone)
			return
		} else if srv.deadChannels[channel] {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(200)
			w.Write([]byte("This event source has been closed"))
			// w.WriteHeader(http.StatusNoContent)
			return
		}
		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		h.Set("Connection", "keep-alive")
		if srv.AllowCORS {
			h.Set("Access-Control-Allow-Origin", "*")
		}
		sub := &subscription{
			channel:     channel,
			lastEventId: req.Header.Get("Last-Event-ID"),
			out:         make(chan Event, srv.BufferSize),
			cout:        make(chan string, srv.BufferSize),
		}
		srv.subs <- sub
		flusher := w.(http.Flusher)
		notifier := w.(http.CloseNotifier)
		flusher.Flush()
		enc := newEncoder(w)
		for {
			select {
			case <-notifier.CloseNotify():
				srv.unregister <- sub
				return
			case ev, ok := <-sub.out:
				if !ok {
					return
				}
				if err := enc.Encode(ev); err != nil {
					srv.unregister <- sub
					log.Println(err)
					return
				}
				flusher.Flush()
			case comment, ok := <-sub.cout:
				if !ok {
					return
				}
				if err := enc.Comment(comment); err != nil {
					srv.unregister <- sub
					log.Println(err)
					return
				}
				flusher.Flush()
			}
		}
	}
}

// Register the repository to be used for the specified channel
func (srv *Server) Register(channel string, repo Repository) {
	srv.registrations <- &registration{
		channel:    channel,
		repository: repo,
	}
}

// Publish an event with the specified id to one or more channels
func (srv *Server) Publish(channels []string, ev Event) {
	srv.pub <- &outbound{
		channels: channels,
		event:    ev,
	}
}

func (srv *Server) PublishComment(channels []string, comment string) {
	srv.comments <- &outComment{
		channels: channels,
		comment:  comment,
	}
}

// Publish an event with the specified id to one or more channels
func (srv *Server) CloseChannel(channel string) {
	srv.kill <- channel
}

func replay(repo Repository, sub *subscription) {
	for ev := range repo.Replay(sub.channel, sub.lastEventId) {
		sub.out <- ev
	}
}

func (srv *Server) run() {
	subs := make(map[string]map[*subscription]struct{})
	repos := make(map[string]Repository)
	for {
		select {
		case reg := <-srv.registrations:
			repos[reg.channel] = reg.repository
		case sub := <-srv.unregister:
			sub.destroy()
			delete(subs[sub.channel], sub)
		case pub := <-srv.pub:
			for _, c := range pub.channels {
				for s := range subs[c] {
					select {
					case s.out <- pub.event:
					default:
						srv.unregister <- s
					}

				}
			}
		case cmt := <-srv.comments:
			for _, c := range cmt.channels {
				for s := range subs[c] {
					select {
					case s.cout <- cmt.comment:
					default:
						srv.unregister <- s
					}

				}
			}
		case die := <-srv.kill:
			for s := range subs[die] {
				s.destroy()
				delete(subs[die], s)
			}
			srv.deadChannels[die] = true
		case sub := <-srv.subs:
			if _, ok := subs[sub.channel]; !ok {
				subs[sub.channel] = make(map[*subscription]struct{})
			}
			subs[sub.channel][sub] = struct{}{}
			if srv.ReplayAll || len(sub.lastEventId) > 0 {
				repo, ok := repos[sub.channel]
				if ok {
					go replay(repo, sub)
				}
			}
		case <-srv.quit:
			for _, sub := range subs {
				for s := range sub {
					s.destroy()
				}
			}
			close(srv.registrations)
			close(srv.pub)
			close(srv.comments)
			close(srv.subs)
			close(srv.unregister)
			close(srv.quit)
			close(srv.kill)
			srv.dead = true
			return
		}
	}
}
