package httpapi

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/manojks1999/kv-store/logger"
	"github.com/manojks1999/kv-store/raftnode"
)

type PeerServer struct {
	RaftNode *raftnode.RaftNode
}

func (ps *PeerServer) ServeHTTP(peerListenURL string) {
	r := mux.NewRouter()
	r.HandleFunc("/raft", ps.RaftNode.Transport.Receive).Methods("POST")

	peerAddr := stripHTTPPrefix(peerListenURL)
	logger.Log.Printf("Starting peer HTTP server on %s", peerAddr)
	if err := http.ListenAndServe(peerAddr, r); err != nil && err != http.ErrServerClosed {
		logger.Log.Fatalf("ListenAndServe(): %v", err)
	}
}
