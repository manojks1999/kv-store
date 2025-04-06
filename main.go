package main

import (
	"flag"
	"strconv"

	"github.com/manojks1999/kv-store/httpapi"
	"github.com/manojks1999/kv-store/kvstore"
	"github.com/manojks1999/kv-store/raftnode"
)

func main() {
	id := flag.Uint64("id", 1, "node ID")
	clientListenURL := flag.String("listen-client-url", "http://localhost:2379", "client listen URL")
	peerListenURL := flag.String("listen-peer-url", "http://localhost:2380", "peer listen URL")
	initialCluster := flag.String("initial-cluster", "", "initial cluster configuration")
	join := flag.Bool("join", false, "join an existing cluster")
	dataDir := flag.String("data-dir", "", "snapshot dir")
	keyValueStorePath := flag.String("key-store-dir", "", "key store path")
	logDir := dataDir
	flag.Parse()

	keyValueFile := *keyValueStorePath + "/data-node" + strconv.FormatUint(*id, 10) + ".json"
	jsonStore := kvstore.NewJsonStore(keyValueFile)
	kvStore := kvstore.NewKeyValueStore(jsonStore)

	rn := raftnode.NewRaftNode(*id, kvStore, *initialCluster, *dataDir, *logDir, *join)

	apiServer := httpapi.ApiServer{rn}
	peerServer := httpapi.PeerServer{rn}
	go apiServer.ServeHTTP(*clientListenURL)
	go peerServer.ServeHTTP(*peerListenURL)
	go rn.Run()
	select {}
}
