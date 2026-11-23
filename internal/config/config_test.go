package config

import (
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

const threePeers = "n1=http://localhost:7001,n2=http://localhost:7002,n3=http://localhost:7003"

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"QK_NODE_ID": "n2", "QK_PEERS": threePeers}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != ":7001" {
		t.Errorf("Addr = %q, want :7001", cfg.Addr)
	}
	if cfg.DataDir != filepath.Join("data", "n2") {
		t.Errorf("DataDir = %q, want data/n2", cfg.DataDir)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if len(cfg.Peers) != 3 || cfg.Peers[1] != (Peer{ID: "n2", URL: "http://localhost:7002"}) {
		t.Errorf("Peers = %+v", cfg.Peers)
	}
	if cfg.HeartbeatTicks() != 5 || cfg.ElectionTicks() != 15 {
		t.Errorf("ticks = heartbeat %d, election %d, want 5 and 15", cfg.HeartbeatTicks(), cfg.ElectionTicks())
	}
}

func TestLoadSortsPeersAndTrimsSlash(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"QK_NODE_ID": "n1",
		"QK_PEERS":   "n3=http://c:7003/, n1=http://a:7001 ,n2=http://b:7002",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := []string{cfg.Peers[0].ID, cfg.Peers[1].ID, cfg.Peers[2].ID}
	if strings.Join(got, ",") != "n1,n2,n3" {
		t.Errorf("peer order = %v, want n1,n2,n3", got)
	}
	if cfg.Peers[2].URL != "http://c:7003" {
		t.Errorf("URL = %q, want trailing slash removed", cfg.Peers[2].URL)
	}
}

func TestLoadRejectsBadInput(t *testing.T) {
	cases := map[string]map[string]string{
		"missing node id":    {"QK_PEERS": threePeers},
		"bad node id":        {"QK_NODE_ID": "N1!", "QK_PEERS": threePeers},
		"missing peers":      {"QK_NODE_ID": "n1"},
		"self not in peers":  {"QK_NODE_ID": "n9", "QK_PEERS": threePeers},
		"pair without equal": {"QK_NODE_ID": "n1", "QK_PEERS": "n1http://a:7001"},
		"duplicate id":       {"QK_NODE_ID": "n1", "QK_PEERS": "n1=http://a:7001,n1=http://b:7002"},
		"bad url":            {"QK_NODE_ID": "n1", "QK_PEERS": "n1=localhost:7001"},
		"bad log level":      {"QK_NODE_ID": "n1", "QK_PEERS": threePeers, "QK_LOG_LEVEL": "loud"},
		"bad heartbeat":      {"QK_NODE_ID": "n1", "QK_PEERS": threePeers, "QK_HEARTBEAT": "fast"},
		"heartbeat too fast": {"QK_NODE_ID": "n1", "QK_PEERS": threePeers, "QK_HEARTBEAT": "5ms"},
		"election too short": {"QK_NODE_ID": "n1", "QK_PEERS": threePeers, "QK_ELECTION_TIMEOUT": "60ms"},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(env(values)); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestQuorum(t *testing.T) {
	cases := []struct{ size, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {7, 4},
	}
	for _, c := range cases {
		if got := Quorum(c.size); got != c.want {
			t.Errorf("Quorum(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}
