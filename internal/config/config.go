package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const Tick = 10 * time.Millisecond

var nodeIDPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

type Peer struct {
	ID  string
	URL string
}

type Config struct {
	NodeID   string
	Peers    []Peer
	Addr     string
	DataDir  string
	LogLevel slog.Level

	Heartbeat       time.Duration
	ElectionTimeout time.Duration
	SnapshotEvery   uint64
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		NodeID: getenv("QK_NODE_ID"),
		Addr:   valueOr(getenv("QK_ADDR"), ":7001"),
	}

	if !nodeIDPattern.MatchString(cfg.NodeID) {
		return Config{}, fmt.Errorf("QK_NODE_ID must match %s, got %q", nodeIDPattern, cfg.NodeID)
	}

	peers, err := parsePeers(getenv("QK_PEERS"))
	if err != nil {
		return Config{}, fmt.Errorf("QK_PEERS: %w", err)
	}
	if !containsID(peers, cfg.NodeID) {
		return Config{}, fmt.Errorf("QK_PEERS must contain this node %q", cfg.NodeID)
	}
	cfg.Peers = peers

	cfg.DataDir = valueOr(getenv("QK_DATA_DIR"), filepath.Join("data", cfg.NodeID))

	if err := cfg.LogLevel.UnmarshalText([]byte(valueOr(getenv("QK_LOG_LEVEL"), "info"))); err != nil {
		return Config{}, fmt.Errorf("QK_LOG_LEVEL: %w", err)
	}

	if cfg.Heartbeat, err = parseDuration(getenv, "QK_HEARTBEAT", "50ms"); err != nil {
		return Config{}, err
	}
	if cfg.ElectionTimeout, err = parseDuration(getenv, "QK_ELECTION_TIMEOUT", "150ms"); err != nil {
		return Config{}, err
	}
	snapEvery, err := strconv.ParseUint(valueOr(getenv("QK_SNAPSHOT_EVERY"), "1000"), 10, 64)
	if err != nil || snapEvery == 0 {
		return Config{}, fmt.Errorf("QK_SNAPSHOT_EVERY must be a positive number")
	}
	cfg.SnapshotEvery = snapEvery

	if cfg.Heartbeat < Tick {
		return Config{}, fmt.Errorf("QK_HEARTBEAT must be at least %s", Tick)
	}
	if cfg.ElectionTimeout < 2*cfg.Heartbeat {
		return Config{}, fmt.Errorf("QK_ELECTION_TIMEOUT (%s) must be at least twice QK_HEARTBEAT (%s)",
			cfg.ElectionTimeout, cfg.Heartbeat)
	}

	return cfg, nil
}

func (c Config) HeartbeatTicks() int {
	return int(c.Heartbeat / Tick)
}

func (c Config) ElectionTicks() int {
	return int(c.ElectionTimeout / Tick)
}

func (c Config) PeerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for _, p := range c.Peers {
		ids = append(ids, p.ID)
	}
	return ids
}

func (c Config) Quorum() int {
	return Quorum(len(c.Peers))
}

func Quorum(clusterSize int) int {
	return clusterSize/2 + 1
}

func parsePeers(raw string) ([]Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("is empty, expected id=url pairs like n1=http://localhost:7001")
	}

	var peers []Peer
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		id, rawURL, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("%q is not id=url", part)
		}
		if !nodeIDPattern.MatchString(id) {
			return nil, fmt.Errorf("peer id %q must match %s", id, nodeIDPattern)
		}
		if seen[id] {
			return nil, fmt.Errorf("peer id %q appears twice", id)
		}
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("peer %q has invalid url %q", id, rawURL)
		}
		seen[id] = true
		peers = append(peers, Peer{ID: id, URL: strings.TrimRight(rawURL, "/")})
	}

	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return peers, nil
}

func containsID(peers []Peer, id string) bool {
	for _, p := range peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

func parseDuration(getenv func(string) string, key, fallback string) (time.Duration, error) {
	d, err := time.ParseDuration(valueOr(getenv(key), fallback))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
