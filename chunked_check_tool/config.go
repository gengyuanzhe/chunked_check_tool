package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Multipart check modes (multipart_check_mode): how multipart objects are
// checked for chunked corruption in check mode.
const (
	// MultipartCheckModeOff writes every multipart object to mp.txt
	// unverified — no probes.
	MultipartCheckModeOff = 0
	// MultipartCheckModeOffset asks the server for real part boundaries: LIST
	// requests carry internal-list-mp-offset: true and multipart ETags come
	// back as <md5>-<partcnt>-<off0>|<off1>|..., each part start is probed.
	// ETags that don't parse (server without the feature) fall back to mp.txt.
	MultipartCheckModeOffset = 1
	// MultipartCheckModeSegment assumes uniform part size: offsets are
	// synthesized as [0, seg, 2*seg, ...] from multipart_segment_size.
	// Legacy mode, lowest priority, slated for removal.
	MultipartCheckModeSegment = 2
)

// defaultJournalCheckpointIntervalSec is journal_checkpoint_interval_sec
// when the config leaves it out: 10 minutes.
const defaultJournalCheckpointIntervalSec = 600

type Config struct {
	Endpoints          []string `yaml:"endpoints"`
	Scheme             string   `yaml:"scheme"`
	AK                 string   `yaml:"ak"`
	SK                 string   `yaml:"sk"`
	ListType           int      `yaml:"list_type"`
	ListAPIVersion     int      `yaml:"list_api_version"`
	ListConcurrency    int      `yaml:"list_concurrency"`
	CheckConcurrency   int      `yaml:"check_concurrency"`
	OutputDir          string   `yaml:"output_dir"`
	OutputDirTimestamp bool     `yaml:"output_dir_timestamp"`
	// BackupOutputDir is the output directory for -backup-file mode. Kept
	// separate from OutputDir so a single config can drive both list/check
	// runs and backup runs without their result files mixing. Required when
	// running in backup mode (main.go enforces); ignored by list/check modes.
	// When OutputDirTimestamp is true, a sibling-suffix stamp is applied to
	// BackupOutputDir the same way it is to OutputDir.
	BackupOutputDir       string `yaml:"backup_output_dir"`
	IsCheck               bool   `yaml:"is_check"`
	IsSuccessLog          bool   `yaml:"is_success_log"`
	MultipartCheckMode    int    `yaml:"multipart_check_mode"`
	MultipartSegmentSize  int64  `yaml:"multipart_segment_size"`
	IsMultipartSuccessLog bool   `yaml:"is_multipart_success_log"`
	// WholeObjectProbeThreshold is the byte cutoff for the small-object
	// fast-path: when 0 < task.Size <= threshold, verify() issues one
	// RangeGetAt(0, threshold) reading the whole object and matches both
	// chunkSigRe and trailerRe against that single body. Objects larger
	// than threshold fall back to the multi-probe matrix (head + boundary
	// + tail). 0 disables the fast-path entirely (always multi-probe).
	WholeObjectProbeThreshold int    `yaml:"whole_object_probe_threshold"`
	NodeIsolateThreshold      int64  `yaml:"node_isolate_threshold"`
	NodeRecoverProbeInterval  int64  `yaml:"node_recover_probe_interval"`
	DialTimeout               int64  `yaml:"dial_timeout"`
	ResponseHeaderTimeout     int64  `yaml:"response_header_timeout"`
	ProgressInterval          int    `yaml:"progress_interval"`
	ObjChCapacity             int    `yaml:"obj_ch_capacity"`
	OutputChCapacity          int    `yaml:"output_ch_capacity"`
	BackupBucket              string `yaml:"backup_bucket"`
	// MaxKeys is the LIST page size AND the threshold for the
	// short-response quirk detection (IsTruncated but fewer entries than
	// requested — a server pagination quirk signal). Required, 1..1000
	// (the S3 wire max; anything above is silently truncated server-side,
	// which would skew the detection). No default: an explicit page size
	// is what makes "returned < requested" meaningful.
	MaxKeys int32 `yaml:"max_keys"`
	// EncodingTypeURL adds EncodingType=url to LIST requests: the server
	// then URL-encodes key/prefix/marker fields on the wire, which keeps
	// XML 1.0-illegal control bytes (\x1f etc.) in keys from tearing the
	// page's XML deserialization. The SDK does NOT auto-decode; the
	// s3client egress (boundary A) decodes everything back to raw form,
	// so the rest of the tool always sees decoded keys. Probe the server
	// first — a server that ignores the parameter is fine, one that
	// mis-handles it is not.
	EncodingTypeURL bool `yaml:"encoding_type_url"`
	// DebugLog turns on one diagnostic line per LIST request (request
	// marker, response counts/next marker, error) at Debug level into
	// run.log. Off by default: zero cost when silent.
	DebugLog bool `yaml:"debug_log"`
	// LatencyWindowSec is the window length of the windowed LIST latency
	// stats (latency_avg_ms/latency_max_ms on progress lines). Windowed,
	// not global: a long run's global average gets diluted by early spikes
	// and hides current-batch health. Default 60s; 0 (absent) = default,
	// negative rejected.
	LatencyWindowSec int `yaml:"latency_window_sec"`
	// JournalCheckpointIntervalSec is the time between two journal
	// checkpoints in list_type=2 scans: a kill -9 loses at most one interval
	// of scan progress. Default 600 (10 minutes); 0 (absent) = default,
	// negative rejected.
	JournalCheckpointIntervalSec int64 `yaml:"journal_checkpoint_interval_sec"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	// Default ON: append a timestamp suffix to output_dir each run unless the
	// user explicitly sets output_dir_timestamp: false. yaml.Unmarshal only
	// overwrites fields present in the YAML doc, so pre-setting true here
	// means an absent key yields true while an explicit false still wins.
	cfg.OutputDirTimestamp = true
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("endpoints must not be empty")
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	if cfg.OutputDirTimestamp {
		// Sibling-suffix form: ./out → ./out_20260908_175201. Every run gets
		// its own directory so repeated runs never append into the same files
		// and mix results. Trailing separators are trimmed first — ./out/
		// must not become a subdir named "_<stamp>" inside ./out. The same
		// stamp is applied to BackupOutputDir when configured, so a run that
		// uses both directories lands in a matched pair
		// (./out_<stamp>, ./backup_<stamp>).
		stamp := time.Now().Format("20060102_150405")
		cfg.OutputDir = strings.TrimRight(cfg.OutputDir, "/\\") + "_" + stamp
		if cfg.BackupOutputDir != "" {
			cfg.BackupOutputDir = strings.TrimRight(cfg.BackupOutputDir, "/\\") + "_" + stamp
		}
	}
	if cfg.ListConcurrency <= 0 {
		cfg.ListConcurrency = 8
	}
	if cfg.CheckConcurrency <= 0 {
		cfg.CheckConcurrency = 16
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 5000
	}
	// Small-object fast-path threshold. 0 in YAML disables the fast-path;
	// absent (also 0 after unmarshal) falls back to the default. Negative
	// values are rejected below.
	if cfg.WholeObjectProbeThreshold == 0 {
		cfg.WholeObjectProbeThreshold = 1024
	}
	if cfg.ListType == 0 {
		cfg.ListType = 2
	}
	if cfg.ListAPIVersion == 0 {
		cfg.ListAPIVersion = 1
	}
	if cfg.MultipartCheckMode < MultipartCheckModeOff || cfg.MultipartCheckMode > MultipartCheckModeSegment {
		return nil, fmt.Errorf("multipart_check_mode must be 0 (off), 1 (offset), or 2 (segment), got %d", cfg.MultipartCheckMode)
	}
	if cfg.MultipartCheckMode == MultipartCheckModeSegment && cfg.MultipartSegmentSize <= 0 {
		return nil, fmt.Errorf("multipart_check_mode=2 (segment) requires multipart_segment_size > 0 (got %d)", cfg.MultipartSegmentSize)
	}
	if cfg.WholeObjectProbeThreshold < 0 {
		return nil, fmt.Errorf("whole_object_probe_threshold must be >= 0 (got %d)", cfg.WholeObjectProbeThreshold)
	}
	// Node isolation threshold: how many process-wide node-fault errors a
	// node must accumulate before it is isolated. 0 (absent) defaults to 3;
	// 1 restores the legacy isolate-on-first-fault behavior.
	if cfg.NodeIsolateThreshold == 0 {
		cfg.NodeIsolateThreshold = 3
	}
	if cfg.NodeIsolateThreshold < 0 {
		return nil, fmt.Errorf("node_isolate_threshold must be >= 1 (got %d)", cfg.NodeIsolateThreshold)
	}
	// Node recovery probe interval in seconds; 0 disables recovery (isolation
	// is then permanent for the process lifetime). Default 60.
	if cfg.NodeRecoverProbeInterval == 0 {
		cfg.NodeRecoverProbeInterval = 60
	}
	if cfg.NodeRecoverProbeInterval < 0 {
		return nil, fmt.Errorf("node_recover_probe_interval must be >= 0 seconds (got %d)", cfg.NodeRecoverProbeInterval)
	}
	// DialTimeout caps a single TCP dial attempt against an endpoint. Default
	// 10s: without it, a black-hole IP (unrouted, no RST) hangs ~75s on macOS
	// / ~127s on Linux (OS default SYN retries). 0 = use Go net/http default
	// (no cap, fall back to OS). Negative is invalid.
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10
	}
	if cfg.DialTimeout < 0 {
		return nil, fmt.Errorf("dial_timeout must be >= 0 seconds (got %d)", cfg.DialTimeout)
	}
	// ResponseHeaderTimeout caps how long the client waits for response headers
	// after sending the request. Default 0 (no cap): a normal S3 server returns
	// headers in milliseconds, and a stuck server is caught by dial_timeout on
	// the next request's reconnect — so we don't impose a header timeout by
	// default. Users with hung-acceptor symptoms can opt in explicitly.
	if cfg.ResponseHeaderTimeout < 0 {
		return nil, fmt.Errorf("response_header_timeout must be >= 0 seconds (got %d)", cfg.ResponseHeaderTimeout)
	}
	// max_keys: required (no default), 1..1000. See the field comment.
	if cfg.MaxKeys < 1 || cfg.MaxKeys > 1000 {
		return nil, fmt.Errorf("max_keys is required and must be 1..1000 (got %d)", cfg.MaxKeys)
	}
	// Windowed latency stats window. Absent (0) = 60s default; negative is
	// meaningless and rejected.
	if cfg.LatencyWindowSec == 0 {
		cfg.LatencyWindowSec = 60
	}
	if cfg.LatencyWindowSec < 0 {
		return nil, fmt.Errorf("latency_window_sec must be >= 1 seconds (got %d)", cfg.LatencyWindowSec)
	}
	// Journal checkpoint interval in seconds; absent (0) = the default.
	// Negative is meaningless and rejected.
	if cfg.JournalCheckpointIntervalSec == 0 {
		cfg.JournalCheckpointIntervalSec = defaultJournalCheckpointIntervalSec
	}
	if cfg.JournalCheckpointIntervalSec < 0 {
		return nil, fmt.Errorf("journal_checkpoint_interval_sec must be >= 1 seconds (got %d)", cfg.JournalCheckpointIntervalSec)
	}
	return &cfg, nil
}
