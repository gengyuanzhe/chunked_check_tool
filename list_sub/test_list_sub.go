package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type listConfig struct {
	Endpoints []parsedEndpoint
	AccessKey string
	SecretKey string
	Bucket    string
	Prefix    string
	Workers   int
	Timeout   time.Duration
}

type parsedEndpoint struct {
	hostPort string
	secure   bool
}

type listProgress struct {
	files        atomic.Int64
	dirs         atomic.Int64
	bytes        atomic.Int64
	listCalls    atomic.Int64
	listDuration atomic.Int64 // nanoseconds
}

func (p *listProgress) addFiles(n int) {
	if n > 0 {
		p.files.Add(int64(n))
	}
}

func (p *listProgress) addDirs(n int) {
	if n > 0 {
		p.dirs.Add(int64(n))
	}
}

func (p *listProgress) addBytes(n int64) {
	if n > 0 {
		p.bytes.Add(n)
	}
}

func (p *listProgress) addListCall(d time.Duration) {
	p.listCalls.Add(1)
	p.listDuration.Add(int64(d))
}

func formatBytes(n int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case n >= gb:
		return fmt.Sprintf("%.2fGiB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2fMiB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2fKiB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func avgObjectSize(files, totalBytes int64) string {
	if files <= 0 {
		return "N/A"
	}
	return formatBytes(totalBytes / files)
}

func startListProgress(p *listProgress) func() {
	start := time.Now()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastFiles, lastDirs int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				files := p.files.Load()
				dirs := p.dirs.Load()
				totalBytes := p.bytes.Load()
				calls := p.listCalls.Load()
				dur := time.Duration(p.listDuration.Load())
				avgDur := time.Duration(0)
				if calls > 0 {
					avgDur = dur / time.Duration(calls)
				}
				log.Printf("列举进度: 文件=%d (+%d) 目录=%d (+%d) 平均大小=%s List调用=%d 平均耗时=%v 总耗时=%v",
					files, files-lastFiles, dirs, dirs-lastDirs, avgObjectSize(files, totalBytes), calls, avgDur, dur.Round(time.Millisecond))
				lastFiles, lastDirs = files, dirs
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
		files := p.files.Load()
		dirs := p.dirs.Load()
		totalBytes := p.bytes.Load()
		calls := p.listCalls.Load()
		dur := time.Duration(p.listDuration.Load())
		avgDur := time.Duration(0)
		if calls > 0 {
			avgDur = dur / time.Duration(calls)
		}
		log.Printf("列举完成: 文件=%d 目录=%d 平均大小=%s List调用=%d 平均耗时=%v List总耗时=%v 总耗时=%v",
			files, dirs, avgObjectSize(files, totalBytes), calls, avgDur, dur.Round(time.Millisecond), time.Since(start))
	}
}

func ensureTrailingSlash(prefix string) string {
	if prefix == "" {
		return ""
	}
	if strings.HasSuffix(prefix, "/") {
		return prefix
	}
	return prefix + "/"
}

func parseKeyValueFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	kv := make(map[string]string)
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d 格式错误，须为 KEY=VALUE", path, lineNo)
		}
		key = strings.ToUpper(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		if key == "" {
			return nil, fmt.Errorf("%s:%d 键名为空", path, lineNo)
		}
		kv[key] = val
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return kv, nil
}

func parseEndpoint(raw string, port int) (parsedEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return parsedEndpoint{}, fmt.Errorf("endpoint 为空")
	}
	secure := false
	switch {
	case strings.HasPrefix(raw, "https://"):
		secure = true
		raw = strings.TrimPrefix(raw, "https://")
	case strings.HasPrefix(raw, "http://"):
		raw = strings.TrimPrefix(raw, "http://")
	}
	raw = strings.TrimRight(raw, "/")
	if host, _, ok := strings.Cut(raw, ":"); ok {
		raw = host
	}
	if raw == "" {
		return parsedEndpoint{}, fmt.Errorf("endpoint host 为空")
	}
	if port <= 0 {
		port = 80
	}
	return parsedEndpoint{hostPort: fmt.Sprintf("%s:%d", raw, port), secure: secure}, nil
}

func splitEndpoints(raw string, port int) ([]parsedEndpoint, error) {
	parts := strings.Split(raw, ",")
	out := make([]parsedEndpoint, 0, len(parts))
	seen := make(map[string]struct{})
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		ep, err := parseEndpoint(p, port)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%t|%s", ep.secure, ep.hostPort)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ENDPOINTS 不能为空")
	}
	return out, nil
}

func loadConfig(path string) (*listConfig, error) {
	kv, err := parseKeyValueFile(path)
	if err != nil {
		return nil, err
	}

	get := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := kv[k]; ok && strings.TrimSpace(v) != "" {
				return v
			}
		}
		return ""
	}

	cfg := &listConfig{
		AccessKey: get("ACCESS_KEY", "ACCESS_KEY_ID", "AK"),
		SecretKey: get("SECRET_KEY", "SECRET_ACCESS_KEY", "SK"),
		Bucket:    get("BUCKET"),
		Prefix:    strings.TrimPrefix(strings.TrimSpace(get("PREFIX")), "/"),
		Workers:   16,
		Timeout:   5 * time.Minute,
	}

	defaultPort := 80
	if s := get("PORT"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > 65535 {
			return nil, fmt.Errorf("PORT 须为 1-65535 的整数: %q", s)
		}
		defaultPort = n
	}

	endpoints, err := splitEndpoints(get("ENDPOINTS", "ENDPOINT"), defaultPort)
	if err != nil {
		return nil, err
	}
	cfg.Endpoints = endpoints

	if cfg.AccessKey == "" {
		return nil, fmt.Errorf("须配置 ACCESS_KEY")
	}
	if cfg.SecretKey == "" {
		return nil, fmt.Errorf("须配置 SECRET_KEY")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("须配置 BUCKET")
	}

	if s := get("WORKERS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("WORKERS 须为正整数: %q", s)
		}
		cfg.Workers = n
	}
	if s := get("TIMEOUT"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("TIMEOUT 无法解析: %q", s)
		}
		cfg.Timeout = d
	}
	return cfg, nil
}

func newListClients(cfg *listConfig) ([]*minio.Client, error) {
	maxConns := cfg.Workers
	if maxConns < 64 {
		maxConns = 64
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConns * len(cfg.Endpoints)
	transport.MaxIdleConnsPerHost = maxConns
	transport.MaxConnsPerHost = 0

	clients := make([]*minio.Client, 0, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		client, err := minio.New(ep.hostPort, &minio.Options{
			Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
			Secure:       ep.secure,
			Transport:    transport,
			Region:       "us-east-1",
			BucketLookup: minio.BucketLookupPath,
		})
		if err != nil {
			return nil, fmt.Errorf("初始化客户端 %s 失败: %w", ep.hostPort, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func pickClient(clients []*minio.Client) *minio.Client {
	return clients[rand.Intn(len(clients))]
}

func endpointLabels(eps []parsedEndpoint) string {
	parts := make([]string, 0, len(eps))
	for _, ep := range eps {
		scheme := "http"
		if ep.secure {
			scheme = "https"
		}
		parts = append(parts, scheme+"://"+ep.hostPort)
	}
	return strings.Join(parts, ",")
}

var listFetchOwner = false

func listOneLevelWithDelimiter(ctx context.Context, client *minio.Client, bucket, prefix string, progress *listProgress) (objectCnt int, subdirs []string, err error) {
	prefix = ensureTrailingSlash(prefix)
	listStart := time.Now()
	var totalBytes int64
	for obj := range client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{
		Prefix:     prefix,
		Recursive:  false,
		FetchOwner: &listFetchOwner,
	}) {
		if obj.Err != nil {
			progress.addListCall(time.Since(listStart))
			return objectCnt, subdirs, obj.Err
		}
		if obj.Key == "" {
			continue
		}
		if strings.HasSuffix(obj.Key, "/") {
			subdirs = append(subdirs, obj.Key)
		} else {
			objectCnt++
			if obj.Size > 0 {
				totalBytes += obj.Size
			}
		}
	}
	progress.addListCall(time.Since(listStart))
	progress.addFiles(objectCnt)
	progress.addBytes(totalBytes)
	progress.addDirs(len(subdirs))
	return objectCnt, subdirs, nil
}

// listParallelBySubdir 先 delimiter 列举拿子目录，再对每个子目录并发下发 delimiter 列举。
// 每次列举随机选择一个 endpoint。
func listParallelBySubdir(ctx context.Context, clients []*minio.Client, bucket, prefix string, workers int) error {
	prefix = ensureTrailingSlash(prefix)
	if workers <= 0 {
		workers = 1
	}

	progress := &listProgress{}
	progress.addDirs(1)
	stopProgress := startListProgress(progress)
	defer stopProgress()

	var (
		firstErr error
		errOnce  sync.Once
		sem      = make(chan struct{}, workers)
	)

	var walk func(string) error
	walk = func(p string) error {
		p = ensureTrailingSlash(p)

		sem <- struct{}{}
		_, subdirs, e := listOneLevelWithDelimiter(ctx, pickClient(clients), bucket, p, progress)
		<-sem

		if e != nil {
			return e
		}
		if len(subdirs) == 0 {
			return nil
		}

		var wg sync.WaitGroup
		for _, sub := range subdirs {
			wg.Add(1)
			go func(subPrefix string) {
				defer wg.Done()
				if subErr := walk(subPrefix); subErr != nil {
					errOnce.Do(func() { firstErr = subErr })
				}
			}(sub)
		}
		wg.Wait()
		return firstErr
	}

	return walk(prefix)
}

func printListUsage() {
	prog := os.Args[0]
	fmt.Fprintf(os.Stderr, `用法:
  %s [-config 配置文件]

parallel-subdir: 根层 delimiter 拆子目录，对每个子目录并发 delimiter="/" 逐层递归列举。
所有入参从配置文件读取，每次列举随机选择一个 ENDPOINT。

参数:
  -config string  配置文件路径，默认 test_list_sub.conf

配置项:
  ENDPOINTS   MinIO 主机地址，多个用逗号分隔，只写 host，端口由 PORT 决定
  PORT        端口，默认 80
  ACCESS_KEY  访问密钥
  SECRET_KEY  秘密密钥
  BUCKET      测试桶名
  PREFIX      测试目录前缀，可空（列举整桶）
  WORKERS     最大并发 List 请求数，默认 16
  TIMEOUT     单次列举超时，默认 5m

示例:
  %s
  %s -config test_list_sub.conf

`, prog, prog, prog)
}

func main() {
	configPath := flag.String("config", "test_list_sub.conf", "配置文件路径")
	flag.Usage = printListUsage
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		printListUsage()
		log.Fatalf("读取配置失败: %v", err)
	}

	log.Printf("配置: file=%s endpoints=%s bucket=%s prefix=%q listWorkers=%d timeout=%v",
		*configPath, endpointLabels(cfg.Endpoints), cfg.Bucket, cfg.Prefix, cfg.Workers, cfg.Timeout)

	clients, err := newListClients(cfg)
	if err != nil {
		log.Fatalf("初始化客户端失败: %v", err)
	}

	exists, err := pickClient(clients).BucketExists(context.Background(), cfg.Bucket)
	if err != nil {
		log.Fatalf("BucketExists 失败: %v", err)
	}
	if !exists {
		log.Fatalf("bucket %q 不存在，请先创建", cfg.Bucket)
	}

	listPrefix := ensureTrailingSlash(cfg.Prefix)
	listCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	if err := listParallelBySubdir(listCtx, clients, cfg.Bucket, listPrefix, cfg.Workers); err != nil {
		log.Fatalf("parallel-subdir 列举失败: %v", err)
	}
	log.Printf("测试前缀: %s", listPrefix)
}
