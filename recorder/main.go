package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/grafov/m3u8"
)

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

type Segment struct {
	URL string
	Seq uint64
}

type Downloaded struct {
	Seq  uint64
	Data []byte
}

var (
	mapMu   sync.Mutex
	mapData []byte
	mapURL  string
)

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting go-stream-capturer (redirect-ready version)...")

	m3uURL := flag.String("url", "", "m3u8 url")
	minutes := flag.Int("minutes", 0, "recording minutes")
	split := flag.Int("split", 600, "split seconds")
	outDir := flag.String("out", "out", "output prefix")
	tag := flag.String("tag", "", "github release tag for auto upload")
	startStr := flag.String("start", "", "start time (YYYY-MM-DD HH:MM in JST)")
	flag.Parse()

	if *m3uURL == "" || *minutes == 0 {
		log.Fatal("Error: url and minutes are required")
	}

	log.Printf("Target URL: %s\n", *m3uURL)
	log.Printf("Recording Duration: %d minutes\n", *minutes)
	log.Printf("File Split Interval: %d seconds\n", *split)
	log.Printf("Release Tag: %s\n", *tag)
	if *startStr != "" {
		log.Printf("Scheduled Start Time (JST): %s\n", *startStr)
	}

	// === 開始時間までの待機処理（00秒ちょうど） ===
	if *startStr != "" {
		loc := time.FixedZone("JST", 9*60*60)
		startTime, err := time.ParseInLocation("2006-01-02 15:04", *startStr, loc)
		if err != nil {
			log.Printf("Warning: failed to parse start time (%s): %v\n", *startStr, err)
		} else {
			now := time.Now().In(loc)
			if now.Before(startTime) {
				waitDur := startTime.Sub(now)
				log.Printf("Waiting %v until recording start time (precisely at 00 seconds)...\n", waitDur)
				time.Sleep(waitDur)
				log.Println("Start time reached (00 seconds). Initiating recording immediately!")
			} else {
				log.Println("Start time has already passed. Initiating recording immediately.")
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*minutes)*time.Minute)
	defer cancel()

	// リダイレクトおよびMasterプレイリストを完全解決
	resolvedURL, err := resolvePlaylist(*m3uURL)
	if err != nil {
		log.Fatalf("Failed to resolve target playlist: %v", err)
	}
	log.Printf("Final Resolved Media Playlist URL: %s\n", resolvedURL)

	taskChan := make(chan *Segment, 100)
	resultChan := make(chan *Downloaded, 100)
	firstSeqChan := make(chan uint64, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go monitorPlaylist(ctx, &wg, *m3uURL, resolvedURL, taskChan, firstSeqChan)

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go downloader(ctx, &wg, i+1, taskChan, resultChan)
	}

	wg.Add(1)
	go writer(ctx, &wg, resultChan, firstSeqChan, *outDir, *split, *tag)

	wg.Wait()
	log.Println("Recording successfully finished and all files are processed.")
}

// リダイレクトの追跡とマスタープレイリストの解決を行う関数
func resolvePlaylist(rawURL string) (string, error) {
	currentURL := rawURL
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}

	for depth := 0; depth < 5; depth++ {
		req, err := http.NewRequest("GET", currentURL, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", defaultUserAgent)

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}

		// 【重要】リダイレクト後の最終着地URLを取得
		finalURL := resp.Request.URL

		p, listType, err := m3u8.DecodeFrom(resp.Body, true)
		resp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("failed to decode m3u8 from %s: %w", finalURL.String(), err)
		}

		if listType == m3u8.MASTER {
			master := p.(*m3u8.MasterPlaylist)
			var maxBW uint32
			var bestURI string
			for _, v := range master.Variants {
				if v.VariantParams.Bandwidth > maxBW {
					maxBW = v.VariantParams.Bandwidth
					bestURI = v.URI
				}
			}
			if bestURI == "" && len(master.Variants) > 0 {
				bestURI = master.Variants[0].URI
			}
			if bestURI == "" {
				return finalURL.String(), nil
			}

			u, err := url.Parse(bestURI)
			if err != nil {
				return "", err
			}
			// 【重要】リダイレクト後の finalURL をベースに相対パスを解決
			nextURL := finalURL.ResolveReference(u).String()
			log.Printf("[Resolver] Master playlist detected at %s. Selected variant (%d bps): %s\n", finalURL.String(), maxBW, nextURL)
			currentURL = nextURL
			continue
		} else if listType == m3u8.MEDIA {
			log.Printf("[Resolver] Media playlist found: %s\n", finalURL.String())
			return finalURL.String(), nil
		}

		return finalURL.String(), nil
	}

	return currentURL, nil
}

func monitorPlaylist(ctx context.Context, wg *sync.WaitGroup, origURL, initialPlaylistURL string, taskChan chan<- *Segment, firstSeqChan chan<- uint64) {
	defer wg.Done()
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}

	playlistURL := initialPlaylistURL
	seen := make(map[uint64]bool)
	var mu sync.Mutex
	first := true
	consecutiveErrors := 0

	fetchAndProcess := func() {
		req, err := http.NewRequestWithContext(ctx, "GET", playlistURL, nil)
		if err != nil {
			return
		}
		req.Header.Set("User-Agent", defaultUserAgent)
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[Monitor] Playlist fetch failed: %v\n", err)
			consecutiveErrors++
			// リダイレクト元URLからセッションを再取得（自動修復）
			if consecutiveErrors >= 5 && origURL != playlistURL {
				log.Printf("[Monitor] Attempting to refresh playlist from original URL: %s\n", origURL)
				if refreshed, rErr := resolvePlaylist(origURL); rErr == nil {
					playlistURL = refreshed
					consecutiveErrors = 0
					log.Printf("[Monitor] Playlist refreshed to: %s\n", playlistURL)
				}
			}
			return
		}

		// リダイレクト後のURLを常にBaseURLとして使用
		baseURL := resp.Request.URL

		p, listType, err := m3u8.DecodeFrom(resp.Body, true)
		resp.Body.Close()
		if err != nil || listType != m3u8.MEDIA {
			return
		}
		consecutiveErrors = 0

		media := p.(*m3u8.MediaPlaylist)

		// fMP4 INIT取得
		if media.Map != nil {
			mapMu.Lock()
			if mapData == nil {
				u, _ := url.Parse(media.Map.URI)
				mURL := baseURL.ResolveReference(u).String()
				mapURL = mURL
				log.Printf("[Monitor] Found EXT-X-MAP: %s\n", mURL)
				reqMap, _ := http.NewRequestWithContext(ctx, "GET", mURL, nil)
				reqMap.Header.Set("User-Agent", defaultUserAgent)
				respMap, err := client.Do(reqMap)
				if err == nil && respMap.StatusCode == 200 {
					data, _ := io.ReadAll(respMap.Body)
					respMap.Body.Close()
					mapData = data
					log.Printf("[Monitor] Got INIT map %d bytes\n", len(mapData))
				} else {
					if respMap != nil {
						respMap.Body.Close()
					}
					log.Printf("[Monitor] Failed to fetch MAP\n")
				}
			}
			mapMu.Unlock()
		}

		mu.Lock()
		var minSeq uint64 = math.MaxUint64
		var found bool

		baseSeq := media.SeqNo
		for i, seg := range media.Segments {
			if seg == nil {
				continue
			}
			seq := baseSeq + uint64(i)
			if !seen[seq] {
				seen[seq] = true
				u, _ := url.Parse(seg.URI)
				segURL := baseURL.ResolveReference(u).String()
				log.Printf("[Monitor] Found new segment -> Seq: %d\n", seq)
				taskChan <- &Segment{URL: segURL, Seq: seq}
				if first {
					if seq < minSeq {
						minSeq = seq
						found = true
					}
				}
			}
		}
		if first && found {
			log.Printf("[Monitor] First segment sequence detected: %d\n", minSeq)
			firstSeqChan <- minSeq
			first = false
		}
		mu.Unlock()
	}

	fetchAndProcess()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Monitor] Stop monitoring playlist.")
			return
		case <-ticker.C:
			fetchAndProcess()
		}
	}
}

func downloader(ctx context.Context, wg *sync.WaitGroup, workerID int, taskChan <-chan *Segment, resultChan chan<- *Downloaded) {
	defer wg.Done()
	client := &http.Client{Timeout: 30 * time.Second}
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Downloader-%d] Stop working.\n", workerID)
			return
		case task, ok := <-taskChan:
			if !ok {
				return
			}
			data := downloadWithRetry(ctx, client, task.URL)
			if data != nil {
				log.Printf("[Downloader-%d] Success Seq: %d (%d bytes)\n", workerID, task.Seq, len(data))
			} else {
				log.Printf("[Downloader-%d] Failed Seq: %d (Skipped)\n", workerID, task.Seq)
			}
			select {
			case <-ctx.Done():
				return
			case resultChan <- &Downloaded{Seq: task.Seq, Data: data}:
			}
		}
	}
}

func downloadWithRetry(ctx context.Context, client *http.Client, targetURL string) []byte {
	maxRetries := 5
	retryCount := 0
	for retryCount < maxRetries {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if err == nil {
			req.Header.Set("User-Agent", defaultUserAgent)
			resp, err := client.Do(req)
			if err == nil {
				if resp.StatusCode == 200 {
					data, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err == nil {
						return data
					}
				} else {
					resp.Body.Close()
					if resp.StatusCode == 404 || resp.StatusCode == 410 {
						if retryCount >= 2 {
							return nil
						}
					}
				}
			}
		}

		retryCount++
		if retryCount >= maxRetries {
			break
		}

		delay := float64(2) * math.Pow(2, float64(retryCount-1))
		if delay > 15 {
			delay = 15
		}
		jitter := rand.Float64() * 2
		sleepDur := time.Duration((delay + jitter) * float64(time.Second))
		log.Printf("[Retry] Waiting %v to retry downloading...\n", sleepDur)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sleepDur):
		}
	}
	return nil
}

func writer(ctx context.Context, wg *sync.WaitGroup, resultChan <-chan *Downloaded, firstSeqChan <-chan uint64, outPrefix string, splitSec int, tag string) {
	defer wg.Done()
	buffer := make(map[uint64][]byte)
	var nextSeq uint64
	firstSeqSet := false

	var currentFile *os.File
	var currentFileIdx int
	var fileOpenTime time.Time
	var currentFilename string
	var uploadWg sync.WaitGroup

	closeAndUpload := func(f *os.File, fname string) {
		f.Close()
		log.Printf("[Writer] Closed file: %s\n", fname)
		if tag != "" {
			uploadWg.Add(1)
			go func(name string) {
				defer uploadWg.Done()
				log.Printf("[Uploader] Start uploading %s to release %s\n", name, tag)
				repo := os.Getenv("GH_REPO")
				if repo == "" {
					repo = os.Getenv("GITHUB_REPOSITORY")
				}
				for retry := 0; retry < 3; retry++ {
					args := []string{"release", "upload", tag, name, "--clobber"}
					if repo != "" {
						args = append(args, "-R", repo)
					}
					cmd := exec.Command("gh", args...)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					err := cmd.Run()
					if err == nil {
						log.Printf("[Uploader] Successfully uploaded %s\n", name)
						os.Remove(name)
						return
					}
					log.Printf("[Uploader] Failed to upload %s (attempt %d/3): %v\n", name, retry+1, err)
					time.Sleep(5 * time.Second)
				}
			}(fname)
		}
	}

	closeCurrentFile := func() {
		if currentFile != nil {
			closeAndUpload(currentFile, currentFilename)
			currentFile = nil
		}
	}

	defer func() {
		closeCurrentFile()
		uploadWg.Wait()
		log.Println("[Writer] All uploads finished.")
	}()

	checkRotation := func() error {
		if currentFile == nil || time.Since(fileOpenTime).Seconds() >= float64(splitSec) {
			closeCurrentFile()
			mapMu.Lock()
			isFMP4 := len(mapData) > 0
			mapMu.Unlock()
			ext := "ts"
			if isFMP4 {
				ext = "mp4"
			}
			currentFilename = fmt.Sprintf("%s-%03d.%s", outPrefix, currentFileIdx, ext)
			f, err := os.Create(currentFilename)
			if err != nil {
				log.Printf("[Writer] Failed to create file %s: %v\n", currentFilename, err)
				return err
			}
			currentFile = f
			currentFileIdx++
			fileOpenTime = time.Now()
			log.Printf("[Writer] Created new file: %s\n", currentFilename)

			mapMu.Lock()
			if len(mapData) > 0 {
				f.Write(mapData)
				log.Printf("[Writer] Wrote INIT header (%d bytes) to %s\n", len(mapData), currentFilename)
			}
			mapMu.Unlock()
		}
		return nil
	}

	flushBuffer := func() {
		for {
			if data, ok := buffer[nextSeq]; ok {
				if len(data) > 0 {
					if err := checkRotation(); err == nil {
						currentFile.Write(data)
					}
				}
				delete(buffer, nextSeq)
				nextSeq++
				continue
			}

			if len(buffer) >= 10 {
				var keys []uint64
				for k := range buffer {
					if k > nextSeq {
						keys = append(keys, k)
					}
				}
				if len(keys) > 0 {
					sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
					minFutureSeq := keys[0]
					log.Printf("[Writer] Sequence gap detected! Skipping missing segments from %d to %d\n", nextSeq, minFutureSeq)
					nextSeq = minFutureSeq
					continue
				}
			}
			break
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case seq := <-firstSeqChan:
			nextSeq = seq
			firstSeqSet = true
			log.Printf("[Writer] Initialized nextSeq to %d\n", nextSeq)
			flushBuffer()
		case res := <-resultChan:
			data := res.Data
			if data == nil {
				data = []byte{}
			}

			if !firstSeqSet {
				buffer[res.Seq] = data
				continue
			}

			if res.Seq < nextSeq {
				if nextSeq > 1000 && res.Seq < 100 {
					log.Printf("[Writer] Stream discontinuity reset detected (Seq %d -> %d). Resetting nextSeq.\n", nextSeq, res.Seq)
					nextSeq = res.Seq
					buffer = make(map[uint64][]byte)
				} else {
					continue
				}
			}

			buffer[res.Seq] = data
			flushBuffer()
		}
	}
}
