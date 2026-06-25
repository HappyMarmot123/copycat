package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"context"
	"encoding/json"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	jobMaxLogs   = 3000
	downloadDir  = "web_downloads"
	defaultPort  = 8080
	defaultTimeout = 30
	envFilePath  = ".env"
)

const (
	cloudinaryFolderEnv = "CLOUDINARY_AUDIO_FOLDER"
	cloudinaryCloudNameEnv = "CLOUDINARY_CLOUD_NAME"
	cloudinaryAPIKeyEnv = "CLOUDINARY_API_KEY"
	cloudinaryAPISecretEnv = "CLOUDINARY_API_SECRET"
)

type CloudinaryConfig struct {
	CloudName   string
	APIKey      string
	APISecret   string
	UploadFolder string
}

type CloudinaryUploadResult struct {
	PublicID string `json:"public_id"`
	URL      string `json:"url"`
	SecureURL string `json:"secure_url"`
	Error    *CloudinaryError `json:"error"`
}

type CloudinaryError struct {
	Message string `json:"message"`
}

type JobStatus string

const (
	statusQueued   JobStatus = "queued"
	statusRunning  JobStatus = "running"
	statusDone     JobStatus = "done"
	statusFailed   JobStatus = "failed"
	statusTimeout  JobStatus = "timeout"
)

type DownloadJob struct {
	ID        string
	URL       string
	Format    string
	Output    string
	OutputByUser bool
	Timeout   int
	Overwrite bool

	Status    JobStatus
	Error     string
	OutputPath string
	ImagePath  string
	AudioCloudinaryURL string
	AudioCloudinaryPublicID string
	ThumbnailCloudinaryURL string
	ThumbnailCloudinaryPublicID string
	ExitCode  int

	Logs       []string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	FinishedAt *time.Time
	Metadata   *YouTubeMetadata

	done bool
	mu   sync.Mutex
}

type jobStatusResponse struct {
	ID         string      `json:"id"`
	URL        string      `json:"url"`
	Format     string      `json:"format"`
	Status     JobStatus   `json:"status"`
	Error      string      `json:"error"`
	OutputPath string      `json:"outputPath"`
	ImagePath  string      `json:"imagePath"`
	AudioCloudinaryURL string `json:"audioCloudinaryUrl"`
	AudioCloudinaryPublicID string `json:"audioCloudinaryPublicId"`
	ThumbnailCloudinaryURL string `json:"thumbnailCloudinaryUrl"`
	ThumbnailCloudinaryPublicID string `json:"thumbnailCloudinaryPublicId"`
	ExitCode   int         `json:"exitCode"`
	Timeout    int         `json:"timeout"`
	Overwrite  bool        `json:"overwrite"`
	Done       bool        `json:"done"`
	CreatedAt  string      `json:"createdAt"`
	UpdatedAt  string      `json:"updatedAt"`
	FinishedAt interface{} `json:"finishedAt"`
	Metadata   *YouTubeMetadata `json:"metadata"`
	Logs       []string    `json:"logs"`
}

type YouTubeMetadata struct {
	Title       string `json:"title"`
	ID          string `json:"id"`
	Channel     string `json:"channel"`
	Uploader    string `json:"uploader"`
	Description string `json:"description"`
	UploadDate  string `json:"upload_date"`
	Duration    int    `json:"duration"`
	Thumbnail   string `json:"thumbnail"`
}

type DownloadRequest struct {
	URL       string `json:"url"`
	Format    string `json:"format"`
	Output    string `json:"output"`
	Timeout   int    `json:"timeout"`
	Overwrite bool   `json:"overwrite"`
}

type JobStore struct {
	mu   sync.RWMutex
	jobs map[string]*DownloadJob
}

func newJobStore() *JobStore {
	return &JobStore{
		jobs: make(map[string]*DownloadJob),
	}
}

func (s *JobStore) add(job *DownloadJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *JobStore) get(id string) *DownloadJob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobs[id]
}

func (j *DownloadJob) log(format string, v ...any) {
	prefix := time.Now().Format("2006-01-02 15:04:05")
	line := fmt.Sprintf("[%s] %s", prefix, fmt.Sprintf(format, v...))

	j.mu.Lock()
	defer j.mu.Unlock()
	j.UpdatedAt = time.Now()
	j.Logs = append(j.Logs, line)
	if len(j.Logs) > jobMaxLogs {
		j.Logs = j.Logs[len(j.Logs)-jobMaxLogs:]
	}
	j.logLocal(line)
}

func (j *DownloadJob) logLocal(line string) {
	log.Printf("[%s] %s", j.ID, line)
}

func (j *DownloadJob) setStatus(status JobStatus) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Status = status
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) setMetadata(metadata *YouTubeMetadata) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Metadata = metadata
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) setOutputPath(output string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Output = output
	j.OutputPath = output
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) setImagePath(path string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.ImagePath = path
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) setAudioCloudinary(url, publicID string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.AudioCloudinaryURL = url
	j.AudioCloudinaryPublicID = publicID
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) setThumbnailCloudinary(url, publicID string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.ThumbnailCloudinaryURL = url
	j.ThumbnailCloudinaryPublicID = publicID
	j.UpdatedAt = time.Now()
}

func (j *DownloadJob) snapshot() *jobStatusResponse {
	j.mu.Lock()
	defer j.mu.Unlock()

	resp := &jobStatusResponse{
		ID:         j.ID,
		URL:        j.URL,
		Format:     j.Format,
		Status:     j.Status,
		Error:      j.Error,
		OutputPath: j.OutputPath,
		ImagePath:  j.ImagePath,
		AudioCloudinaryURL: j.AudioCloudinaryURL,
		AudioCloudinaryPublicID: j.AudioCloudinaryPublicID,
		ThumbnailCloudinaryURL: j.ThumbnailCloudinaryURL,
		ThumbnailCloudinaryPublicID: j.ThumbnailCloudinaryPublicID,
		ExitCode:   j.ExitCode,
		Timeout:    j.Timeout,
		Overwrite:  j.Overwrite,
		Done:       j.done,
		CreatedAt:  j.CreatedAt.Format(time.RFC3339),
		UpdatedAt:  j.UpdatedAt.Format(time.RFC3339),
		Metadata:   j.Metadata,
		Logs:       append([]string{}, j.Logs...),
	}

	if j.FinishedAt != nil {
		resp.FinishedAt = j.FinishedAt.Format(time.RFC3339)
	}

	return resp
}

func main() {
	loadEnvFile(envFilePath)

	rand.Seed(time.Now().UnixNano())

	var (
		urlArg      = flag.String("url", "", "YouTube URL")
		formatArg   = flag.String("format", "mp3", "異쒕젰 ?щ㎎ (mp3|mp4)")
		outputArg   = flag.String("output", "", "異쒕젰 ?뚯씪 寃쎈줈")
		timeoutArg  = flag.Int("timeout", defaultTimeout, "??꾩븘??遺?")
		overwrite   = flag.Bool("overwrite", false, "湲곗〈 ?뚯씪 ??뼱?곌린")
		port        = flag.Int("port", defaultPort, "??UI ?ы듃")
	)
	flag.Parse()

	if strings.TrimSpace(*urlArg) != "" {
		if err := runCLI(*urlArg, *formatArg, *outputArg, *timeoutArg, *overwrite); err != nil {
			log.Printf("CLI failed: %v", err)
			os.Exit(1)
		}
		return
	}

	if *port <= 0 {
		log.Fatalf("?섎せ???ы듃: %d", *port)
	}
	runWeb(*port)
}

func loadEnvFile(path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}

	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if len(val) >= 2 && strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
			val = strings.Trim(val, "\"")
		}
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, val); err == nil {
			continue
		}
		_ = err
	}
}

func getCloudinaryConfig() (CloudinaryConfig, bool) {
	cfg := CloudinaryConfig{
		CloudName: os.Getenv(cloudinaryCloudNameEnv),
		APIKey:    os.Getenv(cloudinaryAPIKeyEnv),
		APISecret: os.Getenv(cloudinaryAPISecretEnv),
		UploadFolder: os.Getenv(cloudinaryFolderEnv),
	}
	if strings.TrimSpace(cfg.CloudName) == "" || strings.TrimSpace(cfg.APIKey) == "" || strings.TrimSpace(cfg.APISecret) == "" {
		return cfg, false
	}
	return cfg, true
}

func cloudinaryResourceType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff":
		return "image"
	default:
		return "video"
	}
}

func sanitizeContextValue(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(v), "|", "-"), "=", "-"), "\n", " ")
}

func sanitizeCloudinaryFilename(v string) string {
	base := strings.TrimSpace(v)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	base = sanitizeFileName(base)
	base = strings.TrimSpace(strings.Trim(base, ".-"))
	if base == "" {
		base = fmt.Sprintf("upload-%d", time.Now().UnixNano())
	}
	return base
}

func buildCloudinaryContext(filename string) string {
	filename = sanitizeCloudinaryFilename(filename)
	return fmt.Sprintf("caption=%s|title=%s", filename, filename)
}

func cloudinarySignature(fields map[string]string, secret string) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		value := fields[key]
		if strings.TrimSpace(value) == "" {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, value))
	}
	raw := strings.Join(pairs, "&") + secret
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func uploadToCloudinary(filePath, caption string, tags string) (string, string, error) {
	cfg, ok := getCloudinaryConfig()
	if !ok {
		return "", "", fmt.Errorf("cloudinary env missing")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return "", "", fmt.Errorf("cloudinary open file: %w", err)
	}
	defer f.Close()

	resourceType := cloudinaryResourceType(filePath)
	publicID := sanitizeCloudinaryFilename(filepath.Base(filePath))
	if strings.TrimSpace(publicID) == "" {
		publicID = fmt.Sprintf("upload-%d", time.Now().UnixNano())
	}

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	contextCaption := sanitizeContextValue(sanitizeCloudinaryFilename(caption))
	contextValue := fmt.Sprintf("caption=%s|title=%s", contextCaption, contextCaption)
	params := map[string]string{
		"timestamp": timestamp,
		"folder":    cfg.UploadFolder,
		"public_id": publicID,
		"tags":      tags,
		"context":   contextValue,
	}

	signature := cloudinarySignature(params, cfg.APISecret)
	endpoint := fmt.Sprintf("https://api.cloudinary.com/v1_1/%s/%s/upload", cfg.CloudName, resourceType)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("api_key", cfg.APIKey); err != nil {
		return "", "", fmt.Errorf("cloudinary form build: %w", err)
	}
	if err := writer.WriteField("timestamp", timestamp); err != nil {
		return "", "", fmt.Errorf("cloudinary form build: %w", err)
	}
	if err := writer.WriteField("signature", signature); err != nil {
		return "", "", fmt.Errorf("cloudinary form build: %w", err)
	}
	if strings.TrimSpace(cfg.UploadFolder) != "" {
		if err := writer.WriteField("folder", cfg.UploadFolder); err != nil {
			return "", "", fmt.Errorf("cloudinary form build: %w", err)
		}
	}
	if strings.TrimSpace(tags) != "" {
		if err := writer.WriteField("tags", tags); err != nil {
			return "", "", fmt.Errorf("cloudinary form build: %w", err)
		}
	}
	if strings.TrimSpace(contextValue) != "" {
		if err := writer.WriteField("context", contextValue); err != nil {
			return "", "", fmt.Errorf("cloudinary form build: %w", err)
		}
	}
	if err := writer.WriteField("public_id", publicID); err != nil {
		return "", "", fmt.Errorf("cloudinary form build: %w", err)
	}

	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return "", "", fmt.Errorf("cloudinary form file: %w", err)
	}
	if _, err := io.Copy(part, f); err != nil {
		return "", "", fmt.Errorf("cloudinary copy file: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", "", fmt.Errorf("cloudinary writer close: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return "", "", fmt.Errorf("cloudinary request build: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("cloudinary upload request: %w", err)
	}
	defer res.Body.Close()

	responseBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", fmt.Errorf("cloudinary response read: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", "", fmt.Errorf("cloudinary upload failed (%d): %s", res.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var uploadResult CloudinaryUploadResult
	if err := json.Unmarshal(responseBody, &uploadResult); err != nil {
		return "", "", fmt.Errorf("cloudinary response parse: %w", err)
	}
	if uploadResult.Error != nil {
		msg := strings.TrimSpace(uploadResult.Error.Message)
		if msg == "" {
			msg = "cloudinary returned error"
		}
		return "", "", fmt.Errorf(msg)
	}

	url := uploadResult.SecureURL
	if strings.TrimSpace(url) == "" {
		url = uploadResult.URL
	}
	return url, uploadResult.PublicID, nil
}

func runCLI(url, formatArg, outputArg string, timeoutMin int, overwrite bool) error {
	format := strings.ToLower(strings.TrimSpace(formatArg))
	if err := validateFormat(format); err != nil {
		return err
	}
	if timeoutMin <= 0 {
		timeoutMin = defaultTimeout
	}
	if err := checkDeps(format); err != nil {
		return err
	}

	outputProvided := strings.TrimSpace(outputArg) != ""
	output := normalizeOutputPath(outputArg, format)

	job := &DownloadJob{
		ID:           "cli",
		URL:          url,
		Format:       format,
		Output:       output,
		OutputByUser: outputProvided,
		Timeout:      timeoutMin,
		Overwrite:    overwrite,
		Status:       statusRunning,
		Logs:         make([]string, 0, 64),
	}

	job.log("CLI mode enabled")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMin)*time.Minute)
	defer cancel()

	metadata, metaErr := fetchYouTubeMetadata(ctx, url)
	if metaErr != nil {
		job.log("YouTube metadata fetch failed: %v", metaErr)
	} else {
		job.setMetadata(metadata)
		job.log("硫뷀??곗씠?? title=%s channel=%s thumbnail=%s", metadata.Title, metadata.Channel, metadata.Thumbnail)
		if !job.OutputByUser {
			titleOutput := normalizeOutputPath(sanitizeFileName(metadata.Title), format)
			if strings.TrimSpace(titleOutput) != "" {
				output = titleOutput
				job.setOutputPath(output)
			}
		}

		if strings.TrimSpace(metadata.Thumbnail) != "" {
			thumbBase := sanitizeFileName(metadata.Title)
			if strings.TrimSpace(thumbBase) == "" {
				thumbBase = strings.TrimSuffix(filepath.Base(output), filepath.Ext(output))
			}
			imagePath, thumbErr := downloadThumbnail(ctx, metadata.Thumbnail, thumbBase)
			if thumbErr != nil {
				job.log("thumbnail download failed: %v", thumbErr)
			} else {
				job.setImagePath(imagePath)
				job.log("thumbnail saved: %s", imagePath)
			}
		}
	}

	if strings.TrimSpace(output) == "" {
		output = "output." + format
		job.setOutputPath(output)
	}

	dir := filepath.Dir(output)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("異쒕젰 ?붾젆?곕━ ?앹꽦 ?ㅽ뙣: %w", err)
		}
	}
	if !overwrite {
		if _, err := os.Stat(output); err == nil {
			job.Error = fmt.Sprintf("異쒕젰 ?뚯씪???대? 議댁옱?⑸땲?? --overwrite ?ъ슜 ?먮뒗 ?ㅻⅨ 寃쎈줈 吏?? %s", output)
			return errors.New(job.Error)
		}
	}

	err := downloadWithLogs(ctx, job, url, output, format)
	job.setStatus(func() JobStatus {
		if err == nil {
			return statusDone
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return statusTimeout
		}
		return statusFailed
	}())
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			job.Error = "?묒뾽???쒗븳 ?쒓컙??珥덇낵?덉뒿?덈떎."
			return errors.New(job.Error)
		}
		job.Error = err.Error()
		return err
	}

	audioCaption := filepath.Base(output)
	job.log("Cloudinary 음원 업로드 시작 (tags=EDM, caption=%s)", audioCaption)
	audioURL, audioPublicID, uploadErr := uploadToCloudinary(output, audioCaption, "EDM")
	if uploadErr != nil {
		job.Error = fmt.Sprintf("cloudinary audio upload failed: %v", uploadErr)
		return errors.New(job.Error)
	}
	job.setAudioCloudinary(audioURL, audioPublicID)
	job.log("Cloudinary 음원 업로드 완료: %s (%s)", audioURL, audioPublicID)

	if strings.TrimSpace(job.ImagePath) != "" {
		thumbCaption := filepath.Base(job.ImagePath)
		job.log("Cloudinary 썸네일 업로드 시작 (tags=EDM Cover, caption=%s)", thumbCaption)
		thumbURL, thumbPublicID, thumbErr := uploadToCloudinary(job.ImagePath, thumbCaption, "EDM Cover")
		if thumbErr != nil {
			job.log("cloudinary thumbnail upload failed: %v", thumbErr)
		} else {
			job.setThumbnailCloudinary(thumbURL, thumbPublicID)
			job.log("Cloudinary 썸네일 업로드 완료: %s (%s)", thumbURL, thumbPublicID)
		}
	}

	now := time.Now()
	job.FinishedAt = &now
	job.OutputPath = output
	log.Printf("?꾨즺: %s", output)
	return nil
}

func runWeb(port int) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	store := newJobStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		renderIndex(w, r)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		handleCreateJob(store, w, r)
	})
	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		handleJobStatus(store, w, r)
	})
	mux.HandleFunc("/api/files/", func(w http.ResponseWriter, r *http.Request) {
		handleDownloadFile(store, w, r)
	})
	mux.HandleFunc("/api/thumbnail/", func(w http.ResponseWriter, r *http.Request) {
		handleDownloadThumbnail(store, w, r)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("??UI ?ㅽ뻾 以? http://localhost:%d", port)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("?쒕쾭 ?ㅽ뻾 ?ㅽ뙣: %v", err)
	}
}

func handleCreateJob(store *JobStore, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DownloadRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	req.URL = strings.TrimSpace(req.URL)
	if req.URL == "" {
		writeJSONError(w, "url is required", http.StatusBadRequest)
		return
	}

	format := strings.ToLower(strings.TrimSpace(req.Format))
	if format == "" {
		format = "mp3"
	}
	if err := validateFormat(format); err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Timeout <= 0 {
		req.Timeout = defaultTimeout
	}

	if err := checkDeps(format); err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	requestedOutput := strings.TrimSpace(req.Output)
	outputByUser := requestedOutput != ""
	output := ""
	if outputByUser {
		output = sanitizeFileName(requestedOutput)
		output = normalizeOutputPath(output, format)
		if strings.TrimSpace(output) == "" {
			outputByUser = false
		}
	}
	if outputByUser {
		output = filepath.Join(downloadDir, output)
	}

	job := &DownloadJob{
		ID:           randomID(),
		URL:          req.URL,
		Format:       format,
		Output:       output,
		OutputByUser: outputByUser,
		Timeout:      req.Timeout,
		Overwrite:    req.Overwrite,
		Status:       statusQueued,
		Logs:         make([]string, 0, 64),
	}
	job.log("다운로드 요청")
	job.CreatedAt = time.Now()
	job.UpdatedAt = job.CreatedAt
	store.add(job)

	go runDownloadJob(job)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"id":  job.ID,
		"url": job.URL,
	})
}

func runDownloadJob(job *DownloadJob) {
	defer func() {
		job.UpdatedAt = time.Now()
		if r := recover(); r != nil {
			job.Error = fmt.Sprintf("internal panic: %v", r)
			job.setStatus(statusFailed)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.Timeout)*time.Minute)
	defer cancel()
	meta, metaErr := fetchYouTubeMetadata(ctx, job.URL)
	if metaErr != nil {
		job.log("메타데이터 조회 실패: %v", metaErr)
	} else {
		job.setMetadata(meta)
		job.log("메타데이터 조회 완료: %s / %s", meta.Title, meta.Channel)
		if !job.OutputByUser {
			titleOutput := normalizeOutputPath(sanitizeFileName(meta.Title), job.Format)
			if strings.TrimSpace(titleOutput) != "" {
				job.setOutputPath(filepath.Join(downloadDir, titleOutput))
			}
		}
		if strings.TrimSpace(meta.Thumbnail) != "" {
			thumbBase := sanitizeFileName(meta.Title)
			if strings.TrimSpace(thumbBase) == "" {
				thumbBase = strings.TrimSuffix(filepath.Base(job.Output), filepath.Ext(job.Output))
			}
			imagePath, thumbErr := downloadThumbnail(ctx, meta.Thumbnail, thumbBase)
			if thumbErr != nil {
				job.log("thumbnail download failed: %v", thumbErr)
			} else {
				job.setImagePath(imagePath)
				job.log("thumbnail saved: %s", imagePath)
			}
		}
	}

	if strings.TrimSpace(job.Output) == "" {
		fallback := normalizeOutputPath(fmt.Sprintf("youtube-%s", job.ID), job.Format)
		if strings.TrimSpace(fallback) == "" {
			fallback = "output." + job.Format
		}
		job.setOutputPath(filepath.Join(downloadDir, fallback))
	}

	job.setStatus(statusRunning)
	job.log("다운로드 시작")
	if err := os.MkdirAll(filepath.Dir(job.Output), 0o755); err != nil {
		job.Error = fmt.Sprintf("출력 경로 생성 실패: %v", err)
		job.log("에러: %s", job.Error)
		job.setStatus(statusFailed)
		job.done = true
		return
	}
	if !job.Overwrite {
		if _, err := os.Stat(job.Output); err == nil {
			job.Error = fmt.Sprintf("출력 파일이 이미 존재합니다: %s", job.Output)
			job.log("에러: %s", job.Error)
			job.setStatus(statusFailed)
			job.done = true
			return
		}
	}

	err := downloadWithLogs(ctx, job, job.URL, job.Output, job.Format)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			job.setStatus(statusTimeout)
			job.Error = "작업시간 초과"
		} else {
			job.setStatus(statusFailed)
			job.Error = err.Error()
		}
		job.log("다운로드 실패: %s", job.Error)
		job.ExitCode = 1
		job.done = true
		now := time.Now()
		job.FinishedAt = &now
		return
	}

	now := time.Now()
	job.FinishedAt = &now
	job.OutputPath = job.Output
	job.setStatus(statusDone)
	audioCaption := filepath.Base(job.Output)
	job.log("Cloudinary 음원 업로드 시작 (tags=EDM, caption=%s)", audioCaption)
	audioURL, audioPublicID, uploadErr := uploadToCloudinary(job.Output, audioCaption, "EDM")
	if uploadErr != nil {
		job.Error = fmt.Sprintf("cloudinary audio upload failed: %v", uploadErr)
		job.setStatus(statusFailed)
		job.log("cloudinary audio upload failed: %v", uploadErr)
		job.ExitCode = 1
		job.done = true
		return
	}
	job.setAudioCloudinary(audioURL, audioPublicID)
	job.log("Cloudinary 음원 업로드 완료: %s (%s)", audioURL, audioPublicID)

	if strings.TrimSpace(job.ImagePath) != "" {
		thumbCaption := filepath.Base(job.ImagePath)
		job.log("Cloudinary 썸네일 업로드 시작 (tags=EDM Cover, caption=%s)", thumbCaption)
		thumbURL, thumbPublicID, thumbErr := uploadToCloudinary(job.ImagePath, thumbCaption, "EDM Cover")
		if thumbErr != nil {
			job.log("cloudinary thumbnail upload failed: %v", thumbErr)
		} else {
			job.setThumbnailCloudinary(thumbURL, thumbPublicID)
			job.log("Cloudinary 썸네일 업로드 완료: %s (%s)", thumbURL, thumbPublicID)
		}
	}

	job.done = true
	job.log("다운로드 완료: %s", job.Output)
}

func fetchYouTubeMetadata(ctx context.Context, videoURL string) (*YouTubeMetadata, error) {
	cmd := exec.CommandContext(ctx, "yt-dlp", "--dump-json", "--no-playlist", videoURL)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("yt-dlp metadata command failed: %w", err)
	}

	var metadata YouTubeMetadata
	if err := json.Unmarshal(out, &metadata); err != nil {
		return nil, fmt.Errorf("metadata parse failed: %w", err)
	}
	return &metadata, nil
}

func downloadThumbnail(ctx context.Context, rawURL, fileBase string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", fmt.Errorf("thumbnail url is empty")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("thumbnail url parse failed: %w", err)
	}
	if strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("invalid thumbnail url: %s", rawURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", fmt.Errorf("thumbnail request build failed: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("thumbnail request failed: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("thumbnail request status: %s", res.Status)
	}

	ext := strings.ToLower(filepath.Ext(u.Path))
	if ext == "" || len(ext) > 5 {
		ext = ".jpg"
	}

	base := sanitizeFileName(fileBase)
	if strings.TrimSpace(base) == "" {
		base = fmt.Sprintf("youtube-%d", time.Now().UnixNano())
	}

	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		return "", fmt.Errorf("thumbnail output dir create failed: %w", err)
	}
	imagePath := filepath.Join(downloadDir, base+ext)

	f, err := os.Create(imagePath)
	if err != nil {
		return "", fmt.Errorf("thumbnail file create failed: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, res.Body); err != nil {
		return "", fmt.Errorf("thumbnail write failed: %w", err)
	}

	return imagePath, nil
}

func handleJobStatus(store *JobStore, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	id = strings.TrimSpace(id)
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	job := store.get(id)
	if job == nil {
		writeJSONError(w, "job not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, job.snapshot())
}

func handleDownloadFile(store *JobStore, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/files/")
	id = strings.TrimSpace(id)
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	job := store.get(id)
	if job == nil {
		writeJSONError(w, "job not found", http.StatusNotFound)
		return
	}
	if !job.done || job.Status != statusDone {
		writeJSONError(w, "job not finished", http.StatusConflict)
		return
	}
	if _, err := os.Stat(job.Output); err != nil {
		writeJSONError(w, "output file missing", http.StatusNotFound)
		return
	}

	fileName := filepath.Base(job.Output)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeFile(w, r, job.Output)
}

func handleDownloadThumbnail(store *JobStore, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/thumbnail/")
	id = strings.TrimSpace(id)
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	job := store.get(id)
	if job == nil {
		writeJSONError(w, "job not found", http.StatusNotFound)
		return
	}
	if strings.TrimSpace(job.ImagePath) == "" {
		writeJSONError(w, "thumbnail not available", http.StatusNotFound)
		return
	}
	if _, err := os.Stat(job.ImagePath); err != nil {
		writeJSONError(w, "thumbnail file missing", http.StatusNotFound)
		return
	}
	fileName := filepath.Base(job.ImagePath)
	contentType := "application/octet-stream"
	switch strings.ToLower(filepath.Ext(fileName)) {
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".png":
		contentType = "image/png"
	case ".webp":
		contentType = "image/webp"
	case ".gif":
		contentType = "image/gif"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", fileName))
	http.ServeFile(w, r, job.ImagePath)
}

func downloadWithLogs(ctx context.Context, job *DownloadJob, videoURL, outPath, format string) error {
	switch format {
	case "mp3":
		args := []string{
			"-x",
			"--audio-format", "mp3",
			"--audio-quality", "0",
			"--no-playlist",
			"-o", outPath,
			videoURL,
		}
		return runCommandWithLog(ctx, job, args)
	case "mp4":
		tempOut := strings.TrimSuffix(outPath, filepath.Ext(outPath)) + ".m4a"
		args := []string{
			"-x",
			"--audio-format", "m4a",
			"--audio-quality", "0",
			"--no-playlist",
			"-o", tempOut,
			videoURL,
		}
		if err := runCommandWithLog(ctx, job, args); err != nil {
			return err
		}
		job.log("?꾩떆 ?ㅻ뵒???뚯씪 ?앹꽦 ?꾨즺: %s", tempOut)
		if err := os.Rename(tempOut, outPath); err != nil {
			job.log("?꾩떆 ?뚯씪紐?蹂寃??ㅽ뙣: %v", err)
			return err
		}
		job.log("異쒕젰 ?뚯씪 ?대쫫 蹂寃? %s", outPath)
		job.OutputPath = outPath
		return nil
	default:
		return fmt.Errorf("吏?먮릺吏 ?딅뒗 ?щ㎎: %s", format)
	}
}

func runCommandWithLog(ctx context.Context, job *DownloadJob, args []string) error {
	job.log("yt-dlp ?ㅽ뻾: %s", strings.Join(append([]string{"yt-dlp"}, args...), " "))
	cmd := exec.CommandContext(ctx, "yt-dlp", args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe ?앹꽦 ?ㅽ뙣: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe ?앹꽦 ?ㅽ뙣: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("?꾨줈?몄뒪 ?쒖옉 ?ㅽ뙣: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	readOutput := func(reader io.ReadCloser, label string) {
		defer wg.Done()
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
		for scanner.Scan() {
			job.log("%s: %s", label, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			job.log("%s reader error: %v", label, err)
		}
	}
	go readOutput(stdoutPipe, "OUT")
	go readOutput(stderrPipe, "ERR")

	waitErr := cmd.Wait()
	wg.Wait()

	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			job.log("yt-dlp 醫낅즺 肄붾뱶: %d", exitErr.ExitCode())
			job.ExitCode = exitErr.ExitCode()
		}
		return fmt.Errorf("yt-dlp ?ㅽ뻾 ?ㅽ뙣: %w", waitErr)
	}
	return nil
}

func validateFormat(format string) error {
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "mp3" && format != "mp4" {
		return fmt.Errorf("format must be mp3 or mp4")
	}
	return nil
}

func checkDeps(format string) error {
	if _, err := exec.LookPath("yt-dlp"); err != nil {
		return errors.New("yt-dlp媛 ?꾩슂?⑸땲?? ?ㅼ튂 ??PATH??異붽??섏꽭??")
	}
	if format == "mp3" || format == "mp4" {
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			return errors.New("mp4/mp3 異붿텧?먮뒗 ffmpeg媛 ?꾩슂?⑸땲?? ?ㅼ튂 ??PATH??異붽??섏꽭??")
		}
	}
	return nil
}

func normalizeOutputPath(output, format string) string {
	clean := strings.TrimSpace(output)
	if clean == "" {
		return ""
	}
	ext := strings.ToLower(filepath.Ext(clean))
	if ext == ".mp3" || ext == ".mp4" {
		return clean
	}
	return clean + "." + strings.ToLower(format)
}

func randomID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

var sanitizeRe = regexp.MustCompile(`[\\/:*?"<>|]+`)

func sanitizeFileName(name string) string {
	clean := strings.TrimSpace(name)
	clean = sanitizeRe.ReplaceAllString(clean, "-")
	clean = strings.TrimSpace(strings.Trim(clean, ".-"))
	return clean
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	_ = enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, msg string, status int) {
	writeJSON(w, status, map[string]string{"error": msg})
}

const indexTemplate = `
<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>YouTube 다운로드 페이지</title>
  <style>
    :root {
      --bg: #eef2ff;
      --card: #ffffff;
      --line: #d8ddf0;
      --text: #20243a;
      --muted: #52607a;
      --accent: #3d63ff;
      --ok: #1c8c44;
      --danger: #b61f2a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top, #eef2ff 0%, #f4f7ff 45%, #f8fafc 100%);
      color: var(--text);
      font-family: "Noto Sans KR", "Apple SD Gothic Neo", Arial, sans-serif;
    }
    .wrap {
      max-width: 980px;
      margin: 36px auto;
      padding: 0 20px 40px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 20px;
      box-shadow: 0 18px 40px rgba(27, 41, 91, 0.08);
    }
    h1 { margin-top: 0; font-size: 30px; }
    label { display: block; font-weight: 700; margin: 12px 0 6px; }
    input, select, button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 15px;
    }
    .row { display: flex; gap: 12px; align-items: end; }
    .row > div { flex: 1; }
    .btn {
      background: var(--accent); color: #fff; border: none;
      font-weight: 700;
      cursor: pointer;
      padding: 11px 14px;
      border-radius: 10px;
    }
    .meta, .info, .actions, .log-wrap {
      margin-top: 14px;
      background: #f3f6ff;
      border: 1px dashed #b8c7ff;
      border-radius: 10px;
      padding: 12px;
    }
    .muted { color: var(--muted); font-size: 13px; }
    .status { font-weight: 700; }
    .logbox {
      margin-top: 12px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: #0b1020;
      color: #e8ecff;
      padding: 12px;
      height: 340px;
      overflow: auto;
      white-space: pre-wrap;
      font-size: 12px;
      line-height: 1.4;
    }
    .actions a {
      display: inline-block;
      margin-top: 10px;
      text-decoration: none;
      background: #f1f5ff;
      color: #0f1c4a;
      border: 1px solid #bdcbff;
      border-radius: 999px;
      padding: 8px 12px;
      margin-right: 8px;
      font-size: 13px;
      font-weight: 700;
    }
    .thumb {
      margin-top: 12px;
      display: none;
      text-align: center;
    }
    .thumb img {
      max-width: 100%;
      width: 360px;
      max-height: 220px;
      border-radius: 8px;
      border: 1px solid #cdd7ff;
      object-fit: cover;
      background: #dde2ff;
    }
    .field {
      font-size: 13px;
      margin: 6px 0;
      word-break: break-all;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>YouTube 다운로드</h1>
      <p class="muted">유튜브 링크를 넣으면 mp3 또는 mp4로 저장하고, 제목을 기반으로 파일/썸네일 이름이 생성됩니다.</p>
      <form id="download-form">
        <div>
          <label for="url">YouTube URL</label>
          <input id="url" name="url" required placeholder="https://www.youtube.com/watch?v=..." />
        </div>
        <div class="row">
          <div>
            <label for="format">포맷</label>
            <select id="format" name="format">
              <option value="mp3">mp3</option>
              <option value="mp4">mp4 (m4a audio extract)</option>
            </select>
          </div>
          <div>
            <label for="timeout">타임아웃(분)</label>
            <input id="timeout" name="timeout" type="number" value="30" min="1" />
          </div>
        </div>
        <div>
          <label for="output">출력 파일 이름(옵션)</label>
          <input id="output" name="output" placeholder="비워두면 제목 기반으로 자동 저장" />
        </div>
        <div class="row" style="align-items:center; margin-top: 10px;">
          <label style="flex:none; width:auto; display:inline-flex; align-items:center; gap:8px; margin-bottom:0;" for="overwrite">
            <input id="overwrite" name="overwrite" type="checkbox" />
            같은 이름 덮어쓰기
          </label>
          <button class="btn" type="submit">다운로드 시작</button>
        </div>
      </form>

      <div class="meta">
        <div class="status" id="status">대기 중</div>
        <div id="message" class="muted"></div>
      </div>

      <div class="info" id="meta-box" style="display:none;">
        <div class="field"><strong>영상 제목:</strong> <span id="meta-title">-</span></div>
        <div class="field"><strong>채널:</strong> <span id="meta-channel">-</span></div>
        <div class="field"><strong>출력 파일명:</strong> <span id="out-name">-</span></div>
        <div class="field"><strong>썸네일 파일명:</strong> <span id="thumb-name">-</span></div>
        <div class="field"><strong>Cloudinary 음원 URL:</strong> <span id="cloud-audio-url">-</span></div>
        <div class="field"><strong>Cloudinary 썸네일 URL:</strong> <span id="cloud-thumb-url">-</span></div>
      </div>

      <div class="actions" id="actions-box">
        <a id="download-link" href="#" style="display:none;">완료 파일 다운로드</a>
        <a id="thumbnail-link" href="#" style="display:none;">썸네일 다운로드</a>
      </div>

      <div class="thumb" id="thumbnail-box">
        <img id="thumbnail-preview" src="#" alt="썸네일" />
      </div>

      <div class="log-wrap">
        <div id="logbox" class="logbox"></div>
      </div>
    </div>
  </div>

  <script>
    const form = document.getElementById('download-form');
    const statusEl = document.getElementById('status');
    const messageEl = document.getElementById('message');
    const logbox = document.getElementById('logbox');
    const downloadLink = document.getElementById('download-link');
    const thumbLink = document.getElementById('thumbnail-link');
    const metaBox = document.getElementById('meta-box');
    const outNameEl = document.getElementById('out-name');
    const thumbNameEl = document.getElementById('thumb-name');
    const thumbImage = document.getElementById('thumbnail-preview');
    const thumbWrap = document.getElementById('thumbnail-box');
    const metaTitleEl = document.getElementById('meta-title');
    const metaChannelEl = document.getElementById('meta-channel');
    const cloudAudioEl = document.getElementById('cloud-audio-url');
    const cloudThumbEl = document.getElementById('cloud-thumb-url');

    const toName = (path) => {
      if (!path) return '-';
      const clean = String(path).trim();
      if (!clean) return '-';
      return clean.split('\\').pop().split('/').pop();
    };

    let timer = null;

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (timer) clearInterval(timer);
      downloadLink.style.display = 'none';
      thumbLink.style.display = 'none';
      thumbWrap.style.display = 'none';
      metaBox.style.display = 'none';
      logbox.textContent = '';
      messageEl.textContent = '';
      statusEl.textContent = '요청 등록 중...';
      metaTitleEl.textContent = '-';
      metaChannelEl.textContent = '-';
      outNameEl.textContent = '-';
      thumbNameEl.textContent = '-';
      cloudAudioEl.textContent = '-';
      cloudThumbEl.textContent = '-';

      const body = {
        url: document.getElementById('url').value,
        format: document.getElementById('format').value,
        timeout: Number(document.getElementById('timeout').value || 30),
        output: document.getElementById('output').value,
        overwrite: document.getElementById('overwrite').checked
      };

      const res = await fetch('/api/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const data = await res.json();
      if (!res.ok) {
        statusEl.textContent = '요청 실패';
        messageEl.textContent = data.error || '요청 생성 실패';
        return;
      }

      const jobId = data.id;
      statusEl.textContent = '다운로드 등록됨: ' + jobId;
      messageEl.textContent = '';
      pollStatus(jobId);
      timer = setInterval(() => pollStatus(jobId), 1000);
    });

    async function pollStatus(jobId) {
      try {
        const res = await fetch('/api/jobs/' + jobId);
        const data = await res.json();
        if (!res.ok) {
          statusEl.textContent = '상태 조회 실패';
          messageEl.textContent = data.error || '상태 조회 실패';
          clearInterval(timer);
          return;
        }

        statusEl.textContent = '상태: ' + data.status;
        logbox.textContent = (data.logs || []).join('\n');
        logbox.scrollTop = logbox.scrollHeight;
        metaBox.style.display = 'block';

        const metadata = data.metadata || {};
        metaTitleEl.textContent = metadata.title || '-';
        metaChannelEl.textContent = metadata.channel || metadata.uploader || '-';
        outNameEl.textContent = toName(data.outputPath);
        thumbNameEl.textContent = toName(data.imagePath);
        cloudAudioEl.textContent = data.audioCloudinaryUrl || '-';
        cloudThumbEl.textContent = data.thumbnailCloudinaryUrl || '-';

        if (data.imagePath) {
          thumbWrap.style.display = 'block';
          thumbImage.src = '/api/thumbnail/' + jobId + '?v=' + Date.now();
          thumbLink.href = '/api/thumbnail/' + jobId;
          thumbLink.style.display = 'inline-block';
          thumbLink.setAttribute('download', toName(data.imagePath));
        }

        if (data.done) {
          clearInterval(timer);
          if (data.status === 'done') {
            statusEl.textContent = '완료';
            messageEl.style.color = '#1c8c44';
            messageEl.textContent = '저장 완료: ' + (data.outputPath || '');
            downloadLink.href = '/api/files/' + data.id;
            downloadLink.style.display = 'inline-block';
            downloadLink.setAttribute('download', toName(data.outputPath));
          } else {
            statusEl.textContent = '실패';
            messageEl.style.color = '#b61f2a';
            messageEl.textContent = data.error || '실패';
          }
        }
      } catch (err) {
        statusEl.textContent = '오류';
        messageEl.style.color = '#b61f2a';
        messageEl.textContent = err.message || String(err);
        clearInterval(timer);
      }
    }
  </script>
</body>
</html>
`

func renderIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write([]byte(indexTemplate)); err != nil {
		log.Printf("index render write error: %v", err)
	}
}



