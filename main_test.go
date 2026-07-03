package main

import (
	"context"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateFormatAllowsOnlyMP4(t *testing.T) {
	if err := validateFormat("mp4"); err != nil {
		t.Fatalf("validateFormat(mp4) returned error: %v", err)
	}

	for _, format := range []string{"mp3", "wav", ""} {
		t.Run(format, func(t *testing.T) {
			if err := validateFormat(format); err == nil {
				t.Fatalf("validateFormat(%q) returned nil, want error", format)
			}
		})
	}
}

func TestUploadJobResourcesUploadsSavedAudioAndThumbnail(t *testing.T) {
	dir := t.TempDir()
	audioPath := filepath.Join(dir, "song.mp4")
	thumbPath := filepath.Join(dir, "thumb.png")
	if err := os.WriteFile(audioPath, []byte("audio"), 0o644); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := os.WriteFile(thumbPath, []byte("thumb"), 0o644); err != nil {
		t.Fatalf("write thumbnail: %v", err)
	}

	job := &DownloadJob{Output: audioPath, ImagePath: thumbPath}
	var calls []struct {
		path    string
		caption string
		tags    string
	}
	upload := func(filePath, caption, tags string) (string, string, error) {
		calls = append(calls, struct {
			path    string
			caption string
			tags    string
		}{path: filePath, caption: caption, tags: tags})
		return "https://cloud.example/" + filepath.Base(filePath), "public-" + caption, nil
	}

	if err := uploadJobResources(context.Background(), job, upload); err != nil {
		t.Fatalf("uploadJobResources returned error: %v", err)
	}

	if len(calls) != 2 {
		t.Fatalf("upload calls = %d, want 2", len(calls))
	}
	if calls[0].path != audioPath || calls[0].caption != "song.mp4" || calls[0].tags != "EDM" {
		t.Fatalf("audio upload call = %+v", calls[0])
	}
	if calls[1].path != thumbPath || calls[1].caption != "thumb.png" || calls[1].tags != "EDM Cover" {
		t.Fatalf("thumbnail upload call = %+v", calls[1])
	}
	if job.AudioCloudinaryURL == "" || job.AudioCloudinaryPublicID == "" {
		t.Fatal("audio cloudinary fields were not set")
	}
	if job.ThumbnailCloudinaryURL == "" || job.ThumbnailCloudinaryPublicID == "" {
		t.Fatal("thumbnail cloudinary fields were not set")
	}
}

func TestUploadJobResourcesSkipsAlreadyUploadedResources(t *testing.T) {
	dir := t.TempDir()
	audioPath := filepath.Join(dir, "song.mp4")
	thumbPath := filepath.Join(dir, "thumb.png")
	if err := os.WriteFile(audioPath, []byte("audio"), 0o644); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := os.WriteFile(thumbPath, []byte("thumb"), 0o644); err != nil {
		t.Fatalf("write thumbnail: %v", err)
	}

	job := &DownloadJob{
		Output:                  audioPath,
		ImagePath:               thumbPath,
		AudioCloudinaryURL:      "https://cloud.example/song.mp4",
		AudioCloudinaryPublicID: "existing-song",
	}
	var calls []string
	upload := func(filePath, caption, tags string) (string, string, error) {
		calls = append(calls, tags)
		return "https://cloud.example/" + filepath.Base(filePath), "public-" + caption, nil
	}

	if err := uploadJobResources(context.Background(), job, upload); err != nil {
		t.Fatalf("uploadJobResources returned error: %v", err)
	}

	if len(calls) != 1 || calls[0] != "EDM Cover" {
		t.Fatalf("upload calls = %v, want only thumbnail upload", calls)
	}
}

func TestUploadJobResourcesRequiresSavedAudio(t *testing.T) {
	job := &DownloadJob{Output: filepath.Join(t.TempDir(), "missing.mp4")}
	upload := func(filePath, caption, tags string) (string, string, error) {
		t.Fatalf("upload called for missing resource: %s", filePath)
		return "", "", nil
	}

	if err := uploadJobResources(context.Background(), job, upload); err == nil {
		t.Fatal("uploadJobResources returned nil, want missing file error")
	}
}

func TestUseExistingOutputResourceAttachesSavedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.mp4")
	if err := os.WriteFile(path, []byte("audio"), 0o644); err != nil {
		t.Fatalf("write existing output: %v", err)
	}

	job := &DownloadJob{Output: path}
	reused, err := useExistingOutputResource(job, path, false)
	if err != nil {
		t.Fatalf("useExistingOutputResource returned error: %v", err)
	}
	if !reused {
		t.Fatal("useExistingOutputResource returned reused=false, want true")
	}
	if job.Output != path || job.OutputPath != path {
		t.Fatalf("job output fields = %q/%q, want %q", job.Output, job.OutputPath, path)
	}
}

func TestSanitizeCloudinaryFilenameRemovesInvalidPublicIDChars(t *testing.T) {
	input := "David Guetta vs Benny Benassi - Satisfaction (Hardwell & Maddix Remix) [Official Music Video].mp4"
	want := "David Guetta vs Benny Benassi - Satisfaction Hardwell Maddix Remix Official Music Video"

	got := sanitizeCloudinaryFilename(input)
	if got != want {
		t.Fatalf("sanitizeCloudinaryFilename() = %q, want %q", got, want)
	}
	if strings.ContainsAny(got, "&[]()") {
		t.Fatalf("sanitizeCloudinaryFilename() left invalid public_id chars in %q", got)
	}
}

func TestNormalizeOutputPathRemovesMP3Extension(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   string
	}{
		{name: "extensionless", output: "song", want: "song.mp4"},
		{name: "already mp4", output: "song.mp4", want: "song.mp4"},
		{name: "old mp3 extension", output: "song.mp3", want: "song.mp4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeOutputPath(tt.output, "mp4")
			if got != tt.want {
				t.Fatalf("normalizeOutputPath(%q, mp4) = %q, want %q", tt.output, got, tt.want)
			}
		})
	}
}

func TestIndexTemplateOffersOnlyMP4(t *testing.T) {
	if strings.Contains(indexTemplate, `value="mp3"`) {
		t.Fatal("index template still offers mp3")
	}
	if !strings.Contains(indexTemplate, `value="mp4"`) {
		t.Fatal("index template does not offer mp4")
	}
}

func TestIndexTemplatePlacesLogPanelBeforeForm(t *testing.T) {
	logIndex := strings.Index(indexTemplate, `id="logbox"`)
	formIndex := strings.Index(indexTemplate, `id="download-form"`)
	if logIndex == -1 {
		t.Fatal("index template does not contain logbox")
	}
	if formIndex == -1 {
		t.Fatal("index template does not contain download form")
	}
	if logIndex > formIndex {
		t.Fatalf("logbox appears after form: logIndex=%d formIndex=%d", logIndex, formIndex)
	}
}

func TestCropImageToSquareUsesCenterCropWithoutStretching(t *testing.T) {
	path := filepath.Join(t.TempDir(), "thumbnail.png")
	src := image.NewRGBA(image.Rect(0, 0, 6, 4))
	for y := 0; y < 4; y++ {
		for x := 0; x < 6; x++ {
			src.Set(x, y, color.RGBA{R: uint8(x), G: uint8(y), A: 255})
		}
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create test image: %v", err)
	}
	if err := png.Encode(f, src); err != nil {
		_ = f.Close()
		t.Fatalf("encode test image: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close test image: %v", err)
	}

	cropped, err := cropImageToSquare(path)
	if err != nil {
		t.Fatalf("cropImageToSquare returned error: %v", err)
	}
	if !cropped {
		t.Fatal("cropImageToSquare returned cropped=false for non-square image")
	}

	gotFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("open cropped image: %v", err)
	}
	defer gotFile.Close()

	got, _, err := image.Decode(gotFile)
	if err != nil {
		t.Fatalf("decode cropped image: %v", err)
	}

	bounds := got.Bounds()
	if bounds.Dx() != 4 || bounds.Dy() != 4 {
		t.Fatalf("cropped bounds = %dx%d, want 4x4", bounds.Dx(), bounds.Dy())
	}
	leftR, _, _, _ := got.At(bounds.Min.X, bounds.Min.Y).RGBA()
	rightR, _, _, _ := got.At(bounds.Min.X+3, bounds.Min.Y).RGBA()
	if leftR != 0x0101 || rightR != 0x0404 {
		t.Fatalf("crop did not use centered source pixels: left=%#x right=%#x", leftR, rightR)
	}
}
