# YouTube 오디오 다운로더 (Go + 웹 UI)

YouTube 영상 URL을 입력받아 오디오만 추출하고 `mp4`(AAC 오디오 컨테이너)로 저장하는 도구입니다.  
기본 동작은 웹 UI이며, `-url`을 함께 주면 CLI로도 동작합니다.

## 1) 실행 환경

- `yt-dlp`
- `ffmpeg`
- Go 1.22+

## 2) 웹 UI 실행

```bash
go run .
```

기본 포트는 `8080`이며, 브라우저에서 아래로 접속합니다.

```
http://localhost:8080
```

포트를 바꾸려면:

```bash
go run . -port 3000
```

## 3) UI 사용법

- URL 입력: 유튜브 영상 링크
- Format: `mp4`
- Timeout: 분 단위 타임아웃
- Output: 출력 파일명(확장자 생략 시 자동 추가)
- Overwrite: 같은 파일명 덮어쓰기 허용 여부

실행하면 화면 하단 로그 박스에 `yt-dlp`의 진행 로그가 상세하게 실시간 누적됩니다.  
완료 시 `완료된 파일 받기` 링크가 표시됩니다.
리소스 추출은 끝났지만 Cloudinary 업로드가 실패한 경우, 같은 작업 화면에서 `Cloudinary 업로드 재시도` 버튼으로 저장된 리소스만 다시 업로드할 수 있습니다.

## 4) API 동작

- `POST /api/download`
  - 입력 예시:
  ```json
  {
    "url": "https://www.youtube.com/watch?v=...",
    "format": "mp4",
    "timeout": 30,
    "output": "my_song",
    "overwrite": false
  }
  ```
  - 응답 예시: `{"id":"...","url":"..."}`

- `GET /api/jobs/{id}`
  - 작업 상태/로그 반환

- `GET /api/files/{id}`
  - 완료된 파일 다운로드

## 5) CLI 모드

기존 CLI도 지원됩니다.

```bash
go run . -url "https://www.youtube.com/watch?v=VIDEO_ID" -format mp4 -timeout 30 -output music.mp4
```

`-url`이 없으면 웹 UI가 자동 실행됩니다.
