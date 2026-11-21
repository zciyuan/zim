package upload

type tokenReq struct {
	FileType string `json:"file_type"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
	Mime     string `json:"mime"`
}

type tokenRes struct {
	ObjectKey   string `json:"object_key"`
	UploadURL   string `json:"upload_url"`
	Method      string `json:"method"`
	ExpiresIn   int64  `json:"expires_in"`
	DownloadURL string `json:"download_url"`
}

type completeReq struct {
	ObjectKey string `json:"object_key"`
}

type putRes struct {
	OK        bool   `json:"ok"`
	ObjectKey string `json:"object_key"`
	Size      int64  `json:"size,omitempty"`
}

type deleteReq struct {
	Key string `json:"key"`
}
