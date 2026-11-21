package auth

type refreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

type registerReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
	Platform int32  `json:"platform"`
}

type tokenReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Platform int32  `json:"platform"`
}

type tokenRes struct {
	UID              string `json:"uid"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

type response struct {
	Code      int    `json:"code"`
	Message   string `json:"message,omitempty"`
	Data      any    `json:"data,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}
