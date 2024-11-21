package ytdlp

type Subtitle struct {
	Ext  string `json:"ext"`
	URL  string `json:"url"`
	Name string `json:"name"`
}

type Video struct {
	ID               string                `json:"id"`
	Title            string                `json:"title"`
	Channel          string                `json:"channel"`
	ChannelID        string                `json:"channel_id"`
	ChannelURL       string                `json:"channel_url"`
	Description      string                `json:"description"`
	Timestamp        int                   `json:"timestamp"`
	Duration         float64               `json:"duration"`
	ViewCount        float64               `json:"view_count"`
	Tags             []string              `json:"tags"`
	Categories       []string              `json:"categories"`
	Thumbnail        string                `json:"thumbnail"`
	Language         string                `json:"language"`
	IsLive           bool                  `json:"is_live"`
	Subtitles        map[string][]Subtitle `json:"subtitles"`
	RequestedFormats []struct {
		Acodec        string      `json:"acodec"`
		AspectRatio   float64     `json:"aspect_ratio"`
		Asr           interface{} `json:"asr"`
		AudioChannels interface{} `json:"audio_channels"`
		AudioExt      string      `json:"audio_ext"`
		Container     string      `json:"container"`
		DynamicRange  string      `json:"dynamic_range"`
		Ext           string      `json:"ext"`
		Filesize      float64     `json:"filesize"`
		Format        string      `json:"format"`
		FormatID      string      `json:"format_id"`
		FormatNote    string      `json:"format_note"`
		Fps           float64     `json:"fps"`
		Fragments     []struct {
			URL string `json:"url"`
		} `json:"fragments"`
		HasDrm             bool              `json:"has_drm"`
		Height             float64           `json:"height"`
		HTTPHeaders        map[string]string `json:"http_headers"`
		Language           interface{}       `json:"language"`
		LanguagePreference float64           `json:"language_preference"`
		Preference         interface{}       `json:"preference"`
		Protocol           string            `json:"protocol"`
		Quality            float64           `json:"quality"`
		Resolution         string            `json:"resolution"`
		SourcePreference   float64           `json:"source_preference"`
		Tbr                float64           `json:"tbr"`
		URL                string            `json:"url"`
		Vbr                float64           `json:"vbr,omitempty"`
		Vcodec             string            `json:"vcodec"`
		VideoExt           string            `json:"video_ext"`
		Width              float64           `json:"width"`
		Abr                float64           `json:"abr,omitempty"`
	} `json:"requested_formats"`
	Formats []struct {
		Acodec      string  `json:"acodec"`
		AspectRatio float64 `json:"aspect_ratio"`
		AudioExt    string  `json:"audio_ext"`
		Columns     float64 `json:"columns,omitempty"`
		Ext         string  `json:"ext"`
		Format      string  `json:"format"`
		FormatID    string  `json:"format_id"`
		FormatNote  string  `json:"format_note"`
		Fps         float64 `json:"fps"`
		Fragments   []struct {
			Duration float64 `json:"duration"`
			URL      string  `json:"url"`
		} `json:"fragments,omitempty"`
		Height      float64 `json:"height"`
		HTTPHeaders struct {
			Accept         string `json:"Accept"`
			AcceptLanguage string `json:"Accept-Language"`
			SecFetchMode   string `json:"Sec-Fetch-Mode"`
			UserAgent      string `json:"User-Agent"`
		} `json:"http_headers"`
		Protocol           string      `json:"protocol"`
		Resolution         string      `json:"resolution"`
		Rows               float64     `json:"rows,omitempty"`
		URL                string      `json:"url"`
		Vcodec             string      `json:"vcodec"`
		VideoExt           string      `json:"video_ext"`
		Width              float64     `json:"width"`
		Abr                float64     `json:"abr,omitempty"`
		Asr                float64     `json:"asr,omitempty"`
		AudioChannels      float64     `json:"audio_channels,omitempty"`
		Container          string      `json:"container,omitempty"`
		DynamicRange       interface{} `json:"dynamic_range,omitempty"`
		Filesize           float64     `json:"filesize,omitempty"`
		HasDrm             bool        `json:"has_drm,omitempty"`
		Language           string      `json:"language,omitempty"`
		LanguagePreference float64     `json:"language_preference,omitempty"`
		Preference         interface{} `json:"preference,omitempty"`
		Quality            float64     `json:"quality,omitempty"`
		SourcePreference   float64     `json:"source_preference,omitempty"`
		Tbr                float64     `json:"tbr,omitempty"`
		Vbr                float64     `json:"vbr,omitempty"`
		FilesizeApprox     float64     `json:"filesize_approx,omitempty"`
	} `json:"formats"`
	Thumbnails []struct {
		URL string `json:"url"`
	} `json:"thumbnails"`
}

type HTTPHeaders struct {
	Accept         string `json:"Accept"`
	AcceptLanguage string `json:"Accept-Language"`
	SecFetchMode   string `json:"Sec-Fetch-Mode"`
	UserAgent      string `json:"User-Agent"`
}
