package vk

import (
	"net/http"
	"strings"
)

func IsVKURL(URL string) bool {
	return strings.Contains(URL, "/vk.com")
}

func AddHeaders(req *http.Request) {
	req.Header.Set("Authority", "vk.com")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
	req.Header.Set("Accept-Language", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Sec-Ch-Ua", "\"Google Chrome\";v=\"117\", \"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"117\"")
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", "\"Linux\"")
	req.Header.Set("Cookie", "remixlang=16; remixstlid=9081481303287407624_J8IBnAtA345Tsk1Xvvev3RnFSpI2MP530RSg18a8to8; remixstid=1946704529_fMgm2XWf0xKs0V3OA5gREkPaPnkZQCtM7jnY4ElxN8L; remixua=-1%7C-1%7C202%7C2116504820; remixlgck=338452558cbd8b71f0; remixnp=0; remixscreen_width=2294; remixscreen_height=960; remixscreen_dpr=1.5; remixscreen_depth=24; remixscreen_orient=1; remixdark_color_scheme=1; remixcolor_scheme_mode=auto; remixdt=-3600; remixgp=a18160be71eb9194e6ae85caf4fd157c; remixscreen_winzoom=1.82")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	// Gives "Your browser is out of date" error when using default UA.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
}
