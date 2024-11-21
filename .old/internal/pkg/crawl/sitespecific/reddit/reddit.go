package reddit

import (
	"net/http"
	"strings"
)

func IsURL(URL string) bool {
	return strings.Contains(URL, "reddit.com")
}

func AddCookies(req *http.Request) {
	newCookies := []http.Cookie{
		{
			Name:   "eu_cookie_v2",
			Value:  "3",
			Domain: ".reddit.com",
			Path:   "/",
		},
		{
			Name:   "over18",
			Value:  "1",
			Domain: ".reddit.com",
			Path:   "/",
		},
		{
			Name:   "_options",
			Value:  "%7B%22pref_quarantine_optin%22%3A%20true%2C%20%22pref_gated_sr_optin%22%3A%20true%7D",
			Domain: ".reddit.com",
			Path:   "/",
		},
	}

	existingCookies := req.Cookies()

	for _, newCookie := range newCookies {
		exists := false
		for _, existingCookie := range existingCookies {
			if existingCookie.Name == newCookie.Name &&
				existingCookie.Domain == newCookie.Domain &&
				existingCookie.Path == newCookie.Path {
				exists = true
				break
			}
		}
		if !exists {
			req.AddCookie(&newCookie)
		}
	}
}
