package libsyn

import (
	"net/url"
	"strings"
)

// Goal is to turn https://traffic.libsyn.com/democratieparticipative/DPS09E16.mp3
// into https://traffic.libsyn.com/secure/force-cdn/highwinds/democratieparticipative/DPS09E16.mp3
// So it's basically adding /secure/force-cdn/highwinds/ after the domain.
func IsLibsynURL(URL string) bool {
	return strings.Contains(URL, "traffic.libsyn.com") && strings.HasSuffix(URL, ".mp3") && !strings.Contains(URL, "force-cdn/highwinds")
}

func GenerateHighwindsURL(URL string) (*url.URL, error) {
	highwindURL, err := url.Parse(strings.Replace(URL, "traffic.libsyn.com", "traffic.libsyn.com/secure/force-cdn/highwinds", 1))
	if err != nil {
		return nil, err
	}

	return highwindURL, nil
}
