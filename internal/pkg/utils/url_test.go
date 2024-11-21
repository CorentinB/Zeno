package utils

import (
	"testing"

	"github.com/internetarchive/Zeno/pkg/models"
)

func TestURLToStringPunycode(t *testing.T) {
	u := &models.URL{Raw: "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia/pic/file/map_of_sarlat.pdf"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("error parsing URL: %v", err)
	}

	expected := "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia/pic/file/map_of_sarlat.pdf"
	actual := u.String()
	if actual != expected {
		t.Fatalf("expected %s, got %s", expected, actual)
	}
}

func TestURLToStringPunycodeWithPort(t *testing.T) {
	u := &models.URL{Raw: "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLToStringUnicodetoIDNA(t *testing.T) {
	u := &models.URL{Raw: "https://о-змладйвеклблнозеж.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLToStringWithPath(t *testing.T) {
	u := &models.URL{Raw: "http://παράδειγμα.δοκιμή/Αρχική_σελίδα"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "http://xn--hxajbheg2az3al.xn--jxalpdlp/%CE%91%CF%81%CF%87%CE%B9%CE%BA%CE%AE_%CF%83%CE%B5%CE%BB%CE%AF%CE%B4%CE%B1"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLToStringUnicodetoIDNAWithPort(t *testing.T) {
	u := &models.URL{Raw: "https://о-змладйвеклблнозеж.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://xn----8sbddjhbicfsohgbg1aeo.xn--p1ia:8080/pic/file/map_of_sarlat.pdf"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLwithIPv6(t *testing.T) {
	u := &models.URL{Raw: "https://[2600:4040:23c7:a620:3642:ebaa:ab23:735e]/test"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://[2600:4040:23c7:a620:3642:ebaa:ab23:735e]/test"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLwithIPv6WithPort(t *testing.T) {
	u := &models.URL{Raw: "https://[2600:4040:23c7:a620:3642:ebaa:ab23:735e]:8080/test"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://[2600:4040:23c7:a620:3642:ebaa:ab23:735e]:8080/test"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestURLwithSpacesandUnicode(t *testing.T) {
	u := &models.URL{Raw: "https://www.youtube.com/watch/0HBwC_wIFF4?t=18363石神視点【Minecraft】平日もど真ん中なんだから早く寝なきゃ【石神のぞみ／にじさんじ所属】https://www.youtube.com/watch/L30uAR9X8Uw?t=10100【倉持エン足中"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://www.youtube.com/watch/0HBwC_wIFF4?t=18363%E7%9F%B3%E7%A5%9E%E8%A6%96%E7%82%B9%E3%80%90Minecraft%E3%80%91%E5%B9%B3%E6%97%A5%E3%82%82%E3%81%A9%E7%9C%9F%E3%82%93%E4%B8%AD%E3%81%AA%E3%82%93%E3%81%A0%E3%81%8B%E3%82%89%E6%97%A9%E3%81%8F%E5%AF%9D%E3%81%AA%E3%81%8D%E3%82%83%E3%80%90%E7%9F%B3%E7%A5%9E%E3%81%AE%E3%81%9E%E3%81%BF%EF%BC%8F%E3%81%AB%E3%81%98%E3%81%95%E3%82%93%E3%81%98%E6%89%80%E5%B1%9E%E3%80%91https%3A%2F%2Fwww.youtube.com%2Fwatch%2FL30uAR9X8Uw%3Ft%3D10100%E3%80%90%E5%80%89%E6%8C%81%E3%82%A8%E3%83%B3%E8%B6%B3%E4%B8%AD"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

// For technical reasons we are not encoding Reddit URLs.
func TestURLwithRedditOverride(t *testing.T) {
	u := &models.URL{Raw: "https://styles.redditmedia.com/t5_7wkhw/styles/profileIcon_8w6r6fr3rh2d1.jpeg?width=64&height=64&frame=1&auto=webp&crop=64:64,smart&s=6d8ab9b89c9b846c9eb65622db9ced4992dc0905"}
	err := u.Parse()
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	expected := "https://styles.redditmedia.com/t5_7wkhw/styles/profileIcon_8w6r6fr3rh2d1.jpeg?width=64&height=64&frame=1&auto=webp&crop=64:64,smart&s=6d8ab9b89c9b846c9eb65622db9ced4992dc0905"
	actual := u.String()
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}
