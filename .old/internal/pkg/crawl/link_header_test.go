package crawl

import (
	"slices"
	"testing"
)

func TestParseOneLink(t *testing.T) {
	var links []Link
	links = append(links, Link{URL: "https://one.example.com", Rel: "preconnect"})

	var link = `<https://one.example.com>; rel="preconnect"`

	got := Parse(link)
	want := links

	if !slices.Equal(got, want) {
		t.Fatalf("got %q, wanted %q", got, want)
	}
}

func TestParseMultipleLinks(t *testing.T) {
	var links []Link
	links = append(links,
		Link{URL: "https://test.com", Rel: "preconnect"},
		Link{URL: "https://app.test.com", Rel: "preconnect"},
		Link{URL: "https://example.com", Rel: "preconnect"},
	)

	var link = `<https://test.com>; rel="preconnect", <https://app.test.com>; rel="preconnect"; foo="bar", <https://example.com>; rel="preconnect"`

	got := Parse(link)
	want := links

	if !slices.Equal(got, want) {
		t.Fatalf("got %q, wanted %q", got, want)
	}
}

func TestParseOneMalformedLink(t *testing.T) {
	var links []Link
	links = append(links, Link{URL: "https://one.example.com", Rel: "preconnect"})

	var link = `https://one.example.com>;; rel=preconnect";`

	got := Parse(link)
	want := links

	if !slices.Equal(got, want) {
		t.Fatalf("got %q, wanted %q", got, want)
	}
}

func TestParseMultipleMalformedLinks(t *testing.T) {
	var links []Link
	links = append(links,
		Link{URL: "", Rel: "preconnect"},
		Link{URL: "https://app.test.com", Rel: ""},
		Link{URL: "", Rel: ""},
	)

	var link = `; rel="preconnect", https://app.test.com; rel=""; "bar", <>; ="preconnect"`

	got := Parse(link)
	want := links

	if !slices.Equal(got, want) {
		t.Fatalf("got %q, wanted %q", got, want)
	}
}

func TestParseAttr(t *testing.T) {
	attr := `rel="preconnect"`

	gotKey, gotValue := ParseAttr(attr)
	wantKey, wantValue := "rel", "preconnect"

	if gotKey != wantKey {
		t.Fatalf("got %q, wanted %q", gotKey, wantKey)
	}

	if gotValue != wantValue {
		t.Fatalf("got %q, wanted %q", gotValue, wantValue)
	}
}

func TestParseMalformedAttr(t *testing.T) {
	attr := `="preconnect"`

	gotKey, gotValue := ParseAttr(attr)
	wantKey, wantValue := "", "preconnect"

	if gotKey != wantKey {
		t.Fatalf("got %q, wanted %q", gotKey, wantKey)
	}

	if gotValue != wantValue {
		t.Fatalf("got %q, wanted %q", gotValue, wantValue)
	}
}
