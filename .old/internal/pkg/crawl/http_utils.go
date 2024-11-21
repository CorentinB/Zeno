package crawl

import (
	"io"
)

// ClosingPipedTeeReader is like a classic io.TeeReader, but it explicitely
// takes an io.PipeWriter, and make sure to close it
func ClosingPipedTeeReader(r io.Reader, pw *io.PipeWriter) io.Reader {
	return &closingPipedTeeReader{r, pw}
}

type closingPipedTeeReader struct {
	r  io.Reader
	pw *io.PipeWriter
}

func (t *closingPipedTeeReader) Read(p []byte) (n int, err error) {
	n, err = t.r.Read(p)
	if n > 0 {
		if n, err := t.pw.Write(p[:n]); err != nil {
			return n, err
		}
	}

	t.pw.Close()

	return
}
