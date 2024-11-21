package preprocessor

import "errors"

var (
	// ErrPreprocessorAlreadyInitialized is the error returned when the preprocessor is already initialized
	ErrPreprocessorAlreadyInitialized = errors.New("preprocessor already initialized")
	//ErrSchemeIsInvalid is the error returned when the scheme of a URL is not http or http
	ErrUnsupportedScheme = errors.New("URL scheme is unsupported")
)
