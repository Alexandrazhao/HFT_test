package server

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed dist
var embeddedStatic embed.FS

func staticHandler() (http.Handler, error) {
	sub, err := fs.Sub(embeddedStatic, "dist")
	if err != nil {
		return nil, err
	}
	return http.FileServer(http.FS(sub)), nil
}
