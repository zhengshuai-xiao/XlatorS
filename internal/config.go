package internal

import "github.com/minio/minio-go/v7/pkg/credentials"

type Config struct {
	Xlator        string
	MultiBucket   bool
	KeepEtag      bool
	Umask         uint16
	ObjTag        bool
	ObjMeta       bool
	HeadDir       bool
	HideDir       bool
	ReadOnly      bool
	BackendAddr   string
	Creds         *credentials.Credentials
	MetaDriver    string
	MetaAddr      string
	DownloadCache string
	DSBackendType string
}

const (
	BackendType = iota
	// Filesystem backend.
	BackendFS
	// Multi disk BackendErasure (single, distributed) backend.
	BackendErasure
	// Gateway backend.
	BackendGateway
	// Add your own backend.
)
