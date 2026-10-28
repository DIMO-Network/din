package admit

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/DIMO-Network/did-directory/pkg/client"
	"github.com/DIMO-Network/did-directory/pkg/dpop"
	"github.com/DIMO-Network/did-directory/pkg/provenance"
	"github.com/DIMO-Network/did-directory/pkg/signin"
)

// Config is what din needs to take events from sources.
type Config struct {
	OrgHostURL   string
	DirectoryURL string
	DauthURL     string
	// DID is din's own DID, and KeyFile holds the P-256 key of its #signing
	// method. din signs in to dauth with it, for the org host, which admits
	// din because DID is one of its issuers.
	DID     string
	KeyFile string
}

// New builds an Admitter over the directory and org host in cfg.
func New(cfg Config) (*Admitter, error) {
	if cfg.OrgHostURL == "" || cfg.DirectoryURL == "" || cfg.DauthURL == "" || cfg.DID == "" || cfg.KeyFile == "" {
		return nil, errors.New("sources need an org host, directory and dauth URL, din's DID and its key file")
	}
	priv, err := LoadKey(cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	key, err := dpop.NewKey(priv)
	if err != nil {
		return nil, err
	}
	identity := &signin.Client{Request: signin.Request{
		Dauth: cfg.DauthURL, DID: cfg.DID, Key: priv,
		Audience: []string{strings.TrimRight(cfg.OrgHostURL, "/")}, Scope: "read",
	}}
	return &Admitter{
		Keys:       provenance.DIDKeys{Directory: client.New(cfg.DirectoryURL)},
		Authorizer: &OrgHost{BaseURL: cfg.OrgHostURL, Identity: identity.Token, Key: key},
	}, nil
}

// LoadKey reads a PEM P-256 private key, SEC 1 or PKCS #8.
func LoadKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading source key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%s holds no PEM block", path)
	}
	if k, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an ECDSA key", path)
	}
	return ec, nil
}
