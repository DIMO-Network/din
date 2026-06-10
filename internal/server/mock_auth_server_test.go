package server

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
)

type mockAuthServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	keyID      string
	issuerURL  string
}

func setupMockAuthServer(t *testing.T) *mockAuthServer {
	t.Helper()

	// Generate RSA key
	sk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	// Generate key ID
	keyID := fmt.Sprintf("key-%d", time.Now().Unix())

	auth := &mockAuthServer{
		privateKey: sk,
		publicKey:  &sk.PublicKey,
		keyID:      keyID,
	}

	// Create test server with JWKS endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/keys" {
			http.NotFound(w, r)
			return
		}

		// Create JWK
		jwk := map[string]any{
			"kty": "RSA",
			"kid": keyID,
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(sk.N.Bytes()),
			"e":   "AQAB",
		}

		jwks := map[string]any{
			"keys": []map[string]any{jwk},
		}

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(jwks)
		if err != nil {
			http.Error(w, "Failed to encode JWKS", http.StatusInternalServerError)
		}
	}))

	auth.server = server
	auth.issuerURL = server.URL
	return auth
}

func (m *mockAuthServer) createToken(t *testing.T, ethereumAddress common.Address, customClaims map[string]any) string {
	t.Helper()

	now := time.Now()

	// Create JWT claims
	claims := jwt.MapClaims{
		"iss":              m.issuerURL,
		"sub":              ethereumAddress.Hex(),
		"aud":              []string{"dimo.zone"},
		"exp":              now.Add(time.Hour).Unix(),
		"iat":              now.Unix(),
		"ethereum_address": ethereumAddress.Hex(),
	}

	// Add custom claims
	for k, v := range customClaims {
		claims[k] = v
	}

	// Create token
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = m.keyID

	// Sign the token
	tokenString, err := token.SignedString(m.privateKey)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}

	return tokenString
}

func (m *mockAuthServer) URL() string {
	return m.server.URL
}

func (m *mockAuthServer) Close() {
	m.server.Close()
}
