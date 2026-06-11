package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAttestationAuth ports the dis attestationMiddleware table test
// against the JWT validation function.
func TestAttestationAuth(t *testing.T) {
	authServer := setupMockAuthServer(t)
	defer authServer.Close()

	auth, err := newAttestationAuth(authServer.issuerURL, authServer.issuerURL+"/keys")
	require.NoError(t, err)

	tests := []struct {
		name           string
		setupRequest   func() *http.Request
		expectedSource string
		expectedError  bool
		errorIs        error
		errorContains  string
	}{
		{
			name: "valid token with custom claims",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
				token := authServer.createToken(t, ethereumAddr, map[string]any{
					"email":       "test@example.com",
					"provider_id": "test-provider",
				})

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				return req
			},
			expectedSource: common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd").Hex(),
			expectedError:  false,
		},
		{
			name: "missing authorization header",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				return req
			},
			expectedError: true,
			errorContains: "Bearer scheme",
		},
		{
			name: "invalid bearer format",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "InvalidFormat token")
				return req
			},
			expectedError: true,
			errorContains: "Bearer scheme",
		},
		{
			name: "empty bearer token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer ")
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
		{
			name: "token without ethereum address",
			setupRequest: func() *http.Request {
				// Create token without ethereum_address claim
				now := time.Now()
				claims := jwt.MapClaims{
					"iss": authServer.issuerURL,
					"sub": "test-subject",
					"aud": []string{"dimo.zone"},
					"exp": now.Add(time.Hour).Unix(),
					"iat": now.Unix(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       ErrInvalidEthAddr,
		},
		{
			name: "token with zero ethereum address",
			setupRequest: func() *http.Request {
				zeroAddr := common.Address{}
				token := authServer.createToken(t, zeroAddr, nil)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				return req
			},
			expectedError: true,
			errorIs:       ErrInvalidEthAddr,
		},
		{
			name: "expired token",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				now := time.Now()
				claims := jwt.MapClaims{
					"iss":              authServer.issuerURL,
					"sub":              ethereumAddr.Hex(),
					"aud":              []string{"dimo.zone"},
					"exp":              now.Add(-time.Hour).Unix(), // Expired
					"iat":              now.Add(-2 * time.Hour).Unix(),
					"ethereum_address": ethereumAddr.Hex(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenExpired,
		},
		{
			name: "token with wrong issuer",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				now := time.Now()
				claims := jwt.MapClaims{
					"iss":              "https://wrong-issuer.com", // Wrong issuer
					"sub":              ethereumAddr.Hex(),
					"aud":              []string{"dimo.zone"},
					"exp":              now.Add(time.Hour).Unix(),
					"iat":              now.Unix(),
					"ethereum_address": ethereumAddr.Hex(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenInvalidIssuer,
		},
		{
			name: "malformed token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer invalid.token.here")
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()

			source, err := auth(req)

			if tt.expectedError {
				require.Error(t, err)
				if tt.errorIs != nil {
					assert.ErrorIs(t, err, tt.errorIs)
				}
				if tt.errorContains != "" {
					assert.ErrorContains(t, err, tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSource, source)
			}
		})
	}
}

func TestAttestationServer_EndToEnd(t *testing.T) {
	authServer := setupMockAuthServer(t)
	defer authServer.Close()

	var gotSource string
	var gotOK bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSource, gotOK = SourceFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	srv, err := NewAttestationServer(AttestationConfig{
		TokenExchangeIssuer:    authServer.issuerURL,
		TokenExchangeKeySetURL: authServer.issuerURL + "/keys",
	}, handler)
	require.NoError(t, err)
	assert.Equal(t, DefaultAttestationAddr, srv.Addr)

	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	t.Run("valid token reaches handler with source", func(t *testing.T) {
		ethereumAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
		token := authServer.createToken(t, ethereumAddr, nil)

		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.True(t, gotOK)
		assert.Equal(t, ethereumAddr.Hex(), gotSource)
	})

	t.Run("missing token is unauthorized", func(t *testing.T) {
		resp, err := ts.Client().Post(ts.URL+"/", "application/json", strings.NewReader(`{}`))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

func TestAttestationServer_RateLimit(t *testing.T) {
	authServer := setupMockAuthServer(t)
	defer authServer.Close()

	srv, err := NewAttestationServer(AttestationConfig{
		TokenExchangeIssuer:    authServer.issuerURL,
		TokenExchangeKeySetURL: authServer.issuerURL + "/keys",
		RateLimitRPS:           1,
		RateLimitBurst:         1,
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	require.NoError(t, err)

	token := authServer.createToken(t, common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"), nil)

	statuses := make([]int, 0, 3)
	for range 3 {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		req.RemoteAddr = "10.0.0.1:4444"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, req)
		statuses = append(statuses, rec.Code)
	}

	assert.Equal(t, http.StatusOK, statuses[0])
	assert.Contains(t, statuses[1:], http.StatusTooManyRequests)
}

func TestNewAttestationServer_BadIssuer(t *testing.T) {
	t.Parallel()

	_, err := NewAttestationServer(AttestationConfig{
		TokenExchangeIssuer:    "://not-a-url",
		TokenExchangeKeySetURL: "https://auth.dev.dimo.zone/keys",
	}, http.NotFoundHandler())
	require.Error(t, err, "malformed issuer URL must fail construction")
}

func TestAttestationAuth_UnreachableJWKSFailsAtRequestTime(t *testing.T) {
	// keyfunc.NewDefault tolerates an unreachable JWKS at construction and
	// refreshes in the background; token validation must still fail because
	// no signing keys are known.
	authServer := setupMockAuthServer(t)
	defer authServer.Close()

	auth, err := newAttestationAuth(authServer.issuerURL, fmt.Sprintf("http://127.0.0.1:1/%s", "keys"))
	require.NoError(t, err)

	token := authServer.createToken(t, common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"), nil)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	_, err = auth(req)
	require.Error(t, err, "token must not validate without JWKS keys")
}
