package server

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"
)

// ErrInvalidEthAddr reports a JWT whose ethereum_address claim is missing
// or the zero address.
var ErrInvalidEthAddr = errors.New("ethereum address not set in claim")

var zeroAddress common.Address

// Claims is the token-exchange JWT claim set used for attestation auth.
type Claims struct {
	EmailAddress    *string        `json:"email,omitempty"`
	ProviderID      *string        `json:"provider_id,omitempty"`
	EthereumAddress common.Address `json:"ethereum_address,omitempty"`
	jwt.RegisteredClaims
}

// certSourceHandler extracts the source address from the verified client
// certificate's CommonName and injects it into the request context.
func certSourceHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		source := r.TLS.VerifiedChains[0][0].Subject.CommonName
		next.ServeHTTP(w, r.WithContext(WithSource(r.Context(), source)))
	})
}

// newAttestationAuth builds the JWT validation function for the attestation
// server. It fetches the JWKS from the token-exchange key set URL and
// validates issuer, signature (RS256), and the ethereum_address claim,
// returning the claim's hex address as the source.
func newAttestationAuth(issuer, jwksURL string) (func(*http.Request) (string, error), error) {
	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse issuer URL: %w", err)
	}

	jwksResource, err := keyfunc.NewDefault([]string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("failed to create a keyfunc.Keyfunc from the server's URL: %w", err)
	}
	parser := jwt.NewParser(
		jwt.WithIssuer(issuerURL.String()),
		jwt.WithValidMethods([]string{"RS256"}),
	)

	return func(r *http.Request) (string, error) {
		authStr := r.Header.Get("Authorization")
		tokenStr := strings.TrimSpace(strings.Replace(authStr, "Bearer ", "", 1))

		var claims Claims
		if _, err := parser.ParseWithClaims(tokenStr, &claims, jwksResource.Keyfunc); err != nil {
			return "", fmt.Errorf("invalid token string: %w", err)
		}

		if claims.EthereumAddress == zeroAddress {
			return "", ErrInvalidEthAddr
		}

		return claims.EthereumAddress.Hex(), nil
	}, nil
}

// maxBytesMiddleware caps request body reads at limit bytes using
// http.MaxBytesReader; handlers reading past the limit get an
// *http.MaxBytesError.
func maxBytesMiddleware(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// remoteLimiter holds one token bucket per remote key.
type remoteLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	rps      rate.Limit
	burst    int
}

func newRemoteLimiter(rps float64, burst int) *remoteLimiter {
	return &remoteLimiter{
		limiters: map[string]*rate.Limiter{},
		rps:      rate.Limit(rps),
		burst:    burst,
	}
}

func (l *remoteLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limiters[key]
	if !ok {
		lim = rate.NewLimiter(l.rps, l.burst)
		l.limiters[key] = lim
	}
	return lim.Allow()
}

// rateLimitMiddleware enforces a per-remote token bucket keyed by keyFn,
// answering 429 when the bucket is empty. rps <= 0 disables limiting.
func rateLimitMiddleware(rps float64, burst int, keyFn func(*http.Request) string) func(http.Handler) http.Handler {
	if rps <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	if burst <= 0 {
		burst = int(math.Ceil(rps))
		if burst < 1 {
			burst = 1
		}
	}
	limiter := newRemoteLimiter(rps, burst)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.allow(keyFn(r)) {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// remoteIPKey keys rate limiting on the remote host address.
func remoteIPKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// certCNKey keys rate limiting on the client certificate CommonName so each
// connection license gets its own bucket; falls back to the remote IP when
// no verified chain is present.
func certCNKey(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		return r.TLS.VerifiedChains[0][0].Subject.CommonName
	}
	return remoteIPKey(r)
}
