package server

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

// httpHandlerPanics counts panics recovered in an HTTP handler (returned as 500).
var httpHandlerPanics = promauto.NewCounter(prometheus.CounterOpts{
	Name: "din_http_handler_panics_total",
	Help: "Panics recovered in an HTTP handler and returned as 500.",
})

// recoverMiddleware turns a panic in any downstream handler into a logged 500 plus a
// metric, rather than net/http's default — which dumps the stack to stderr outside the
// structured log and drops the connection with no response. Apply it as the OUTERMOST
// wrapper so it also covers the auth / rate-limit / max-bytes middleware.
func recoverMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					httpHandlerPanics.Inc()
					logger.Error().Interface("panic", rec).Str("path", r.URL.Path).
						Str("remote", r.RemoteAddr).Bytes("stack", debug.Stack()).
						Msg("recovered from panic in HTTP handler")
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

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
		tokenStr, ok := strings.CutPrefix(authStr, "Bearer ")
		if !ok {
			return "", errors.New("authorization header must use the Bearer scheme")
		}
		tokenStr = strings.TrimSpace(tokenStr)

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

// limiterMaxKeys bounds the per-remote bucket map across all shards. A
// device fleet churning source IPs (or an adversary spraying keys) must
// not grow process memory without bound.
const limiterMaxKeys = 100_000

// limiterShards splits the key space so eviction — an O(shard-size) walk
// under that shard's lock — never stalls more than 1/64th of traffic, and
// walks ~1.5k entries instead of 100k.
const limiterShards = 64

// remoteLimiter holds one token bucket per remote key, sharded by key
// hash, bounded at limiterMaxKeys entries total.
type remoteLimiter struct {
	shards [limiterShards]limiterShard
	rps    rate.Limit
	burst  int
}

type limiterShard struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

func newRemoteLimiter(rps float64, burst int) *remoteLimiter {
	l := &remoteLimiter{rps: rate.Limit(rps), burst: burst}
	for i := range l.shards {
		l.shards[i].limiters = map[string]*rate.Limiter{}
	}
	return l
}

func (l *remoteLimiter) shard(key string) *limiterShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &l.shards[h.Sum32()%limiterShards]
}

func (l *remoteLimiter) allow(key string) bool {
	s := l.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	lim, ok := s.limiters[key]
	if !ok {
		if len(s.limiters) >= limiterMaxKeys/limiterShards {
			s.evictLocked(l.burst)
		}
		lim = rate.NewLimiter(l.rps, l.burst)
		s.limiters[key] = lim
	}
	return lim.Allow()
}

// evictLocked drops idle entries from one shard. A bucket refilled to full
// burst behaves identically to a brand-new limiter, so removing it changes
// nothing for that remote. If every bucket is hot (pathological key
// cardinality), arbitrary entries go anyway — bounded memory beats perfect
// fairness; the affected remotes merely regain a fresh burst.
func (s *limiterShard) evictLocked(burst int) {
	for key, lim := range s.limiters {
		if lim.Tokens() >= float64(burst) {
			delete(s.limiters, key)
		}
	}
	max := limiterMaxKeys / limiterShards
	if len(s.limiters) < max {
		return
	}
	dropped := 0
	for key := range s.limiters {
		delete(s.limiters, key)
		dropped++
		if dropped >= max/10 {
			break
		}
	}
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
