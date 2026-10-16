package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

type DispatcherConfig struct {
	Timeout              time.Duration // total per-request timeout
	MaxResponseBytes     int64         // drain at most this much of the response
	AllowPrivateNetworks bool          // dev only; false in production (SSRF guard)
	UserAgent            string
}

type Result struct {
	StatusCode int
	Duration   time.Duration
	Err        error
	Retryable  bool
}

func (r Result) Success() bool { return r.Err == nil }

type Dispatcher struct {
	cfg    DispatcherConfig
	client *http.Client
}

func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 64 << 10
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "RealTimeChat-Webhooks/1.0"
	}

	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		// Control runs after DNS resolution, so it also defeats DNS-rebinding:
		// we validate the actual IP being connected to.
		Control: func(network, address string, _ syscall.RawConn) error {
			if cfg.AllowPrivateNetworks {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil || isBlockedIP(ip) {
				return fmt.Errorf("%w: %s", ErrBlockedAddress, host)
			}
			return nil
		},
	}

	transport := &http.Transport{
		Proxy:                 nil, // never honour env proxies: they bypass the SSRF guard
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       60 * time.Second,
	}

	return &Dispatcher{
		cfg: cfg,
		client: &http.Client{
			Timeout:       cfg.Timeout,
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Deliver performs one signed POST. It never panics and never retries by itself;
// retry decisions belong to the Processor.
func (d *Dispatcher) Deliver(ctx context.Context, ep Endpoint, env Envelope, attempt int) Result {
	start := time.Now()
	body, err := json.Marshal(env.Outbound())
	if err != nil {
		return Result{Err: err, Duration: time.Since(start)}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		return Result{Err: err, Duration: time.Since(start)}
	}
	now := time.Now()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", d.cfg.UserAgent)
	req.Header.Set(HeaderID, env.EventID)
	req.Header.Set(HeaderAttempt, strconv.Itoa(attempt))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(now.Unix(), 10))
	req.Header.Set(HeaderSignature, Sign(ep.Secret, now, body))

	resp, err := d.client.Do(req)
	if err != nil {
		return Result{
			Err:       err,
			Duration:  time.Since(start),
			Retryable: !errors.Is(err, ErrBlockedAddress), // blocked target will never become valid
		}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, d.cfg.MaxResponseBytes))

	res := Result{StatusCode: resp.StatusCode, Duration: time.Since(start)}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return res
	}
	res.Err = fmt.Errorf("unexpected status %d", resp.StatusCode)
	res.Retryable = resp.StatusCode >= 500 ||
		resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooEarly ||
		resp.StatusCode == http.StatusTooManyRequests
	return res
}

var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

func isBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() ||
		cgnat.Contains(ip) // 169.254.169.254 (cloud metadata) is link-local, covered above
}
