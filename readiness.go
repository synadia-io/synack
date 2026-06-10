package main

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/go-logr/logr"
)

type controlPlaneReadiness struct {
	client      tokenValidator
	baseURL     string
	log         logr.Logger
	ready       atomic.Bool
	loggedError atomic.Bool
}

type tokenValidator interface {
	ValidateToken(context.Context) error
}

func newControlPlaneReadiness(client tokenValidator, baseURL string, log logr.Logger) *controlPlaneReadiness {
	return &controlPlaneReadiness{
		client:  client,
		baseURL: baseURL,
		log:     log,
	}
}

func (r *controlPlaneReadiness) Check(req *http.Request) error {
	if r.ready.Load() {
		return nil
	}

	err := r.client.ValidateToken(req.Context())
	if err != nil {
		if !r.loggedError.Swap(true) {
			r.log.Error(err, "control plane token validation failed", "baseURL", r.baseURL)
		}
		return err
	}

	if !r.ready.Swap(true) {
		r.log.Info("connected to control plane", "baseURL", r.baseURL)
	}
	r.loggedError.Store(false)

	return nil
}
