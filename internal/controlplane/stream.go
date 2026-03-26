package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type StreamInput struct {
	AccountSelectors

	StreamID string

	Name     string
	Subjects []string

	Description       string
	Retention         string
	MaxConsumers      int
	MaxMsgsPerSubject int
	MaxMsgs           int
	MaxBytes          int
	MaxAge            string
	MaxMsgSize        int
	Storage           string
	Discard           string
	Replicas          int
	NoAck             bool
	DuplicateWindow   string
	Placement         *syncp.Placement
	Sources           []syncp.StreamSource
	Compression       string
	SubjectTransform  *syncp.SubjectTransformConfig
	RePublish         *syncp.RePublish
	Sealed            bool
	DenyDelete        bool
	DenyPurge         bool
	AllowDirect       bool
	AllowRollup       bool
	DiscardPerSubject bool
	FirstSequence     uint64
	Metadata          map[string]string
}

type StreamResult struct {
	AccountID string
	StreamID  string
}

func (c *client) EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "stream", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return StreamResult{}, err
	}

	desired := inputToStreamConfig(in)

	// If we already know the ID, use it directly — never fall through to create.
	if in.StreamID != "" {
		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, in.StreamID).JSStreamConfigRequest(desired).Execute()
		if err != nil {
			return StreamResult{}, fmt.Errorf("update stream by stream id %q: %w", in.StreamID, err)
		}
		l.Info("stream updated", "resourceID", updated.Id, "accountID", in.AccountID)
		return StreamResult{AccountID: in.AccountID, StreamID: updated.Id}, nil
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		return StreamResult{}, err
	}

	list, _, err := c.api.AccountAPI.ListStreams(authCtx, accountID).Execute()
	if err != nil {
		return StreamResult{}, fmt.Errorf("list streams: %w", err)
	}

	for _, s := range list.Items {
		if s.Config.Name != in.Name {
			continue
		}

		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, s.Id).JSStreamConfigRequest(desired).Execute()
		if err != nil {
			return StreamResult{}, fmt.Errorf("update stream %q: %w", in.Name, err)
		}

		l.Info("stream updated", "resourceID", updated.Id, "accountID", accountID)

		return StreamResult{AccountID: accountID, StreamID: updated.Id}, nil
	}

	created, _, err := c.api.AccountAPI.CreateStream(authCtx, accountID).JSStreamConfigRequest(desired).Execute()
	if err != nil {
		return StreamResult{}, fmt.Errorf("create stream %q: %w", in.Name, err)
	}

	l.Info("stream created", "resourceID", created.Id, "accountID", accountID)

	return StreamResult{AccountID: accountID, StreamID: created.Id}, nil
}

func (c *client) DeleteStream(ctx context.Context, in StreamInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "stream", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.StreamID != "" {
		_, err := c.api.StreamAPI.DeleteStream(authCtx, in.StreamID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			l.Info("stream deleted", "resourceID", in.StreamID)
			return nil
		}
		return fmt.Errorf("delete stream by stream id %q: %w", in.StreamID, err)
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil
		}
		return err
	}

	list, _, err := c.api.AccountAPI.ListStreams(authCtx, accountID).Execute()
	if err != nil {
		return fmt.Errorf("list streams for delete: %w", err)
	}

	for _, s := range list.Items {
		if s.Config.Name != in.Name {
			continue
		}

		_, err := c.api.StreamAPI.DeleteStream(authCtx, s.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			l.Info("stream deleted", "resourceID", s.Id, "accountID", accountID)
			return nil
		}

		return fmt.Errorf("delete stream %q: %w", in.Name, err)
	}

	return nil
}

func (c *client) ReadStreamState(ctx context.Context, in StreamInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.StreamID != "" {
		info, _, err := c.api.StreamAPI.GetStreamInfo(authCtx, in.StreamID).Execute()
		if err != nil {
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get stream by stream id %q: %w", in.StreamID, err)
		}
		state, err := json.Marshal(info.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	list, _, err := c.api.AccountAPI.ListStreams(authCtx, accountID).Execute()
	if err != nil {
		return nil, false, fmt.Errorf("list streams: %w", err)
	}

	for _, s := range list.Items {
		if s.Config.Name != in.Name {
			continue
		}
		state, err := json.Marshal(s.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	return nil, false, nil
}

func inputToStreamConfig(in StreamInput) syncp.JSStreamConfigRequest {
	maxAge := int64(0)
	if in.MaxAge != "" {
		if d, err := time.ParseDuration(in.MaxAge); err == nil {
			maxAge = int64(d)
		}
	}

	duplicateWindow := int64(0)
	if in.DuplicateWindow != "" {
		if d, err := time.ParseDuration(in.DuplicateWindow); err == nil {
			duplicateWindow = int64(d)
		}
	}

	retention := syncp.RETENTIONPOLICY_LIMITS
	switch in.Retention {
	case string(syncp.RETENTIONPOLICY_INTEREST):
		retention = syncp.RETENTIONPOLICY_INTEREST
	case string(syncp.RETENTIONPOLICY_WORKQUEUE):
		retention = syncp.RETENTIONPOLICY_WORKQUEUE
	}

	storage := syncp.STORAGETYPE_MEMORY
	if in.Storage == string(syncp.STORAGETYPE_FILE) {
		storage = syncp.STORAGETYPE_FILE
	}

	discard := syncp.DISCARDPOLICY_OLD
	if in.Discard == string(syncp.DISCARDPOLICY_NEW) {
		discard = syncp.DISCARDPOLICY_NEW
	}

	replicas := max(int64(in.Replicas), 1)

	description := in.Description
	noAck := in.NoAck
	discardPerSubject := in.DiscardPerSubject
	maxMsgSize := int64(in.MaxMsgSize)
	firstSeq := in.FirstSequence
	compression := mapCompression(in.Compression)

	return syncp.JSStreamConfigRequest{
		Subjects: in.Subjects,
		JSCommonStreamConfig: syncp.JSCommonStreamConfig{
			AllowDirect:          in.AllowDirect,
			AllowRollupHdrs:      in.AllowRollup,
			Compression:          compression,
			DenyDelete:           in.DenyDelete,
			DenyPurge:            in.DenyPurge,
			Description:          &description,
			Discard:              discard,
			DiscardNewPerSubject: &discardPerSubject,
			DuplicateWindow:      &duplicateWindow,
			FirstSeq:             streamFirstSeqPtr(firstSeq),
			MaxAge:               maxAge,
			MaxBytes:             int64(in.MaxBytes),
			MaxConsumers:         int64(in.MaxConsumers),
			MaxMsgSize:           &maxMsgSize,
			MaxMsgs:              int64(in.MaxMsgs),
			MaxMsgsPerSubject:    int64(in.MaxMsgsPerSubject),
			Metadata:             in.Metadata,
			Name:                 in.Name,
			NoAck:                &noAck,
			NumReplicas:          replicas,
			Placement:            in.Placement,
			Republish:            in.RePublish,
			Retention:            retention,
			Sealed:               in.Sealed,
			Sources:              in.Sources,
			Storage:              storage,
			SubjectTransform:     in.SubjectTransform,
		},
	}
}
