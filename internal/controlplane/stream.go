package controlplane

import (
	"context"
	"encoding/json"
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

	accountID := in.AccountID
	if in.StreamID == "" {
		accountID, err = c.resolveAccountID(authCtx, in.AccountSelectors)
		if err != nil {
			return StreamResult{}, err
		}
		in.StreamID = streamIDFromAccount(accountID, in.Name)
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.StreamID != "" {
		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, in.StreamID).JSStreamConfigRequest(desired).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				l.Info("known stream ID not found, creating new stream", "resourceID", in.StreamID)
				in.StreamID = ""
			} else {
				return StreamResult{}, fmt.Errorf("update stream by stream id %q: %w", in.StreamID, err)
			}
		} else {
			l.Info("stream updated", "resourceID", updated.Id, "accountID", in.AccountID)
			return StreamResult{AccountID: in.AccountID, StreamID: updated.Id}, nil
		}
	}

	if accountID == "" {
		accountID, err = c.resolveAccountID(authCtx, in.AccountSelectors)
		if err != nil {
			return StreamResult{}, err
		}
	}

	created, _, err := c.api.AccountAPI.CreateStream(authCtx, accountID).JSStreamConfigRequest(desired).Execute()
	if err != nil {
		err = withAPIError(err)
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

	if in.StreamID == "" {
		accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
		if err != nil {
			if isAccountNotFound(err) {
				return nil
			}
			return err
		}
		in.StreamID = streamIDFromAccount(accountID, in.Name)
	}

	if in.StreamID != "" {
		_, err := c.api.StreamAPI.DeleteStream(authCtx, in.StreamID).Execute()
		err = withAPIError(err)
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			l.Info("stream deleted", "resourceID", in.StreamID)
			return nil
		}
		return fmt.Errorf("delete stream by stream id %q: %w", in.StreamID, err)
	}

	return nil
}

func (c *client) ReadStreamState(ctx context.Context, in StreamInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.StreamID == "" {
		accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
		if err != nil {
			if isAccountNotFound(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
		in.StreamID = streamIDFromAccount(accountID, in.Name)
	}

	if in.StreamID != "" {
		info, _, err := c.api.StreamAPI.GetStreamInfo(authCtx, in.StreamID).Execute()
		if err != nil {
			err = withAPIError(err)
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

	return nil, false, nil
}

func streamIDFromAccount(accountID, streamName string) string {
	if accountID == "" || streamName == "" {
		return ""
	}
	return accountID + "." + streamName
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
