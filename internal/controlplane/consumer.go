package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ConsumerInput struct {
	StreamID   string
	ConsumerID string
	Spec       natsv1.ConsumerSpec
}

type ConsumerResult struct {
	ConsumerID string
	StreamID   string
}

func (c *client) EnsureConsumer(ctx context.Context, in ConsumerInput) (ConsumerResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "consumer", "resourceName", in.Spec.Name, "streamID", in.StreamID)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return ConsumerResult{}, err
	}

	isPush := in.Spec.DeliverSubject != ""

	if in.ConsumerID == "" {
		in.ConsumerID = consumerIDFromStream(in.StreamID, in.Spec.Name)
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.ConsumerID != "" {
		if isPush {
			updatedID, err := c.updatePushConsumer(authCtx, in.ConsumerID, in)
			if err != nil {
				err = withAPIError(err)
				if isStatusCode(err, http.StatusNotFound) {
					l.Info("known push consumer ID not found, creating new consumer", "resourceID", in.ConsumerID)
					in.ConsumerID = ""
				} else {
					return ConsumerResult{}, fmt.Errorf("update push consumer by id %q: %w", in.ConsumerID, err)
				}
			} else {
				l.Info("push consumer updated", "resourceID", updatedID, "consumerType", "push")
				return ConsumerResult{ConsumerID: updatedID, StreamID: in.StreamID}, nil
			}
		}
		if in.ConsumerID != "" {
			updatedID, err := c.updatePullConsumer(authCtx, in.ConsumerID, in)
			if err != nil {
				err = withAPIError(err)
				if isStatusCode(err, http.StatusNotFound) {
					l.Info("known pull consumer ID not found, creating new consumer", "resourceID", in.ConsumerID)
					in.ConsumerID = ""
				} else {
					return ConsumerResult{}, fmt.Errorf("update pull consumer by id %q: %w", in.ConsumerID, err)
				}
			} else {
				l.Info("pull consumer updated", "resourceID", updatedID, "consumerType", "pull")
				return ConsumerResult{ConsumerID: updatedID, StreamID: in.StreamID}, nil
			}
		}
	}

	if isPush {
		desired := pushConsumerConfig(in)
		created, _, err := c.api.StreamAPI.CreatePushConsumer(authCtx, in.StreamID).JSPushConsumerConfigRequest(desired).Execute()
		if err != nil {
			err = withAPIError(err)
			return ConsumerResult{}, fmt.Errorf("create push consumer %q: %w", in.Spec.Name, err)
		}
		l.Info("push consumer created", "resourceID", created.Id, "consumerType", "push")
		return ConsumerResult{ConsumerID: created.Id, StreamID: in.StreamID}, nil
	}

	desired := pullConsumerConfig(in)

	created, _, err := c.api.StreamAPI.CreatePullConsumer(authCtx, in.StreamID).JSPullConsumerConfigRequest(desired).Execute()
	if err != nil {
		err = withAPIError(err)
		return ConsumerResult{}, fmt.Errorf("create pull consumer %q: %w", in.Spec.Name, err)
	}
	l.Info("pull consumer created", "resourceID", created.Id, "consumerType", "pull")
	return ConsumerResult{ConsumerID: created.Id, StreamID: in.StreamID}, nil
}

func (c *client) DeleteConsumer(ctx context.Context, in ConsumerInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "consumer", "resourceName", in.Spec.Name, "streamID", in.StreamID)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.ConsumerID == "" {
		in.ConsumerID = consumerIDFromStream(in.StreamID, in.Spec.Name)
	}

	if in.ConsumerID != "" {
		if in.Spec.DeliverSubject != "" {
			err := c.deletePushConsumer(authCtx, in.ConsumerID)
			if err == nil || isStatusCode(err, http.StatusNotFound) {
				l.Info("push consumer deleted", "resourceID", in.ConsumerID, "consumerType", "push")
				return nil
			}
			return fmt.Errorf("delete push consumer by id %q: %w", in.ConsumerID, err)
		}

		err := c.deletePullConsumer(authCtx, in.ConsumerID)
		if err == nil {
			l.Info("pull consumer deleted", "resourceID", in.ConsumerID, "consumerType", "pull")
			return nil
		}
		if !isStatusCode(err, http.StatusNotFound) {
			return fmt.Errorf("delete pull consumer by id %q: %w", in.ConsumerID, err)
		}

		err = c.deletePushConsumer(authCtx, in.ConsumerID)
		if err == nil {
			l.Info("push consumer deleted", "resourceID", in.ConsumerID, "consumerType", "push")
			return nil
		}
		if isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete push consumer by id %q: %w", in.ConsumerID, err)
	}

	return nil
}

func (c *client) ReadConsumerState(ctx context.Context, in ConsumerInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.ConsumerID == "" {
		in.ConsumerID = consumerIDFromStream(in.StreamID, in.Spec.Name)
	}
	if in.ConsumerID == "" {
		return nil, false, nil
	}

	if in.Spec.DeliverSubject != "" {
		info, _, err := c.api.PushConsumerAPI.GetPushConsumerInfo(authCtx, in.ConsumerID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get push consumer by consumer id %q: %w", in.ConsumerID, err)
		}
		state, err := json.Marshal(info.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	info, _, err := c.api.PullConsumerAPI.GetPullConsumerInfo(authCtx, in.ConsumerID).Execute()
	if err == nil {
		state, err := json.Marshal(info.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}
	err = withAPIError(err)
	if !isStatusCode(err, http.StatusNotFound) {
		return nil, false, fmt.Errorf("get pull consumer by consumer id %q: %w", in.ConsumerID, err)
	}

	pushInfo, _, err := c.api.PushConsumerAPI.GetPushConsumerInfo(authCtx, in.ConsumerID).Execute()
	if err != nil {
		err = withAPIError(err)
		if isStatusCode(err, http.StatusNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get push consumer by consumer id %q: %w", in.ConsumerID, err)
	}

	state, err := json.Marshal(pushInfo.Config)
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

func (c *client) updatePushConsumer(ctx context.Context, id string, in ConsumerInput) (string, error) {
	updateReq := pushConsumerUpdateConfig(in)
	updated, _, err := c.api.PushConsumerAPI.UpdatePushConsumer(ctx, id).JSPushConsumerUpdateRequest(updateReq).Execute()
	if err != nil {
		return "", err
	}
	return updated.Id, nil
}

func (c *client) updatePullConsumer(ctx context.Context, id string, in ConsumerInput) (string, error) {
	updateReq := pullConsumerUpdateConfig(in)
	updated, _, err := c.api.PullConsumerAPI.UpdatePullConsumer(ctx, id).JSPullConsumerUpdateRequest(updateReq).Execute()
	if err != nil {
		return "", err
	}
	return updated.Id, nil
}

func (c *client) deletePushConsumer(ctx context.Context, id string) error {
	_, err := c.api.PushConsumerAPI.DeletePushConsumer(ctx, id).Execute()
	return withAPIError(err)
}

func (c *client) deletePullConsumer(ctx context.Context, id string) error {
	_, err := c.api.PullConsumerAPI.DeletePullConsumer(ctx, id).Execute()
	return withAPIError(err)
}

func consumerIDFromStream(streamID, name string) string {
	if streamID == "" || name == "" {
		return ""
	}
	return streamID + "." + name
}

func pullConsumerConfig(in ConsumerInput) syncp.JSPullConsumerConfigRequest {
	s := in.Spec
	ackPolicy := mapAckPolicy(s.AckPolicy)
	deliverPolicy := mapDeliverPolicy(s.DeliverPolicy)
	replayPolicy := mapReplayPolicy(s.ReplayPolicy)

	replicas := int64(s.Replicas)
	description := s.Description
	name := s.Name

	cfg := syncp.JSPullConsumerConfigRequest{
		JSCommonConsumerConfigRequest: syncp.JSCommonConsumerConfigRequest{
			Name:          &name,
			Description:   &description,
			AckPolicy:     ackPolicy,
			DeliverPolicy: deliverPolicy,
			ReplayPolicy:  replayPolicy,
			NumReplicas:   replicas,
		},
	}

	if s.MaxAckPending > 0 {
		v := int64(s.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if s.MaxDeliver > 0 {
		v := int64(s.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if s.MemStorage {
		cfg.MemStorage = &s.MemStorage
	}
	if s.Direct {
		cfg.Direct = &s.Direct
	}
	if s.DurableName != "" {
		cfg.DurableName = &s.DurableName
	} else {
		cfg.DurableName = &name
	}
	if s.AckWait != "" {
		if d, err := time.ParseDuration(s.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if len(s.FilterSubjects) > 0 {
		cfg.FilterSubjects = s.FilterSubjects
	}
	if s.InactiveThreshold != "" {
		if d, err := time.ParseDuration(s.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if s.OptStartSeq > 0 {
		cfg.OptStartSeq = &s.OptStartSeq
	}
	if s.OptStartTime != "" {
		if t, err := time.Parse(time.RFC3339, s.OptStartTime); err == nil {
			cfg.OptStartTime = &t
		}
	}
	if s.SampleFreq != "" {
		cfg.SampleFreq = &s.SampleFreq
	}
	if len(s.Backoff) > 0 {
		cfg.Backoff = parseDurations(s.Backoff)
	}
	if s.Metadata != nil {
		// Metadata not directly on JSCommonConsumerConfigRequest; skip for now.
	}

	// Pull-specific.
	if s.MaxRequestBatch > 0 {
		v := int64(s.MaxRequestBatch)
		cfg.MaxBatch = &v
	}
	if s.MaxRequestMaxBytes > 0 {
		v := int64(s.MaxRequestMaxBytes)
		cfg.MaxBytes = &v
	}
	if s.MaxRequestExpires != "" {
		if d, err := time.ParseDuration(s.MaxRequestExpires); err == nil {
			ns := int64(d)
			cfg.MaxExpires = &ns
		}
	}
	if s.MaxWaiting > 0 {
		v := int64(s.MaxWaiting)
		cfg.MaxWaiting = &v
	}

	return cfg
}

func pushConsumerConfig(in ConsumerInput) syncp.JSPushConsumerConfigRequest {
	s := in.Spec
	ackPolicy := mapAckPolicy(s.AckPolicy)
	deliverPolicy := mapDeliverPolicy(s.DeliverPolicy)
	replayPolicy := mapReplayPolicy(s.ReplayPolicy)

	replicas := int64(s.Replicas)
	description := s.Description
	name := s.Name

	cfg := syncp.JSPushConsumerConfigRequest{
		JSCommonConsumerConfigRequest: syncp.JSCommonConsumerConfigRequest{
			Name:          &name,
			DurableName:   &name,
			Description:   &description,
			AckPolicy:     ackPolicy,
			DeliverPolicy: deliverPolicy,
			ReplayPolicy:  replayPolicy,
			NumReplicas:   replicas,
		},
		DeliverSubject: &s.DeliverSubject,
	}

	if s.MaxAckPending > 0 {
		v := int64(s.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if s.MaxDeliver > 0 {
		v := int64(s.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if s.MemStorage {
		cfg.MemStorage = &s.MemStorage
	}
	if s.Direct {
		cfg.Direct = &s.Direct
	}
	if s.DurableName != "" {
		cfg.DurableName = &s.DurableName
	}
	if s.DeliverGroup != "" {
		cfg.DeliverGroup = &s.DeliverGroup
	}
	if s.AckWait != "" {
		if d, err := time.ParseDuration(s.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if len(s.FilterSubjects) > 0 {
		cfg.FilterSubjects = s.FilterSubjects
	}
	if s.InactiveThreshold != "" {
		if d, err := time.ParseDuration(s.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if s.OptStartSeq > 0 {
		cfg.OptStartSeq = &s.OptStartSeq
	}
	if s.OptStartTime != "" {
		if t, err := time.Parse(time.RFC3339, s.OptStartTime); err == nil {
			cfg.OptStartTime = &t
		}
	}
	if s.SampleFreq != "" {
		cfg.SampleFreq = &s.SampleFreq
	}
	if len(s.Backoff) > 0 {
		cfg.Backoff = parseDurations(s.Backoff)
	}

	// Push-specific.
	if s.FlowControl {
		cfg.FlowControl = &s.FlowControl
	}
	if s.HeadersOnly {
		cfg.HeadersOnly = &s.HeadersOnly
	}
	if s.HeartbeatInterval != "" {
		if d, err := time.ParseDuration(s.HeartbeatInterval); err == nil {
			ns := int64(d)
			cfg.IdleHeartbeat = &ns
		}
	}
	if s.RateLimitBps > 0 {
		cfg.RateLimitBps = &s.RateLimitBps
	}

	return cfg
}

func pullConsumerUpdateConfig(in ConsumerInput) syncp.JSPullConsumerUpdateRequest {
	s := in.Spec
	description := s.Description
	cfg := syncp.JSPullConsumerUpdateRequest{
		Description: &description,
	}
	if s.AckWait != "" {
		if d, err := time.ParseDuration(s.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if s.MaxAckPending > 0 {
		v := int64(s.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if s.MaxDeliver > 0 {
		v := int64(s.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if s.Replicas > 0 {
		v := int64(s.Replicas)
		cfg.NumReplicas = &v
	}
	if s.InactiveThreshold != "" {
		if d, err := time.ParseDuration(s.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if s.SampleFreq != "" {
		cfg.SampleFreq = &s.SampleFreq
	}
	if len(s.FilterSubjects) > 0 {
		cfg.FilterSubjects = s.FilterSubjects
	}
	if len(s.Backoff) > 0 {
		cfg.Backoff = parseDurations(s.Backoff)
	}
	if s.Metadata != nil {
		cfg.Metadata = s.Metadata
	}
	if s.MaxRequestBatch > 0 {
		v := int64(s.MaxRequestBatch)
		cfg.MaxBatch = &v
	}
	if s.MaxRequestMaxBytes > 0 {
		v := int64(s.MaxRequestMaxBytes)
		cfg.MaxBytes = &v
	}
	if s.MaxRequestExpires != "" {
		if d, err := time.ParseDuration(s.MaxRequestExpires); err == nil {
			ns := int64(d)
			cfg.MaxExpires = &ns
		}
	}
	return cfg
}

func pushConsumerUpdateConfig(in ConsumerInput) syncp.JSPushConsumerUpdateRequest {
	s := in.Spec
	description := s.Description
	cfg := syncp.JSPushConsumerUpdateRequest{
		Description: &description,
	}
	if s.AckWait != "" {
		if d, err := time.ParseDuration(s.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if s.MaxAckPending > 0 {
		v := int64(s.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if s.MaxDeliver > 0 {
		v := int64(s.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if s.Replicas > 0 {
		v := int64(s.Replicas)
		cfg.NumReplicas = &v
	}
	if s.InactiveThreshold != "" {
		if d, err := time.ParseDuration(s.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if s.SampleFreq != "" {
		cfg.SampleFreq = &s.SampleFreq
	}
	if len(s.FilterSubjects) > 0 {
		cfg.FilterSubjects = s.FilterSubjects
	}
	if len(s.Backoff) > 0 {
		cfg.Backoff = parseDurations(s.Backoff)
	}
	if s.Metadata != nil {
		cfg.Metadata = s.Metadata
	}
	if s.DeliverSubject != "" {
		cfg.DeliverSubject = &s.DeliverSubject
	}
	if s.HeadersOnly {
		cfg.HeadersOnly = &s.HeadersOnly
	}
	if s.RateLimitBps > 0 {
		cfg.RateLimitBps = &s.RateLimitBps
	}
	return cfg
}
