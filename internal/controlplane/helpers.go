package controlplane

import (
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
)

func mapAckPolicy(v string) syncp.AckPolicy {
	switch v {
	case string(syncp.ACKPOLICY_ALL):
		return syncp.ACKPOLICY_ALL
	case string(syncp.ACKPOLICY_NONE):
		return syncp.ACKPOLICY_NONE
	case string(syncp.ACKPOLICY_EXPLICIT):
		return syncp.ACKPOLICY_EXPLICIT
	default:
		return syncp.ACKPOLICY_EXPLICIT
	}
}

func mapDeliverPolicy(v string) syncp.DeliverPolicy {
	switch v {
	case string(syncp.DELIVERPOLICY_ALL):
		return syncp.DELIVERPOLICY_ALL
	case string(syncp.DELIVERPOLICY_LAST):
		return syncp.DELIVERPOLICY_LAST
	case string(syncp.DELIVERPOLICY_LAST_PER_SUBJECT):
		return syncp.DELIVERPOLICY_LAST_PER_SUBJECT
	case string(syncp.DELIVERPOLICY_NEW):
		return syncp.DELIVERPOLICY_NEW
	case string(syncp.DELIVERPOLICY_BY_START_SEQUENCE):
		return syncp.DELIVERPOLICY_BY_START_SEQUENCE
	case string(syncp.DELIVERPOLICY_BY_START_TIME):
		return syncp.DELIVERPOLICY_BY_START_TIME
	default:
		return syncp.DELIVERPOLICY_ALL
	}
}

func mapReplayPolicy(v string) syncp.ReplayPolicy {
	switch v {
	case string(syncp.REPLAYPOLICY_ORIGINAL):
		return syncp.REPLAYPOLICY_ORIGINAL
	default:
		return syncp.REPLAYPOLICY_INSTANT
	}
}

func parseDurations(in []string) []int64 {
	out := make([]int64, 0, len(in))
	for _, s := range in {
		if d, err := time.ParseDuration(s); err == nil {
			out = append(out, int64(d))
		}
	}
	return out
}

func streamFirstSeqPtr(v uint64) *uint64 {
	if v == 0 {
		return nil
	}
	return &v
}

func mapCompression(v string) *syncp.S2Compression {
	if v != string(syncp.S2COMPRESSION_S2) {
		return nil
	}
	c := syncp.S2COMPRESSION_S2
	return &c
}
