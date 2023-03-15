package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Requests

	// COMMITOFFSETS is the slug for a commit_offset request
	COMMITOFFSETS = "commit_offsets"

	// LISTCOMMITTEDOFFSETS is the slug for a list_committed_offsets request
	LISTCOMMITTEDOFFSETS = "list_committed_offsets"

	// POLL is the slug for a poll requeset
	POLL = "poll"

	// SEND is the slug for a send request
	SEND = "send"

	// Responses

	// COMMITOFFSETSOK is the slug for a commit_offset response
	COMMITOFFSETOK = "commit_offset_ok"

	// LISTCOMMITTEDOFFSETSOK is the slug for a list_committed_offsets response
	LISTCOMMITTEEDOFFSETSOK = "list_committed_offsets_ok"

	// POLLOK is the slug for a poll response
	POLLOK = "poll_ok"

	// SENDOK is the slug for a send response
	SENDOK = "send_ok"
)

// CommitOffsetsRequest represents the important information included
// in a CommitOffset request from the client
type CommitOffsetsRequest struct {
	Offsets map[string]int `json:"offsets"`
}

// ListCommitOffsetsRequest represents the important information included
// in a ListCommitOffsets request from the client
type ListCommitOffsetsRequest struct {
	Keys []string `json:"keys"`
}

// PollRequest represents the important information included
// in a Poll request from the client
type PollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

// SendRequest represents the important information included
// in a Send request from the client
type SendRequest struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

// ListCommittedOffsetsResponse represents the important information
// in a response to a ListCommittedOffsetsRequest
type ListCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

// PollResponse represents the important information
// in a response to a PollRequest
type PollResponse struct {
	Type string  `json:"type"`
	Msgs [][]int `json:"msgs"`
}

// SendResponse represents the important information
// in a response to a SendRequest
type SendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}
