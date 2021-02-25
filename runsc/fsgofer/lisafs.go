// Copyright 2021 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fsgofer

import (
	"path/filepath"
	"syscall"

	"gvisor.dev/gvisor/pkg/lisafs"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/marshal"
	"gvisor.dev/gvisor/pkg/marshal/primitive"
)

// LisafsHandlers are fsgofer's RPC handlers for lisafs protocol messages.
var LisafsHandlers []lisafs.RPCHanlder = buildLisafsHandlers()

// MountHandler handles the Mount RPC for fsgofer.
func MountHandler(c *lisafs.Connection, payload []byte) (marshal.Marshallable, []int, error) {
	var req lisafs.MountReq
	req.UnmarshalBytes(payload)

	if gotMountPath := filepath.Clean(req.MountPath.String()); gotMountPath != c.Server().MountPath() {
		log.Warningf("incorrect mount path found in request: expected %q, got %q", c.Server().MountPath(), gotMountPath)
		return nil, nil, syscall.EINVAL
	}

	var resp lisafs.MountResp
	// TODO(ayushranjan): generate a legit root FD.
	resp.Root = 1
	resp.MaxM = c.MaxMessage()
	resp.UnsupportedMs = c.UnsupportedMessages()
	resp.NumUnsupported = primitive.Uint16(len(resp.UnsupportedMs))
	return &resp, nil, nil
}

func buildLisafsHandlers() []lisafs.RPCHanlder {
	var handlers [3]lisafs.RPCHanlder
	handlers[lisafs.Error] = nil // No error handler needed.
	handlers[lisafs.Mount] = MountHandler
	handlers[lisafs.Channel] = lisafs.ChannelHandler
	return handlers[:]
}
