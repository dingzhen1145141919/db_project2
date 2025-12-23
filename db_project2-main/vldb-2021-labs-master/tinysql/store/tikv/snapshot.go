// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
)

const (
	scanBatchSize = 256
)

// tikvSnapshot implements the kv.Snapshot interface.
type tikvSnapshot struct {
	store   *TinykvStore
	version kv.Version
	syncLog bool
	keyOnly bool
	vars    *kv.Variables
	minCommitTSPushed

	// Cache the result of BatchGet.
	// The invariance is that calling BatchGet multiple times using the same start ts,
	// the result should not change.
	// NOTE: This representation here is different from the BatchGet API.
	// cached use len(value)=0 to represent a key-value entry doesn't exist (a reliable truth from TiKV).
	// In the BatchGet API, it use no key-value entry to represent non-exist.
	// It's OK as long as there are no zero-byte values in the protocol.
	cached map[string][]byte
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *TinykvStore, ver kv.Version) *tikvSnapshot {
	return &tikvSnapshot{
		store:   store,
		version: ver,
		vars:    kv.DefaultVars,
		minCommitTSPushed: minCommitTSPushed{
			data: make(map[uint64]struct{}, 5),
		},
	}
}

func (s *tikvSnapshot) setSnapshotTS(ts uint64) {
	// Invalidate cache if the snapshotTS change!
	s.version.Ver = ts
	s.cached = nil
	// And also the minCommitTS pushed information.
	s.minCommitTSPushed.data = make(map[uint64]struct{}, 5)
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	ctx = context.WithValue(ctx, txnStartKey, s.version.Ver)
	val, err := s.get(NewBackoffer(ctx, getMaxBackoff), k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
}

func (s *tikvSnapshot) get(bo *Backoffer, k kv.Key) ([]byte, error) {
	// Check the cached values first.
	if s.cached != nil {
		if value, ok := s.cached[string(k)]; ok {
			return value, nil
		}
	}

	failpoint.Inject("snapshot-get-cache-fail", func(_ failpoint.Value) {
		if bo.ctx.Value("TestSnapshotCache") != nil {
			panic("cache miss")
		}
	})

	cli := clientHelper{
		LockResolver:      s.store.lockResolver,
		RegionCache:       s.store.regionCache,
		minCommitTSPushed: &s.minCommitTSPushed,
		Client:            s.store.client,
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdGet,
		&pb.GetRequest{
			Key:     k,
			Version: s.version.Ver,
		}, pb.Context{})
	for {
		loc, err := s.store.regionCache.LocateKey(bo, k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, loc.Region, readTimeoutShort, "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return nil, errors.Trace(ErrBodyMissing)
		}
		cmdGetResp := resp.Resp.(*pb.GetResponse)
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			// 你需要在此处处理key相关的错误
            // 若该key错误是由锁引起的，存在两种可能的情况：
            //   1. 对应的事务正在提交中，等待一段时间后重试即可。
            //   2. 对应的事务已异常终止但遗留了部分锁，需要解析并处理这些锁。
            // 请在此处编写代码（实验三要求）。
			// 1. 从key错误中提取锁信息
			lock := keyErr.GetLocked()
			if lock != nil {
				// 2. 封装锁为本地Lock对象
				l := NewLock(lock)
				// 3. 调用ResolveLocks解析锁（查询锁所属事务的状态并处理）
				msBeforeTxnExpired, err := cli.ResolveLocks(bo, s.version.Ver, []*Lock{l})
				if err != nil {
					return nil, errors.Trace(err)
				}
				// 4. 若锁所属事务仍在进行中（还有过期时间），执行退避等待
				if msBeforeTxnExpired > 0 {
					err = bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeTxnExpired), errors.New(lock.String()))
					if err != nil {
						return nil, errors.Trace(err)
					}
				}
				// 5. 锁处理完成后，重试读操作（continue进入下一次循环）
				continue
			}
			// 6. 若不是锁错误，返回未知错误
			return nil, errors.Errorf("unexpected key error: %v", keyErr)
					}
		return val, nil
	}
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	scanner, err := newScanner(s, k, upperBound, scanBatchSize, false)
	return scanner, errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	scanner, err := newScanner(s, nil, k, scanBatchSize, true)
	return scanner, errors.Trace(err)
}

func extractLockFromKeyErr(keyErr *pb.KeyError) (*Lock, error) {
	if locked := keyErr.GetLocked(); locked != nil {
		return NewLock(locked), nil
	}
	return nil, extractKeyErr(keyErr)
}

func extractKeyErr(keyErr *pb.KeyError) error {
	failpoint.Inject("ErrMockRetryableOnly", func(val failpoint.Value) {
		if val.(bool) {
			keyErr.Conflict = nil
			keyErr.Retryable = "mock retryable error"
		}
	})

	if keyErr.Conflict != nil {
		return newWriteConflictError(keyErr.Conflict)
	}
	if keyErr.Retryable != "" {
		notFoundDetail := prettyLockNotFoundKey(keyErr.GetRetryable())
		return kv.ErrTxnRetryable.GenWithStackByArgs(keyErr.GetRetryable() + " " + notFoundDetail)
	}
	if keyErr.Abort != "" {
		err := errors.Errorf("tikv aborts txn: %s", keyErr.GetAbort())
		logutil.BgLogger().Warn("2PC failed", zap.Error(err))
		return errors.Trace(err)
	}
	return errors.Errorf("unexpected KeyError: %s", keyErr.String())
}

func prettyLockNotFoundKey(rawRetry string) string {
	if !strings.Contains(rawRetry, "TxnLockNotFound") {
		return ""
	}
	start := strings.Index(rawRetry, "[")
	if start == -1 {
		return ""
	}
	rawRetry = rawRetry[start:]
	end := strings.Index(rawRetry, "]")
	if end == -1 {
		return ""
	}
	rawRetry = rawRetry[:end+1]
	var key []byte
	err := json.Unmarshal([]byte(rawRetry), &key)
	if err != nil {
		return ""
	}
	var buf bytes.Buffer
	prettyWriteKey(&buf, key)
	return buf.String()
}

func newWriteConflictError(conflict *pb.WriteConflict) error {
	var buf bytes.Buffer
	prettyWriteKey(&buf, conflict.Key)
	buf.WriteString(" primary=")
	prettyWriteKey(&buf, conflict.Primary)
	return kv.ErrWriteConflict.FastGenByArgs(conflict.StartTs, conflict.ConflictTs, buf.String())
}

func prettyWriteKey(buf *bytes.Buffer, key []byte) {
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		_, err1 := fmt.Fprintf(buf, "{tableID=%d, indexID=%d, indexValues={", tableID, indexID)
		if err1 != nil {
			logutil.BgLogger().Error("error", zap.Error(err1))
		}
		for _, v := range indexValues {
			_, err2 := fmt.Fprintf(buf, "%s, ", v)
			if err2 != nil {
				logutil.BgLogger().Error("error", zap.Error(err2))
			}
		}
		buf.WriteString("}}")
		return
	}

	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{tableID=%d, handle=%d}", tableID, handle)
		if err3 != nil {
			logutil.BgLogger().Error("error", zap.Error(err3))
		}
		return
	}

	mKey, mField, err := tablecodec.DecodeMetaKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{metaKey=true, key=%s, field=%s}", string(mKey), string(mField))
		if err3 != nil {
			logutil.Logger(context.Background()).Error("error", zap.Error(err3))
		}
		return
	}

	_, err4 := fmt.Fprintf(buf, "%#v", key)
	if err4 != nil {
		logutil.BgLogger().Error("error", zap.Error(err4))
	}
}
