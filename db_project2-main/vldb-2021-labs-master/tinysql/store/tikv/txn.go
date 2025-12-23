// Copyright 2016 PingCAP, Inc.
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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *TinykvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	lockKeys  [][]byte
	lockedMap map[string]struct{}
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *kv.Variables
	committer *twoPhaseCommitter

	valid bool
	dirty bool
}

func newTiKVTxn(store *TinykvStore) (*tikvTxn, error) {
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTikvTxnWithStartTS(store, startTS)
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *TinykvStore, startTS uint64) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
		lockedMap: map[string]struct{}{},
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      kv.DefaultVars,
	}, nil
}

func (txn *tikvTxn) SetVars(vars *kv.Variables) {
	txn.vars = vars
	txn.snapshot.vars = vars
}

// SetCap sets the transaction's MemBuffer capability, to reduce memory allocations.
func (txn *tikvTxn) SetCap(cap int) {
	txn.us.SetCap(cap)
}

// Reset reset tikvTxn's membuf.
func (txn *tikvTxn) Reset() {
	txn.us.Reset()
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = txn.store.CheckVisibility(txn.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txn.setCnt++

	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
	switch opt {
	case kv.SyncLog:
		txn.snapshot.syncLog = val.(bool)
	case kv.KeyOnly:
		txn.snapshot.keyOnly = val.(bool)
	case kv.SnapshotTS:
		txn.snapshot.setSnapshotTS(val.(uint64))
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

// Commit 是分布式事务提交的入口方法，实现 Percolator 协议的两阶段提交
// 核心流程：
//  1. 检查事务是否有效
//  2. 创建两阶段提交器（twoPhaseCommitter）
//  3. 准备要提交的变更数据（mutations）
//  4. 执行两阶段提交：
//     4.1 Prewrite 阶段（预写，写入数据和锁）
//     4.2 Commit 阶段（提交，清理锁，使数据可见）
//     4.3 若提交失败，执行 Cleanup 阶段（回滚，清理预写的数据和锁）
func (txn *tikvTxn) Commit(ctx context.Context) error {
	// 检查事务是否有效（已关闭/已提交/已回滚则返回错误）
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	// 无论提交成功或失败，最终都关闭事务（标记为无效）
	defer txn.close()

	// 注入失败点：模拟提交错误（用于测试异常场景）
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	// connID 用于日志记录，标识当前数据库连接（从上下文获取）
	var connID uint64
	val := ctx.Value(sessionctx.ConnID)
	if val != nil {
		connID = val.(uint64)
	}

	var err error
	// 获取或创建两阶段提交器
	committer := txn.committer
	if committer == nil {
		// 初始化两阶段提交器，关联当前事务和连接 ID
		committer, err = newTwoPhaseCommitter(txn, connID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// 初始化要提交的 Key 和变更数据（从联合存储中提取脏数据）
	if err := committer.initKeysAndMutations(); err != nil {
		return errors.Trace(err)
	}
	// 如果没有要提交的 Key，直接返回（空事务）
	if len(committer.keys) == 0 {
		return nil
	}

	// 执行两阶段提交的核心逻辑
	err = committer.execute(ctx)
	return errors.Trace(err)
}
func (txn *tikvTxn) close() {
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	txn.close()
	logutil.BgLogger().Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	return nil
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	// Exclude keys that are already locked.
	keys := make([][]byte, 0, len(keysInput))
	txn.mu.Lock()
	for _, key := range keysInput {
		if _, ok := txn.lockedMap[string(key)]; !ok {
			keys = append(keys, key)
		}
	}
	txn.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}
	txn.mu.Lock()
	txn.lockKeys = append(txn.lockKeys, keys...)
	for _, key := range keys {
		txn.lockedMap[string(key)] = struct{}{}
	}
	txn.dirty = true
	txn.mu.Unlock()
	return nil
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	return txn.valid
}

func (txn *tikvTxn) Len() int {
	return txn.us.Len()
}

func (txn *tikvTxn) Size() int {
	return txn.us.Size()
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.us.GetMemBuffer()
}
