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
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchKeys) error
	String() string
}

type actionPrewrite struct{}
type actionCommit struct{}
type actionCleanup struct{}

var (
	_ twoPhaseCommitAction = actionPrewrite{}
	_ twoPhaseCommitAction = actionCommit{}
	_ twoPhaseCommitAction = actionCleanup{}
)

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionCommit) String() string {
	return "commit"
}

func (actionCleanup) String() string {
	return "cleanup"
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store     *TinykvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*mutationEx
	lockTTL   uint64
	commitTS  uint64
	connID    uint64 // connID is used for log.
	cleanWg   sync.WaitGroup
	txnSize   int

	primaryKey []byte

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
}

// batchExecutor is txn controller providing rate control like utils
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *rateLimit           // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *Backoffer           // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

type mutationEx struct {
	pb.Mutation
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
	}, nil
}

// The txn mutations buffered in `txn.us` before commit.
// Your task is to convert buffer to KV mutations in order to execute as a transaction.
// This function runs before commit execution
func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var (
		keys    [][]byte
		size    int
		putCnt  int
		delCnt  int
		lockCnt int
	)
	mutations := make(map[string]*mutationEx)
	txn := c.txn
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		// In membuffer, there are 2 kinds of mutations
		//   put: there is a new value in membuffer
		//   delete: there is a nil value in membuffer
		// You need to build the mutations from membuffer here
		if len(v) > 0 {
			// `len(v) > 0` means it's a put operation.
			// YOUR CODE HERE (lab3).
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:    pb.Op_Put,
					Key:   k,
					Value: v,
				},
			}
			putCnt++
		} else {
			// `len(v) == 0` means it's a delete operation.
			// YOUR CODE HERE (lab3).
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Del,
					Key: k,
				},
			}
			delCnt++
		}
		// Update the keys array and statistic information
		// YOUR CODE HERE (lab3).
		keys = append(keys, k)
		size += len(k) + len(v)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// In prewrite phase, there will be a lock for every key
	// If the key is already locked but is not updated, you need to write a lock mutation for it to prevent lost update
	// Don't forget to update the keys array and statistic information
	for _, lockKey := range txn.lockKeys {
		// YOUR CODE HERE (lab3).
		_, ok := mutations[string(lockKey)]
		if !ok {
			// Key is locked but not modified, add a Lock mutation
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
			}
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	c.txnSize = size

	if size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	c.keys = keys
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.keys[0]
	}
	return c.primaryKey
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > maxLockTTL {
			lockTTL = maxLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

// doActionOnKeys groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
// There are three kind of actions which implement the twoPhaseCommitAction interface.
// actionPrewrite prewrites a transaction
// actionCommit commits a transaction
// actionCleanup rollbacks a transaction
// This function split the keys by region and parallel execute the batches in a transaction using given action
func (c *twoPhaseCommitter) doActionOnKeys(bo *Backoffer, action twoPhaseCommitAction, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	var batches []batchKeys
	var sizeFunc = c.keySize
	if _, ok := action.(actionPrewrite); ok {
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for region, keys := range groups {
				c.regionTxnSize[region.id] = len(keys)
			}
		}
		sizeFunc = c.keyValueSize
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	_, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	if firstIsPrimary && (actionIsCommit || actionIsCleanup) {
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if actionIsCommit {
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potential data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackoffer(context.Background(), CommitMaxBackoff).WithVars(c.txn.vars)
		go func() {
			e := c.doActionOnBatches(secondaryBo, action, batches)
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batches)
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchKeys) error {
	if len(batches) == 0 {
		return nil
	}

	if len(batches) == 1 {
		e := action.handleSingleBatch(c, bo, batches[0])
		if e != nil {
			logutil.BgLogger().Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
		}
		return errors.Trace(e)
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > 16 {
		rateLim = 16
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	err := batchExecutor.process(batches)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key []byte) int {
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

func (c *twoPhaseCommitter) keySize(key []byte) int {
	return len(key)
}

// You need to build the prewrite request in this function
// All keys in a batch are in the same region
func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchKeys) *tikvrpc.Request {
	// 声明预写请求对象，后续将填充具体参数
	var req *pb.PrewriteRequest

	// 构建预写请求的核心逻辑：
	// 1. 从输入的batch中提取mutation（数据变更）信息
	// 2. 使用twoPhaseCommitter的primary方法确保主锁key不为空
	// 3. 填充预写请求的必要参数（变更数据、主锁、版本号、锁过期时间等）

	// 初始化mutation切片，容量为batch中key的数量，减少内存分配
	mutations := make([]*pb.Mutation, 0, len(batch.keys))
	// 遍历批次中的所有key，提取对应的mutation信息
	for _, key := range batch.keys {
		// 从提交器的mutation映射中获取当前key对应的变更信息
		mut := c.mutations[string(key)]
		// 将mutation添加到切片中，用于填充预写请求
		mutations = append(mutations, &mut.Mutation)
	}

	// 初始化预写请求对象，填充核心参数
	req = &pb.PrewriteRequest{
		Mutations:    mutations,    // 当前批次的所有数据变更信息
		PrimaryLock:  c.primary(), // 主锁key（通过primary方法确保非空）
		StartVersion: c.startTS,   // 事务的起始版本号（时间戳）
		LockTtl:      c.lockTTL,   // 锁的过期时间（TTL），控制锁的存活时长
	}

	// 将预写请求封装为TiKV RPC请求并返回（使用空的Context上下文）
	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, pb.Context{})
}

// handleSingleBatch prewrites a batch of keys
func (actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// 步骤1：调用构建方法，生成当前批次的预写请求
	req := c.buildPrewriteRequest(batch)

	// 循环处理：预写操作可能因区域错误、锁冲突等原因失败，需要重试
	for {
		// 步骤2：发送预写请求到TiKV节点
		// 参数说明：bo（退避器）、req（预写请求）、batch.region（目标Region）、readTimeoutShort（短超时时间）
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			// 发送请求失败，包装并返回错误
			return errors.Trace(err)
		}

		// 步骤3：提取响应中的区域错误（RegionError）
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}

		// 处理区域错误（比如Region缓存失效、Region分裂/迁移等）
		if regionErr != nil {
			// 背景说明：Region信息是从本地缓存读取的，因此需要处理缓存失效的情况
			// 步骤3.1：执行退避策略（BoRegionMiss类型），等待后重试
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			// 步骤3.2：重新拆分key并再次执行预写（因为Region可能已分裂，原批次的key可能归属不同Region）
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}

		// 步骤4：检查响应体是否为空（空响应表示TiKV返回异常）
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}

		// 步骤5：将响应体断言为预写响应类型，提取具体结果
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()

		// 步骤6：如果没有key级别的错误，说明预写成功，返回nil
		if len(keyErrs) == 0 {
			return nil
		}

		// 步骤7：处理key级别的错误（主要是锁冲突错误）
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// 步骤7.1：从key错误中提取锁信息（这些锁是其他事务遗留的）
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			// 记录日志：预写过程中遇到的锁信息
			logutil.BgLogger().Debug("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			// 将锁添加到切片，后续统一处理
			locks = append(locks, lock)
		}

		// 背景说明：预写时若遇到其他事务遗留的重叠锁，TiKV会返回key错误。
		// 这些事务的状态是未知的（可能提交、可能回滚），需要解析锁的状态并处理。
		// 步骤8：解析锁的状态并解决锁冲突（ResolveLocks会检查锁所属事务的状态，然后释放或保留锁）
		// 参数说明：bo（退避器）、0（callerStartTS，设为0表示不更新minCommitTS）、locks（待解析的锁列表）
		msBeforeExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return errors.Trace(err)
		}

		// 步骤9：如果锁还未过期，执行退避策略，等待锁过期后重试预写
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}

		// 循环继续：退避后重新发送预写请求
	}
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Only set undeterminedErr if it's not already set
	// This preserves the original error (e.g., RPC timeout)
	if c.mu.undeterminedErr == nil {
		c.mu.undeterminedErr = err
	}
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

// actionCommit 是两阶段提交中**提交阶段**的操作类型（实现了统一的批次处理接口）
// actionCleanup 是事务失败后**回滚清理阶段**的操作类型（清理预写阶段的锁和临时数据）

// handleSingleBatch 处理单个批次key的提交（Commit）操作。
// 函数说明：遵循actionPrewrite.handleSingleBatch的逻辑，构建提交请求并发送到TiKV，
// 处理响应中的区域错误、空响应等异常，同时处理主锁key提交的不确定性问题。
// 参数：
//   c: 两阶段提交器实例，提供事务上下文（起始版本、提交版本、主锁key等）
//   bo: 用于重试和退避的Backoffer对象
//   batch: 包含同一Region内待提交key的批次对象
// 返回值：
//   error: 执行过程中出现的错误（若有）
func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// 参考actionPrewrite.handleSingleBatch的逻辑，构建提交请求

	var resp *tikvrpc.Response // 存储TiKV的响应结果
	var err error              // 存储执行过程中的错误
	// 创建区域请求发送器：用于向指定Region的TiKV节点发送RPC请求
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)

	// ========== 构建并发送提交请求（核心业务逻辑） ==========
	// YOUR CODE HERE (lab3).
	req := &pb.CommitRequest{
		StartVersion:  c.startTS,  // 事务的起始版本号（预写阶段的版本，与PrewriteRequest一致）
		Keys:          batch.keys, // 当前批次中待提交的key列表（同一Region）
		CommitVersion: c.commitTS, // 事务的提交版本号（TiKV的MVCC最终版本）
	}
	// 发送提交请求到TiKV：封装为Commit类型的RPC请求，指定目标Region和短超时时间
	resp, err = sender.SendReq(bo, tikvrpc.NewRequest(tikvrpc.CmdCommit, req, pb.Context{}), batch.region, readTimeoutShort)
	// 记录日志：调试提交请求的响应是否为空
	logutil.BgLogger().Debug("actionCommit handleSingleBatch", zap.Bool("nil response", resp == nil))

	// ========== 处理主锁key提交的不确定性问题（关键业务逻辑） ==========
	// 背景说明：如果提交主锁key的请求未能收到响应，无法确定事务是否提交成功。
	// 此时不能直接声明提交完成（可能导致数据丢失），也不能抛出错误（上层重试可能导致主键重复）。
	// 最佳方案：标记不确定性错误，让上层断开对应MySQL客户端的连接。
	// 判断当前批次是否包含主锁key（批次第一个key与主锁key一致）
	isPrimary := bytes.Equal(batch.keys[0], c.primary())
	// 若是主锁key且发生RPC错误（未收到响应），设置不确定性错误
	if isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(terror.ErrResultUndetermined)
	}

	// ========== 故障注入：模拟主锁key提交后，从锁key提交失败的场景（测试用） ==========
	failpoint.Inject("mockFailAfterPK", func() {
		// 仅当不是主锁key批次时，模拟提交失败
		if !isPrimary {
			err = errors.New("commit secondary keys error")
		}
	})

	// ========== 处理请求发送过程中的错误 ==========
	if err != nil {
		// 若是主锁key且已存在不确定性错误，返回该错误（优先级更高）
		if isPrimary && c.getUndeterminedErr() != nil {
			return errors.Trace(c.getUndeterminedErr())
		}
		// 否则返回原始错误
		return errors.Trace(err)
	}

	// ========== 处理TiKV响应，参考actionPrewrite.handleSingleBatch的逻辑 ==========
	// YOUR CODE HERE (lab3).
	// 情况1：响应为空（可能是超时重试导致）
	if resp == nil {
		// 若是主锁key且存在不确定性错误，返回该错误
		if isPrimary && c.getUndeterminedErr() != nil {
			return errors.Trace(c.getUndeterminedErr())
		}
		// 否则返回响应体缺失错误
		return errors.Trace(ErrBodyMissing)
	}

	// 情况2：提取响应中的区域错误（Region缓存失效、分裂/迁移等）
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// 执行退避策略（BoRegionMiss类型），等待后重试
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// 若是主锁key且存在不确定性错误，不再重试（无法确定提交状态）
		if isPrimary && c.getUndeterminedErr() != nil {
			return errors.Trace(c.getUndeterminedErr())
		}
		// 重新拆分key并执行提交操作（Region可能已分裂）
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}

	// 情况3：响应体为空
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}

	// 情况4：解析提交响应，处理业务错误
	commitResp := resp.Resp.(*pb.CommitResponse)
	if commitResp.Error != nil {
		return errors.Errorf("commit failed: %v", commitResp.Error)
	}

	// ========== 标记事务为已提交状态（关键：主锁key批次成功后触发） ==========
	// 加锁保护：避免并发修改事务状态
	c.mu.Lock()
	defer c.mu.Unlock()
	// 业务规则：包含主锁key的批次总是第一个被处理，因此收到第一个成功响应时，标记事务已提交
	c.mu.committed = true

	return nil
}

// handleSingleBatch 处理单个批次key的回滚清理（Cleanup/BatchRollback）操作。
// 函数说明：遵循actionPrewrite.handleSingleBatch的逻辑，构建批量回滚请求并发送到TiKV，
// 清理预写阶段的锁和临时数据，处理响应中的异常。
// 参数：
//   c: 两阶段提交器实例，提供事务上下文（起始版本等）
//   bo: 用于重试和退避的Backoffer对象
//   batch: 包含同一Region内待回滚key的批次对象
// 返回值：
//   error: 执行过程中出现的错误（若有）
func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// 参考actionPrewrite.handleSingleBatch的逻辑，构建回滚请求

	// ========== 构建并发送批量回滚请求（核心业务逻辑） ==========
	// YOUR CODE HERE (lab3).
	req := &pb.BatchRollbackRequest{
		StartVersion: c.startTS, // 事务的起始版本号（与预写阶段一致，用于定位预写的临时数据）
		Keys:         batch.keys, // 当前批次中待回滚的key列表（同一Region）
	}
	// 创建区域请求发送器
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	// 发送批量回滚请求到TiKV：封装为BatchRollback类型的RPC请求
	resp, err := sender.SendReq(bo, tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, req, pb.Context{}), batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}

	// ========== 处理TiKV响应，参考actionPrewrite.handleSingleBatch的逻辑 ==========
	// YOUR CODE HERE (lab3).
	// 步骤1：提取响应中的区域错误
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// 执行退避策略（BoRegionMiss类型）
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// 重新拆分key并执行回滚操作
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}

	// 步骤2：检查响应体是否为空
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}

	// 步骤3：解析回滚响应，处理业务错误
	rollbackResp := resp.Resp.(*pb.BatchRollbackResponse)
	if rollbackResp.Error != nil {
		return errors.Errorf("batch rollback failed: %v", rollbackResp.Error)
	}

	return nil
}
func (c *twoPhaseCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionPrewrite{}, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCommit{}, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCleanup{}, keys)
}

// execute executes the two-phase commit protocol.
// Prewrite phase:
//		1. Split keys by region -> batchKeys
// 		2. Prewrite all batches with transaction's start timestamp
// Commit phase:
//		1. Get the latest timestamp as commit ts
//      2. Check if the transaction can be committed(schema change during execution will fail the transaction)
//		3. Commit the primary key
//		4. Commit the secondary keys
// Cleanup phase:
//		When the transaction is unavailable to successfully committed,
//		transaction will fail and cleanup phase would start.
//		Cleanup phase will rollback a transaction.
// 		1. Cleanup primary key
// 		2. Cleanup secondary keys
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			c.cleanWg.Add(1)
			go func() {
			cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
			cleanupBo := NewBackoffer(cleanupKeysCtx, cleanupMaxBackoff).WithVars(c.txn.vars)
			logutil.BgLogger().Debug("cleanupBo", zap.Bool("nil", cleanupBo == nil))
			// cleanup phase
			// YOUR CODE HERE (lab3).
			err := c.cleanupKeys(cleanupBo, c.keys)
			if err != nil {
				logutil.Logger(cleanupKeysCtx).Info("2PC cleanup failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
			}
			c.cleanWg.Done()
			}()
		}
		c.txn.commitTS = c.commitTS
	}()

	// prewrite phase
	prewriteBo := NewBackoffer(ctx, PrewriteMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("prewriteBo", zap.Bool("nil", prewriteBo == nil))
	// YOUR CODE HERE (lab3).
	err = c.prewriteKeys(prewriteBo, c.keys)
	if err != nil {
		logutil.Logger(ctx).Warn("2PC prewrite failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// commit phase
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// check commitTS
	if commitTS <= c.startTS {
		err = errors.Errorf("conn %d Invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return errors.Trace(err)
	}
	c.commitTS = commitTS
	if err = c.checkSchemaValid(); err != nil {
		return errors.Trace(err)
	}

	if c.store.oracle.IsExpired(c.startTS, kv.MaxTxnTimeUse) {
		err = errors.Errorf("conn %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("commitBo", zap.Bool("nil", commitBo == nil))
	// Commit the transaction with `commitBo`.
	// If there is an error returned by commit operation, you should check if there is an undetermined error before return it.
	// Undetermined error should be returned if exists, and the database connection will be closed.
	// YOUR CODE HERE (lab3).
	err = c.commitKeys(commitBo, c.keys)
	if err != nil {
		if c.getUndeterminedErr() != nil {
			logutil.Logger(ctx).Error("2PC commit has undetermined error", zap.Error(c.getUndeterminedErr()), zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(c.getUndeterminedErr())
		}
		logutil.Logger(ctx).Warn("2PC commit failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	return nil
}

type schemaLeaseChecker interface {
	Check(txnTS uint64) error
}

// checkSchemaValid checks if there are schema changes during the transaction execution(from startTS to commitTS).
// Schema change in a transaction is not allowed.
func (c *twoPhaseCommitter) checkSchemaValid() error {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if ok {
		err := checker.Check(c.commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

// batchKeys is a batch of keys in the same region.
type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchBySize appends keys to []batchKeys. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, time.Duration(1 * time.Millisecond)}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = newRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchKeys) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.getToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.putToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
			}()
		} else {
			logutil.Logger(batchExe.backoffer.ctx).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchKeys) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.ctx).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	backoffer := batchExe.backoffer
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", batchExe.committer.connID),
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches to cancel other actions",
					zap.Uint64("conn", batchExe.committer.connID),
					zap.Stringer("action type", batchExe.action),
					zap.Uint64("txnStartTS", batchExe.committer.startTS))
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)

	return err
}
