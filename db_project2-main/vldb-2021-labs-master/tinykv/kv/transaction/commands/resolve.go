package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ResolveLock 表示解析事务遗留锁的命令
// 作用：根据事务的最终状态（提交/回滚），对事务遗留的所有锁执行提交或回滚操作
// 这是TinyKV处理分布式事务遗留锁的核心命令，用于清理事务异常时的残留资源
type ResolveLock struct {
	CommandBase               // 命令基类，包含上下文（context）和事务启动时间戳（startTs）等公共字段
	request  *kvrpcpb.ResolveLockRequest // 解析锁的请求参数，包含事务最终状态、提交版本等关键信息
	keyLocks []mvcc.KlPair               // 事务遗留的锁列表（KlPair：键+锁的组合体）
}

// NewResolveLock 创建ResolveLock命令实例
// 参数：request - 解析锁的RPC请求结构体
// 返回：初始化后的ResolveLock命令对象
func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{
		CommandBase: CommandBase{
			context: request.Context,  // 请求上下文，包含Region元数据等信息
			startTs: request.StartVersion, // 事务的启动时间戳，用于标识唯一事务
		},
		request: request, // 保存请求参数
	}
}

// PrepareWrites 执行解析锁的核心写逻辑：遍历所有遗留锁，根据事务状态执行提交或回滚
// 参数：txn - 可写的MVCC事务实例，用于执行写操作（写入提交/回滚记录、删除锁等）
// 返回值：
// 1. interface{}: 解析锁的响应结果（ResolveLockResponse）
// 2. error: 执行过程中的内部错误（如IO错误、锁状态异常等）
func (rl *ResolveLock) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 获取事务的提交版本：仅当事务状态为提交时，该值有效；回滚时该值为0
	commitTs := rl.request.CommitVersion
	// 初始化响应结构体：用于返回解析锁的执行结果（目前仅为空结构体，无额外字段）
	response := new(kvrpcpb.ResolveLockResponse)

	// 打印日志：记录解析锁的关键信息（事务锁TS、锁的数量、提交版本），辅助调试
	log.Info("There keys to resolve",
		zap.Uint64("lockTS", txn.StartTS),
		zap.Int("number", len(rl.keyLocks)),
		zap.Uint64("commit_ts", commitTs))
	// 原有占位panic：提示该功能尚未实现，后续需要删除
	// panic("ResolveLock is not implemented yet")

	// 遍历事务的所有遗留锁，逐个处理每个键的锁
	for _, kl := range rl.keyLocks {
		// YOUR CODE HERE (lab2).
		// 核心逻辑提示：
		// 1. 如果事务已提交，调用commitKey函数提交该键的锁
		// 2. 如果事务已回滚，调用rollbackKey函数回滚该键的锁
		// 辅助工具：commitKey和rollbackKey函数已提供，可直接调用
		// 打印调试日志：记录当前正在解析的键（十六进制格式，便于查看）
		log.Debug("resolve key", zap.String("key", hex.EncodeToString(kl.Key)))
		// 步骤1：判断事务状态，执行对应的操作
		var resp interface{}
		var err error
		if commitTs > 0 {
			// 事务已提交：调用commitKey提交单个键
			// 参数：键、提交版本、可写事务、响应结构体
			resp, err = commitKey(kl.Key, commitTs, txn, response)
		} else {
			// 事务已回滚：调用rollbackKey回滚单个键
			// 参数：键、可写事务、响应结构体
			resp, err = rollbackKey(kl.Key, txn, response)
		}

		// 步骤2：处理操作结果，若有错误则立即返回
		if err != nil {
			return nil, err
		}
		if resp != nil {
			// 有响应（表示存在错误信息），返回该响应
			return resp, nil
		}
	}

	// 返回响应结果，无错误
	return response, nil
}

// WillWrite 返回当前命令需要写入的键列表
// 注：此处暂时返回nil，因为锁列表是在Read阶段动态获取的，后续可根据实际情况调整（或保持nil）
func (rl *ResolveLock) WillWrite() [][]byte {
	return nil
}

// Read 执行解析锁的读逻辑：获取当前事务的所有遗留锁
// 这是Command接口的Read方法，在PrepareWrites之前执行，用于收集需要处理的锁信息
// 参数：txn - 只读的MVCC事务实例，用于读取锁信息（不执行写操作）
// 返回值：
// 1. interface{}: 暂返回nil（无额外读结果）
// 2. [][]byte: 事务遗留锁对应的键列表（用于后续写操作的锁存器加锁）
// 3. error: 读取锁过程中的内部错误
func (rl *ResolveLock) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	// 设置只读事务的启动TS为当前事务的启动TS，用于筛选该事务的锁
	txn.StartTS = rl.request.StartVersion
	// 获取该事务的所有遗留锁（mvcc包提供的工具函数）
	keyLocks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		// 读取锁失败时，返回错误
		return nil, nil, err
	}
	// 将获取到的锁列表保存到ResolveLock结构体中，供后续PrepareWrites使用
	rl.keyLocks = keyLocks

	// 收集所有锁对应的键，返回给上层（用于锁存器加锁，避免并发冲突）
	keys := [][]byte{}
	for _, kl := range keyLocks {
		keys = append(keys, kl.Key)
	}
	return nil, keys, nil
}