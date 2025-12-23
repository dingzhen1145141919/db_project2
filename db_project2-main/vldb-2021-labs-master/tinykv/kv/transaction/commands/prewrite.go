package commands

import (
	"encoding/hex" // 用于字节数组转十六进制字符串，日志打印

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc" // MVCC 可写事务依赖
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"   // RPC 协议中的 Prewrite 请求/响应结构体
	"github.com/pingcap/log"                                  // 日志库
	"go.uber.org/zap"                                         // 日志字段封装
)

// Prewrite 表示一个 Prewrite 命令（事务预写阶段，第一阶段提交）
// 预写阶段包含事务的所有写入操作（无读取），如果所有键都能原子性写入且无冲突，则返回成功
// 若预写成功，客户端会发送 Commit 命令（第二阶段提交）
// 嵌入了 CommandBase（基础属性和方法）
type Prewrite struct {
	CommandBase        // 基础属性：context 和 startTs
	request *kvrpcpb.PrewriteRequest // Prewrite 请求的具体参数（突变、主键、TTL 等）
}

// NewPrewrite 创建一个 Prewrite 命令实例
// 参数：request - Prewrite 请求的 RPC 结构体
// 返回：初始化后的 Prewrite 命令
func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{
		CommandBase: CommandBase{
			context: request.Context,       // 请求上下文（region 元数据）
			startTs: request.StartVersion, // 事务启动时间戳
		},
		request: request, // 保存请求参数
	}
}

// PrepareWrites 执行 Prewrite 命令的写逻辑（核心实现部分）
// 实现 Command 接口的 PrepareWrites 方法，构建待写入的锁和数据
// 参数：txn - 可写 MVCC 事务，用于写入锁和数据
// 返回值：
// 1. interface{}: Prewrite 响应结果（kvrpcpb.PrewriteResponse）
// 2. error: 执行过程中的错误（如内部错误）
func (p *Prewrite) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 初始化 Prewrite 响应结构体
	response := new(kvrpcpb.PrewriteResponse)

	// 遍历请求中的所有突变（Mutation：键值对的操作，如 Put/Delete）
	for _, m := range p.request.Mutations {
		// 预写单个突变，返回键错误（如锁冲突）或内部错误
		keyError, err := p.prewriteMutation(txn, m)
		if keyError != nil {
			// 收集键错误，继续处理下一个突变（批量处理）
			response.Errors = append(response.Errors, keyError)
		} else if err != nil {
			// 内部错误，直接返回
			return nil, err
		}
	}

	// 返回响应结果、无错误
	return response, nil
}

// prewriteMutation 预写单个突变（核心逻辑，处理单个键的预写）
// 参数：
// - txn: 可写 MVCC 事务
// - mut: 单个突变（键、值、操作类型）
// 返回值：
// 1. *kvrpcpb.KeyError: 键相关的错误（如锁冲突、键被锁定），返回后继续处理其他键
// 2. error: 内部错误（如存储读取失败），返回后终止整个预写流程
func (p *Prewrite) prewriteMutation(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.KeyError, error) {
	key := mut.Key // 获取要预写的键
	// 打印调试日志：事务启动时间戳、要预写的键（十六进制字符串）
	log.Debug("prewrite key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	// ========== 回滚记录检查 ==========
	// 检查当前键是否有回滚记录，若有则直接返回Abort错误
	currentWrite, _, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	if currentWrite != nil && currentWrite.Kind == mvcc.WriteKindRollback {
		// 事务已回滚，拒绝预写
		keyError := &kvrpcpb.KeyError{
			Abort: "transaction has been rolled back",
		}
		return keyError, nil
	}

	// YOUR CODE HERE (lab2).
	// 实验二待实现：检查写冲突（核心逻辑）
	write, commitTs, err := txn.MostRecentWrite(key)
	if err != nil {
		// 内部错误（存储读取失败），返回第二个返回值
		return nil, err
	}
	// 判断是否存在写冲突：最新写记录的提交TS > 当前事务的StartTS
	if write != nil && commitTs > txn.StartTS {
		keyErr := &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,       // 当前事务的启动TS
				ConflictTs: commitTs,           // 冲突的提交TS
				Key:        key,                // 冲突的键
				Primary:    p.request.PrimaryLock, // 补全Primary字段
			},
		}
		// 返回键相关错误，继续处理其他键
		return keyErr, nil
	}

	// YOUR CODE HERE (lab2).
	// 实验二待实现：检查键是否被锁定（核心逻辑）
	existingLock, err := txn.GetLock(key)
	if err != nil {
		// 内部错误，返回第二个返回值
		return nil, err
	}
	// 如果锁存在，且不是当前事务的锁（被其他事务锁定）
	if existingLock != nil && existingLock.Ts != txn.StartTS {
		// 构造锁冲突错误：封装到kvrpcpb.KeyError的Locked字段
		keyErr := &kvrpcpb.KeyError{
			Locked: existingLock.Info(key), // 转换为kvrpcpb.LockInfo
		}
		// 返回键相关错误，继续处理其他键
		return keyErr, nil
	}
	// 如果是当前事务的锁（重复预写请求），直接跳过后续逻辑（忽略，不报错）
	if existingLock != nil && existingLock.Ts == txn.StartTS {
		// 预写成功，返回无错误
		return nil, nil
	}

	// YOUR CODE HERE (lab2).
	// 实验二待实现：写入锁和数值（核心逻辑）
	// 步骤3：构造预写锁并写入
	// 3.1 将RPC的操作类型（Put/Del）转换为本地的WriteKind
	writeKind := mvcc.WriteKindFromProto(mut.Op)
	// 3.2 构造mvcc.Lock结构体（包含主键、事务TS、TTL、操作类型）
	lock := &mvcc.Lock{
		Primary: p.request.PrimaryLock, // 事务的主键（由请求传入）
		Ts:      txn.StartTS,           // 当前事务的启动TS
		Ttl:     p.request.LockTtl,     // 锁的TTL（超时时间，由请求传入）
		Kind:    writeKind,             // 操作类型（Put/Del/Rollback）
	}
	// 3.3 写入锁到事务缓冲区
	txn.PutLock(key, lock)

	// 步骤4：写入数值（根据操作类型处理）
	switch mut.Op {
	case kvrpcpb.Op_Put:
		// Put操作：写入值到事务缓冲区
		txn.PutValue(key, mut.Value)
	case kvrpcpb.Op_Del:
		// Delete操作：删除值（标记为删除，后续Commit时生效）
		txn.DeleteValue(key)
	}

	// 预写成功，返回无错误
	return nil, nil
}

// WillWrite 返回 Prewrite 命令要写入的所有键列表（实现 Command 接口的 WillWrite 方法）
// 用于锁存器加锁，避免并发写入冲突
func (p *Prewrite) WillWrite() [][]byte {
	result := [][]byte{}
	// 遍历所有突变，收集要写入的键
	for _, m := range p.request.Mutations {
		result = append(result, m.Key)
	}
	return result
}