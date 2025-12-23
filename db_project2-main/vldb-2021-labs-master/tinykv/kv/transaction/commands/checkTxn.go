package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// CheckTxnStatus 表示检查事务状态的命令
// 主要用于处理事务超时、锁不存在等场景，执行回滚操作并写入回滚记录
type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest // 检查事务状态的请求参数
}

// NewCheckTxnStatus 创建CheckTxnStatus命令实例
// 参数：request - 检查事务状态的RPC请求结构体
// 返回：初始化后的CheckTxnStatus命令
func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context, // 请求上下文（包含region元数据等）
			startTs: request.LockTs,  // 事务的锁时间戳（即事务启动TS）
		},
		request: request, // 保存请求参数
	}
}

// PrepareWrites 执行检查事务状态的核心逻辑，处理锁超时、锁不存在等场景
// 参数：txn - 可写的MVCC事务实例
// 返回值：
// 1. interface{}: 检查事务状态的响应结果
// 2. error: 执行过程中的内部错误
func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 获取事务的主键（Primary Key），事务的锁状态主要通过主键判断
	key := c.request.PrimaryKey
	// 初始化响应结构体
	response := new(kvrpcpb.CheckTxnStatusResponse)

	// 第一步：获取主键对应的锁
	lock, err := txn.GetLock(key)
	if err != nil {
		// 读取锁时发生内部错误，直接返回
		return nil, err
	}

	// 第二步：如果锁存在且属于当前事务（锁的TS等于事务启动TS）
	if lock != nil && lock.Ts == txn.StartTS {
		// 判断锁是否超时：锁的物理时间 + TTL < 当前物理时间
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {
			// YOUR CODE HERE (lab2).
			// 锁已超时，执行回滚操作：使用mvcc.WriteKindRollback标记回滚记录
			// 提示：使用mvcc.MvccTxn提供的接口完成数据清理、回滚记录写入和锁删除

			// 打印日志：记录锁超时回滚的相关信息
			log.Info("checkTxnStatus rollback the primary lock as it's expired",
				zap.Uint64("lock.TS", lock.Ts),
				zap.Uint64("physical(lock.TS)", physical(lock.Ts)),
				zap.Uint64("txn.StartTS", txn.StartTS),
				zap.Uint64("currentTS", c.request.CurrentTs),
				zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))
			
			// 1. 清理预写的Put类型数据（Delete类型无需清理，因为本身就是删除）
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
			// 2. 写入回滚记录：标记该事务已回滚，防止后续预写操作
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, &write)
			// 3. 删除主键的锁：释放资源
			txn.DeleteLock(key)
			// 4. 设置响应动作：表示锁超时后执行了回滚
			response.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// 锁未超时，不执行任何操作
			response.Action = kvrpcpb.Action_NoAction
			// 返回锁的剩余TTL，供客户端判断是否需要再次检查
			response.LockTtl = lock.Ttl
		}

		return response, nil
	}

	// 第三步：锁不存在时，检查主键的最新写记录（提交/回滚）
	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}

	// 情况1：没有任何写记录（预写请求丢失）
	if existingWrite == nil {
		// YOUR CODE HERE (lab2).
		// 锁从未存在过，需要写入回滚记录，防止过期的预写命令执行成功
		// 注意：设置正确的response.Action（参考kvrpcpb.Action_xxx枚举）

		// 1. 写入回滚记录：标记该事务已回滚
		write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, &write)
		// 2. 设置响应动作：表示锁不存在并写入了回滚记录
		response.Action = kvrpcpb.Action_LockNotExistRollback
		return response, nil
	}

	// 情况2：已有回滚记录（事务已回滚）
	if existingWrite.Kind == mvcc.WriteKindRollback {
		// 无需操作，直接返回无动作
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}

	// 情况3：已有提交记录（事务已提交）
	// 设置响应的提交版本，供客户端确认事务状态
	response.CommitVersion = commitTs
	response.Action = kvrpcpb.Action_NoAction
	return response, nil
}

// physical 计算时间戳的物理时间部分（将逻辑时间戳右移，提取物理时间）
// 参数：ts - 完整的时间戳（物理时间+逻辑时间）
// 返回：时间戳的物理时间部分
func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

// WillWrite 返回当前命令需要写入的键列表（仅主键）
// 用于锁存器加锁，避免并发写入冲突
func (c *CheckTxnStatus) WillWrite() [][]byte {
	return [][]byte{c.request.PrimaryKey}
}