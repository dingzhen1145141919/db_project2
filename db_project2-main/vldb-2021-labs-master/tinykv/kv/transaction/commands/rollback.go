package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Rollback 表示批量回滚事务的命令
// 用于回滚事务的所有键，清理预写数据、写入回滚记录并删除锁
type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest // 批量回滚的请求参数
}

// NewRollback 创建Rollback命令实例
// 参数：request - 批量回滚的RPC请求结构体
// 返回：初始化后的Rollback命令
func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,    // 请求上下文（包含region元数据等）
			startTs: request.StartVersion, // 事务启动时间戳
		},
		request: request, // 保存请求参数
	}
}

// PrepareWrites 执行批量回滚的核心逻辑，遍历所有键并逐个回滚
// 参数：txn - 可写的MVCC事务实例
// 返回值：
// 1. interface{}: 批量回滚的响应结果
// 2. error: 执行过程中的内部错误
func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 初始化批量回滚的响应结构体
	response := new(kvrpcpb.BatchRollbackResponse)

	// 遍历请求中的所有键，逐个执行回滚
	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			// 某个键回滚失败，直接返回错误
			return resp, err
		}
	}
	return response, nil
}

// rollbackKey 回滚单个键的核心逻辑
// 参数：
//   key - 要回滚的键
//   txn - 可写的MVCC事务实例
//   response - 批量回滚的响应结构体（用于设置错误信息）
// 返回值：
//   1. interface{}: 响应结果（若有错误）
//   2. error: 执行过程中的内部错误
func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	// 第一步：获取键对应的锁
	lock, err := txn.GetLock(key)
	if err != nil {
		// 读取锁时发生内部错误，直接返回
		return nil, err
	}

	// 打印日志：记录回滚键的相关信息（事务TS、键的十六进制表示）
	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	// 第二步：锁不存在或不属于当前事务（处理过期请求或预写丢失场景）
	if lock == nil || lock.Ts != txn.StartTS {
		// 检查键的最新写记录（提交/回滚）
		// ts: 写记录的提交时间戳（此处未使用，保留以兼容原有逻辑）
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// 逻辑说明：
		// 1. 无写记录：预写请求丢失，写入回滚记录
		// 2. 有回滚记录：事务已回滚，无需操作
		// 3. 有提交记录：事务已提交，返回错误（客户端不应该同时发送提交和回滚请求）

		// 情况1：没有任何写记录（预写请求丢失）
		if existingWrite == nil {
			// YOUR CODE HERE (lab2).
			// 无写记录时，插入回滚记录（使用mvcc.WriteKindRollback标记类型）

			// 写入回滚记录：标记该事务已回滚，防止后续预写操作
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, &write)
			return nil, nil
		} else {
			// 情况2：已有回滚记录（事务已回滚）
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// 无需操作，直接返回成功
				return nil, nil
			}

			// 情况3：已有提交记录（事务已提交）
			// 构造键错误：提示键已提交，客户端操作异常
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			// 使用反射设置响应的错误字段（兼容原有代码的反射逻辑）
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	// 第三步：锁存在且属于当前事务（正常回滚流程）
	// 1. 清理预写的Put类型数据（Delete类型无需清理）
	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}

	// 2. 写入回滚记录：标记该事务已回滚
	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	// 3. 删除键的锁：释放资源
	txn.DeleteLock(key)

	// 回滚成功，返回无错误
	return nil, nil
}

// WillWrite 返回当前命令需要写入的键列表（批量回滚的所有键）
// 用于锁存器加锁，避免并发写入冲突
func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}