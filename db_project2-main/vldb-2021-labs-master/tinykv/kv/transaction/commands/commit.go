package commands

import (
	"encoding/hex" // 字节数组转十六进制字符串，日志打印
	"fmt"          // 格式化字符串
	"reflect"      // 反射，用于动态设置响应中的错误（实验中可替换为直接赋值）

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc" // MVCC 可写事务依赖
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"   // RPC 协议中的 Commit 请求/响应结构体
	"github.com/pingcap/log"                                  // 日志库
	"go.uber.org/zap"                                         // 日志字段封装
)

// Commit 表示一个 Commit 命令（事务提交阶段，第二阶段提交）
// 嵌入了 CommandBase（基础属性和方法）
type Commit struct {
	CommandBase        // 基础属性：context 和 startTs
	request *kvrpcpb.CommitRequest // Commit 请求的具体参数（键、提交版本等）
}

// NewCommit 创建一个 Commit 命令实例
// 参数：request - Commit 请求的 RPC 结构体
// 返回：初始化后的 Commit 命令
func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,       // 请求上下文（region 元数据）
			startTs: request.StartVersion, // 事务启动时间戳
		},
		request: request, // 保存请求参数
	}
}

// PrepareWrites 执行 Commit 命令的写逻辑（核心实现部分）
// 实现 Command 接口的 PrepareWrites 方法，构建待写入的写记录并释放锁
// 参数：txn - 可写 MVCC 事务，用于写入写记录和删除锁
// 返回值：
// 1. interface{}: Commit 响应结果（kvrpcpb.CommitResponse）
// 2. error: 执行过程中的错误（如提交版本无效、内部错误）
func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion // 获取事务提交时间戳

	// YOUR CODE HERE (lab2).
	// 实验二待实现：检查提交时间戳是否有效（核心逻辑）
	// 要点：提交时间戳必须大于事务启动时间戳，否则返回异常错误
	// panic("PrepareWrites is not implemented for commit command")
	if commitTs <= c.StartTs() {
		// 提交TS无效，返回错误（终止整个提交流程）
		return nil, fmt.Errorf("commitTs %d is not greater than startTs %d", commitTs, c.StartTs())
	}

	// 初始化 Commit 响应结构体
	response := new(kvrpcpb.CommitResponse)

	// 遍历请求中的所有键，逐个提交
	for _, k := range c.request.Keys {
		// 提交单个键，返回响应或错误
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	// 所有键提交成功，返回响应结果、无错误
	return response, nil
}

// commitKey 提交单个键（核心逻辑，处理单个键的提交）
// 参数：
// - key: 要提交的键
// - commitTs: 提交时间戳
// - txn: 可写 MVCC 事务
// - response: Commit 响应结构体（用于设置错误信息）
// 返回值：
// 1. interface{}: 响应结构体（如果有错误）
// 2. error: 内部错误（如存储读取失败）
func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	// 获取键对应的锁（预写阶段写入的锁）
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// 原代码的 panic 表示该方法尚未实现
	// panic("commitKey is not implemented yet")

	// 打印调试日志：事务启动时间戳、提交时间戳、要提交的键（十六进制字符串）
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))

	// 检查锁是否存在，且是否属于当前事务（删除重复的注释）
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab2).
		// 实验二待实现：处理锁不存在或不属于当前事务的情况（核心逻辑）
		// 要点：
		// 1. 检查该键是否有提交/回滚记录（已提交或已回滚，属于重复请求，忽略）
		// 2. 如果没有任何记录，返回锁未找到错误
		// 3. 处理重复提交请求（已提交则忽略，已回滚则返回错误）
		// ===== 仅修改这里，补全逻辑和闭合括号 =====
		// 步骤1：获取当前键的最新写记录（提交/回滚）
		currentWrite, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// 步骤2：处理已有记录的情况
		if currentWrite != nil {
			if currentWrite.Kind == mvcc.WriteKindRollback {
				// 事务已回滚，用反射设置错误（保留原有反射逻辑）
				respValue := reflect.ValueOf(response)
				keyError := &kvrpcpb.KeyError{Abort: "transaction has been rolled back"}
				reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
				return response, nil
			} else if currentWrite.StartTS == txn.StartTS {
				// 重复提交（已提交），忽略，返回无错误
				return nil, nil
			}
		}

		// 步骤3：无锁且无记录，返回锁未找到错误（补全实验要求的要点2）
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %x", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
		// ===== 新增：闭合 if 块的 } =====
	}

	// 构建写记录（标记该键的事务已提交）
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	// 将写记录写入存储（键 + 提交时间戳 作为索引）
	txn.PutWrite(key, commitTs, &write)
	// 删除锁（释放该键的预写锁）
	txn.DeleteLock(key)

	// 提交成功，返回无响应、无错误
	return nil, nil
}

// WillWrite 返回 Commit 命令要写入的所有键列表（实现 Command 接口的 WillWrite 方法）
// 用于锁存器加锁，避免并发写入冲突
func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}