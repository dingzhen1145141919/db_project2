package commands

import (
	"encoding/hex" // 用于将字节数组转换为十六进制字符串，方便日志打印

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc" // MVCC 只读事务依赖
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"   // RPC 协议中的 Get 请求/响应结构体
	"github.com/pingcap/log"                                  // 日志库
	"go.uber.org/zap"                                         // 日志字段封装
)

// Get 表示一个 Get 命令（点查询，获取指定键的指定版本数据）
// 嵌入了 ReadOnly（标记为只读命令）和 CommandBase（基础属性和方法）
type Get struct {
	ReadOnly       // 只读命令标记，提供 WillWrite 和 PrepareWrites 的默认实现
	CommandBase    // 基础属性：context 和 startTs
	request *kvrpcpb.GetRequest // Get 请求的具体参数（键、版本等）
}

// NewGet 创建一个 Get 命令实例
// 参数：request - Get 请求的 RPC 结构体
// 返回：初始化后的 Get 命令
func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context, // 请求上下文（region 元数据）
			startTs: request.Version, // 事务启动时间戳（即要读取的版本号）
		},
		request: request, // 保存请求参数
	}
}

// Read 执行 Get 命令的只读逻辑（核心实现部分）
// 实现 Command 接口的 Read 方法，因为是只读命令，WillWrite 返回 nil，因此该方法会被调用
// 参数：txn - 只读 MVCC 事务，用于读取数据
// 返回值：
// 1. interface{}: Get 响应结果（kvrpcpb.GetResponse）
// 2. [][]byte: 只读命令返回 nil
// 3. error: 执行过程中的错误（如锁冲突、读取失败）
func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key // 获取要查询的键
	// 打印调试日志：事务启动时间戳、要查询的键（十六进制字符串）
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	// 初始化 Get 响应结构体
	response := new(kvrpcpb.GetResponse)

	// 原代码的 panic 表示该方法尚未实现
	// panic("kv get is not implemented yet")
	// YOUR CODE HERE (lab2).
	// 实验二待实现：检查锁及其可见性（核心逻辑）
	// 提示：使用 mvcc.RoTxn 提供的接口（如 GetLock）检查键是否被锁定
	// 要点：如果键被其他事务锁定，需要返回锁冲突错误；如果是当前事务锁定，可继续读取
	// ========== 步骤1：检查key是否被锁定（核心逻辑） ==========
	// 调用本地RoTxn的GetLock方法，获取key对应的锁（*mvcc.Lock）
	lock, err := txn.GetLock(key)
	if err != nil {
		// 读取锁时发生内部错误，直接返回
		return nil, nil, err
	}

	// 如果锁存在，且锁的Ts不等于当前事务的StartTS（被其他事务锁定）
	if lock != nil && lock.Ts <= txn.StartTS {
	// if lock != nil && lock.Ts != txn.StartTS { 究竟是哪个呢
		// 构造锁冲突错误：将mvcc.Lock转换为kvrpcpb.LockInfo，封装到KeyError中
		keyError := &kvrpcpb.KeyError{Locked: lock.Info(key)}
        response.Error = keyError
		return response, nil, nil
	}


	// YOUR CODE HERE (lab2).
	// 实验二待实现：查找已提交的数值，将结果设置到响应中
	// 提示：使用 mvcc.RoTxn 提供的接口（如 GetValue）读取指定版本的数值
	// 要点：读取小于等于 startTs 的最新提交版本的数值
	// ========== 步骤2：读取key的最新有效值（核心逻辑） ==========
	// 调用本地RoTxn的GetValue方法，获取key在txn.StartTS前的最新值
	value, err := txn.GetValue(key)
	if err != nil {
		return nil, nil, err
	}

	// 填充响应结果
	response.Value = value
	// Found为true表示找到值，false表示未找到
	response.NotFound = value == nil


	// 返回响应结果、空的待写入键列表、无错误
	return response, nil, nil
}