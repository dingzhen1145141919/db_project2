package commands

// 注意：原代码中的 TODO 标记表示该包后续会被删除，现阶段是核心抽象层
// TODO delete the commands package.

import (
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches" // 事务锁存器，用于解决并发冲突
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"    // MVCC 核心层，处理多版本数据
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"      // RPC 协议定义，包含请求/响应结构体
)

// Command 是一个抽象接口，涵盖了从接收 gRPC 请求到返回响应的完整处理流程
// 所有事务命令（Get/Prewrite/Commit 等）都实现该接口
type Command interface {
	// Context 返回命令的上下文信息（包含 region 等元数据）
	Context() *kvrpcpb.Context
	// StartTs 返回事务的启动时间戳（全局唯一，标识事务）
	StartTs() uint64
	// WillWrite 返回当前命令可能写入的所有键的列表
	// 只读命令返回 nil，写命令返回待写入的键集合（用于加锁和冲突检测）
	WillWrite() [][]byte
	// Read 执行命令的只读逻辑，仅在 WillWrite 返回 nil 时被调用
	// 返回值说明：
	// 1. interface{}: 命令执行结果（如 Get 的值）
	// 2. [][]byte: 如果后续需要写入，返回待写入的键列表
	// 3. error: 执行过程中的错误
	Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error)
	// PrepareWrites 用于在 MVCC 事务中构建待写入的数据（写命令的核心逻辑）
	// 命令也可以通过 txn 执行非事务性的读写操作
	// 如果方法执行后未修改 txn，则表示不会执行任何事务操作
	PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error)
}

// RunCommand 是执行事务命令的入口函数，封装了命令的完整执行流程
// 参数：
// - cmd: 待执行的命令（实现 Command 接口）
// - storage: 存储引擎，用于读写数据
// - latches: 锁存器，用于保护待写入的键，避免并发冲突
// 返回值：
// - interface{}: 命令执行结果
// - error: 执行过程中的错误
func RunCommand(cmd Command, storage storage.Storage, latches *latches.Latches) (interface{}, error) {
	ctxt := cmd.Context()
	var resp interface{} // 存储命令执行结果

	// 第一步：获取命令可能写入的键列表
	keysToWrite := cmd.WillWrite()
	if keysToWrite == nil {
		// 情况1：命令是只读的，或需要先读取数据才能确定待写入的键
		// 1. 创建只读存储读取器
		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		// 2. 创建只读 MVCC 事务（仅能读取数据，不能写入）
		txn := mvcc.RoTxn{Reader: reader, StartTS: cmd.StartTs()}
		// 3. 执行 Read 方法（只读逻辑）
		resp, keysToWrite, err = cmd.Read(&txn)
		reader.Close() // 关闭读取器，释放资源
		if err != nil {
			return nil, err
		}
	}

	if keysToWrite != nil {
		// 情况2：命令需要写入数据（包括原本的写命令，或 Read 后确定需要写入的命令）
		// 1. 为待写入的键加锁存器，避免并发写入冲突
		latches.WaitForLatches(keysToWrite)
		defer latches.ReleaseLatches(keysToWrite) // 延迟释放锁存器

		// 2. 创建存储读取器（用于读取现有数据）
		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		defer reader.Close() // 延迟关闭读取器

		// 3. 创建可写 MVCC 事务（支持读写操作）
		txn := mvcc.NewTxn(reader, cmd.StartTs())
		// 4. 执行 PrepareWrites 方法，构建待写入的数据
		resp, err = cmd.PrepareWrites(&txn)
		if err != nil {
			return nil, err
		}

		// 5. 验证锁存器和事务的一致性（确保没有并发修改）
		latches.Validate(&txn, keysToWrite)

		// 6. 将事务中的写入操作持久化到存储引擎
		err = storage.Write(ctxt, txn.Writes())
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// CommandBase 为 Command 接口提供了默认的方法实现（基础属性和通用方法）
// 所有命令可以嵌入该结构体，避免重复实现 Context 和 StartTs 方法
type CommandBase struct {
	context *kvrpcpb.Context // 命令上下文（region 元数据等）
	startTs uint64           // 事务启动时间戳
}

// Context 返回命令的上下文信息，实现 Command 接口的 Context 方法
func (base CommandBase) Context() *kvrpcpb.Context {
	return base.context
}

// StartTs 返回事务启动时间戳，实现 Command 接口的 StartTs 方法
func (base CommandBase) StartTs() uint64 {
	return base.startTs
}

// Read 是默认的 Read 方法实现（空实现），实现 Command 接口的 Read 方法
// 写命令可以重写该方法，只读命令必须重写该方法
func (base CommandBase) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	return nil, nil, nil
}

// ReadOnly 是一个辅助类型，用于标记永远不会写入数据的只读命令
// 提供了 WillWrite 和 PrepareWrites 的默认实现（空实现）
type ReadOnly struct{}

// WillWrite 返回 nil，表示只读命令不会写入任何数据，实现 Command 接口的 WillWrite 方法
func (ro ReadOnly) WillWrite() [][]byte {
	return nil
}

// PrepareWrites 是空实现，实现 Command 接口的 PrepareWrites 方法
// 只读命令不会调用该方法，因此无需实现
func (ro ReadOnly) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	return nil, nil
}