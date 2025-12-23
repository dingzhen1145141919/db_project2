# 实验 1 存储与日志层

## 设计

在本章中，我们将讨论分布式事务数据库系统的设计。架构的总体概述如下：

![overview](imgs/overview.png)

众所周知，事务系统必须确保在大多数情况下**`ACID`**属性能够得到保证。如何确保这些属性呢？

### 首先确保 `持久性`

在分布式环境中，为了满足**可用性**和**可靠性**要求，关键点在于提高**事务日志**的性能。正如 `AWS Aurora`所说“**日志即数据库**”，如果日志被可靠地持久化，那么**`持久性`**就能得到保障。

在单机数据库如 MySQL 中，通过在返回结果给客户端之前将 InnoDB 重做日志持久化到磁盘来实现这一点，显然，如果单个节点发生故障，日志可能会丢失。为了更可靠地持久化日志，需要副本，因此引入了分布式系统。困难之处在于如何确保不同副本中的日志完全相同。通常我们会使用一些共识算法将日志复制到不同的节点，`CAP`理论告诉我们，使用的副本越多，可用性会增加，丢失日志的可能性也会降低。

在[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)的架构中，[Raft](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/raft.go#L129-L179)被用作共识算法来将日志复制到副本节点，代码模块称为 `raftStore`。

![raft](imgs/raft.png)

`raftStore`负责将事务日志或提交日志复制到不同的节点，在大多数复制成员成功接受日志之后，
它们被认为是 `已提交`，然后可以响应客户端写入过程可以继续。这是保证一旦事务被提交，其所有写入内容至少在大多数节点上被持久化到磁盘，从而在分布式环境中保持事务 `ACID`的**`持久性`**。

仅有提交日志是不够的，读写请求无法被服务。存储引擎用于应用或回放这些日志，然后它将处理这些请求。
在 TiKV 中，[rocksdb](https://docs.pingcap.com/zh/tidb/stable/rocksdb-overview)被用来构建存储引擎层。在 tinykv 中，类似的存储引擎[badger](https://github.com/dgraph-io/badger)被使用，所有的读写请求都将由它处理。存储引擎的难点在于尽可能快地处理请求，同时减少所需的资源。

### 如何使其 `原子化`

在传统的分布式事务架构解决方案中，使用特殊的协议如 `两阶段提交`来确保事务处理的原子性。问题是如何确保事务处理在故障转移后能够**正确**继续，因为在传统的 2PC 处理过程中，如果协调器不可用，整个过程将会卡住。

在[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)中，`raftStore`已经确保日志会被复制到副本，并且故障转移更容易，只要 raft 组的多数节点存活，事务状态总能恢复。也就是说 2PC 处理可以继续，因为新选举的领导者总是拥有相同的事务状态，无论协调器还是参与者失败。

在 tinysql/tinykv 中，[percolator](https://research.google/pubs/pub36726/)协议被用作分布式事务协议，它类似于传统的 2PC 方式，但有一些不同。主要区别之一是协调器或调度器不需要在本地持久化事务状态，所有事务状态都持久化在参与节点中。在 tinysql/tinykv 集群中，`tinysql`节点充当事务协调器，所有[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)节点都是参与者。在接下来的实验中，我们将在现有的 `tinysql`和[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)框架基础上实现 percolator 协议。

### 并发与隔离

为了获得更好的性能，事务引擎需要处理许多并发请求，如何并发处理它们并确保结果合理？定义了隔离级别来描述性能和并发约束之间的权衡，如果不熟悉事务隔离级别及相关概念，可以参考[文档](https://pingcap.com/blog-cn/take-you-through-the-isolation-level-of-tidb-1)。

在 tinysql/tinykv 集群中，我们将实现一种强隔离约束，称为 `快照隔离`或 `可重复读`。在分布式环境中，使用全局时间戳排序分配器来序列化所有并发事务，每个事务都有一个唯一的 `start_ts`，表示它使用的快照，这个时间戳排序分配器位于 tinysql/tinykv 集群中的 `tinyscheudler`服务器中。要了解更多关于集群的调度服务信息，可以参考此[文档](https://pingcap.com/blog-cn/placement-driver)。

### 支持 SQL 事务

为了构建一个完整的分布式事务数据库，我们将为事务语法添加 SQL 支持，如 `BEGIN`、`COMMIT`、`ROLLBACK`。常用的写入语句如 `INSERT`、`DELETE`将被实现以使用分布式事务将数据写入存储层。`SELECT`结果也将保持上述章节中描述的事务属性。并且事务层能够正确处理读写和写写冲突。

## 实验 1

在这个实验中，我们将熟悉[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)中的整个框架，并完成 `raftStore`和 `storeEngine`的实现。如上所述，`raftStore`将处理所有提交日志并将它们复制到不同节点的不同 raft 组中。在 tinykv 中，一个 raft 组被称为[Region](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinysql/store/tikv/region_cache.go#L50-L55)，每个 region 都有它服务的键范围。在引导阶段之后将有一个 region，未来该 region 可以分裂成更多的 region，然后不同的 raft 组或我们在 `raftStore`中称之为[regions](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinysql/store/mockstore/mocktikv/cluster.go#L43-L43)将负责不同的键范围，`多raft`或 `多个region`将独立处理客户端请求。目前你可以简单地认为只有一个 raft 组或一个 region 在处理请求。
这份[文档](https://docs.pingcap.com/zh/tidb/stable/tikv-overview)可能有助于理解 `raftStore`架构。

### 代码

#### `raftStore`抽象

在[kv/storage/storage.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/storage.go)中，有 `raftStore`的接口或抽象。

```
// Storage代表TinyKV内部面对服务器的部分，它处理与其他TinyKV节点的发送和接收。
// 作为这一责任的一部分，它还会从磁盘（或半永久内存）读取和写入数据。
type Storage interface {
	Start(client scheduler_client.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
	Client() scheduler_client.Client
}
```

`Write`接口将被事务引擎用来持久化写日志，如果返回 ok，则表示日志已被成功持久化并由存储引擎应用。这里需要两个步骤，第一步是在多数节点上持久化日志，第二步是在存储引擎中应用这些写操作。

为了简化，我们将跳过 raft 日志共识步骤，首先考虑单机存储引擎。在此之后，我们将熟悉存储引擎接口，这非常有用，因为 `raftStore`将使用相同的存储引擎来持久化日志。

#### 实现[StandAloneStorage](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/standalone_storage/standalone_storage.go#L13-L15)的核心接口

尝试实现[kv/storage/standalone_storage/standalone_storage.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/standalone_storage/standalone_storage.go)中缺失的代码，这些代码部分标记为：

`// YOUR CODE HERE (lab1).`

完成这些部分后，运行 `make lab1P0`命令检查是否所有测试用例都通过。注意事项：

- 由于[badger](https://github.com/dgraph-io/badger)被用作存储引擎，常见用法可以在其文档和存储库中找到。
- `badger`存储引擎不支持[`列族`](https://en.wikipedia.org/wiki/Standard_column_family)。`percolator`事务模型需要列族，在[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)中，列族相关的实用程序已经在[kv/util/engine_util/util.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/util.go)中封装。当处理[storage.Modify](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/modify.go#L3-L5)时，写入存储引擎的键应使用[KeyWithCF](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/util.go#L9-L11)函数编码，考虑其期望的列族。在[tinykv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/deploy/main.go#L19-L19)中有两种类型的[Modify](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/modify.go#L3-L5)，更多信息请查看[kv/storage/modify.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/modify.go)。
- [scheduler_client.Client](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/scheduler_client/client.go#L33-L47)不会被 `standAloneServer`使用，因此可以跳过。
- 可以考虑 `badger`提供的[txn](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/standalone_storage/standalone_storage.go#L51-L51)功能及相关的读写接口。更多信息请查看[BadgerReader](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/standalone_storage/standalone_storage.go#L50-L52)。
- 一些测试用例可能有助于理解存储接口的用法。

#### 实现[RaftStorage](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/raft_storage/raft_server.go#L25-L38)的核心接口

在[StandAloneStorage](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/standalone_storage/standalone_storage.go#L13-L15)中，日志引擎层被忽略，所有读写请求直接由存储引擎处理。在本章中，我们将尝试构建如上所述的日志引擎。在 `standalone_storage`中，单个 badger 实例被用作存储引擎。
在 `raftStore`中将有两个 `badger`实例，第一个用作存储引擎或状态机，就像 `standalone_storage`一样，第二个将被 `raftStore`中的日志引擎用来持久化 raft 日志。在[kv/util/engine_util/engines.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/engines.go)中，你可以找到[Kv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/engines.go#L18-L18)和[Raft](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/engines.go#L21-L21)结构成员，[Kv](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/engines.go#L18-L18)实例用作存储引擎，[Raft](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/util/engine_util/engines.go#L21-L21)被日志引擎使用。

`raftStore`的工作流程如下：

![raftStore](imgs/raftstore.png)

有一些重要的概念和抽象，其中一些已在上面提到，列表如下：

- [RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)。raft 实例的包装，[Step](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/raft.go#L539-L615)、[Ready](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L42-L70)和[Advance](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L203-L220)接口由上层使用来驱动 raft 进程。[RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)及其内部的 raft 实例不负责实际发送消息和持久化日志，它们将在结果[Ready](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L42-L70)结构中设置，上层将处理 ready 结果执行实际工作。
- [Ready](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L42-L70)。ready 是 raft 实例的输出，它们预计由上层处理，例如向其他节点发送消息并将信息持久化到日志引擎。更多关于[Ready](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L42-L70)的信息可以在 `kv/raft/rawnode.go`中的注释中找到。

上述概念或抽象是关于 raft 实例的。以下概念建立在 raft 实例或[RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)之上。

- [Region](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinysql/store/tikv/region_cache.go#L50-L55)。一个 region 是一个 raft 组，负责处理与特定键范围相关的读/写请求。
- [Peer](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinysql/store/tikv/region_cache.go#L245-L245)。一个 peer 是一个 raft 组或 region 的成员，默认使用 3 个副本，一个 region 将有 3 个不同的 peers。一个 peer 将有一个包含 raft 实例的[RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)。
- [raftWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L9-L21)。处理所有路由到不同 region 领导者或 region peers 的客户端请求的工作线程。
- [peerMsgHandler](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_msg_handler.go#L30-L34)。用于处理特定领导者 peer 的客户端请求的委托。
- [applyWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L85-L91)。在提议的请求和相关日志被 raft 组提交后，相应的应用请求将被路由到[applyWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L85-L91)，然后这些日志将被应用到状态机，即 tinykv 中的 `badger`存储引擎。

将它们放在一起，消息流可以分为两个阶段：

- **日志共识阶段**。客户端请求被发送到带有回调的路由器，[raftWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L9-L21)将使用相应的对等消息处理器处理请求。
- **日志应用阶段**。当日志被 raft 组提交后，应用请求将被发送到应用路由器，[applyWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L85-L91)将处理应用请求并最终调用回调，然后允许向客户端响应结果。

有助于理解 `raftStore`的文档：

- [raftStore 代码分析](https://pingcap.com/blog-cn/tikv-source-code-reading-17)
- [tikv 源码阅读 raft 提议](https://pingcap.com/blog-cn/tikv-source-code-reading-2)
- [tikv 源码阅读 raft 提交/应用](https://pingcap.com/blog-cn/tikv-source-code-reading-18)

尝试实现以下缺失的代码：

- [kv/raftstore/peer_msg_handler.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_msg_handler.go)，[proposeRaftCommand](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_msg_handler.go#L619-L633)方法，这是读/写请求提议的核心部分。
- [kv/raftstore/peer.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer.go)，[HandleRaftReady](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_msg_handler.go#L136-L166)方法，这是 raft ready 处理的核心部分。
- [kv/raftstore/peer_storage.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_storage.go)，[SaveReadyState](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_storage.go#L403-L436)方法，这是状态和日志持久性的核心部分。
- [kv/raftstore/peer_storage.go](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_storage.go)，[Append](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/peer_storage.go#L254-L283)方法，它将来自 raft ready 的日志追加到日志引擎。

这些代码入口标记为：

`// YOUR CODE HERE (lab1).`

待完成的代码部分标记为

`// Hintx: xxxxx`

周围有一些有用的注释和指导。

由于 `raftStore`相当复杂，所以测试分为 4 个部分，[Makefile](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/Makefile)文件中有更多信息，测试顺序如下：

- `make lab1P1a`。这是关于 `raftStore`逻辑的基本测试。
- `make lab1P1b` 带有故障注入。这是关于 `raftStore`逻辑的基本测试。
- `make lab1P2a`。这是关于 `raftStore`持久性的测试。
- `make lab1P2b` 带有故障注入。这是关于 `raftStore`持久性的测试。
- `make lab1P3a`。这是关于 `raftStore`快照相关测试。
- `make lab1P3b` 带有故障注入。这是关于 `raftStore`快照相关测试。
- `make lab1P4a`。这是关于 `raftStore`配置变更测试。
- `make lab1P4b` 带有故障注入。这是关于 `raftStore`配置变更测试。

注意事项：

- 当测试失败时，`/tmp/test-raftstore-xxx`中可能会有一些垃圾目录或文件，可以手动删除，或者使用 `make clean`命令进行清理工作。
- 测试可能消耗大量内存，最好使用 RAM(>= 16 GB)的开发机器，如果因为 OOM 测试无法一起运行，尝试逐一运行它们，使用类似 `go test -v ./kv/test_raftstore -run test_name`的命令。
- 尝试设置更大的打开文件限制，例如 `ulimit -n 8192`。
- raft 包提供了 raft 实现。它被封装进[RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)，[Step](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L165-L174)和[Ready](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L42-L70)是使用[RawNode](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/raft/rawnode.go#L94-L98)的核心接口。
- 在[RaftStorage](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/storage/raft_storage/raft_server.go#L25-L38)中会有不同种类的工作线程，最重要的工作线程是[raftWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L9-L21)和[applyWorker](file:///Users/tangyi/Desktop/study/db/project2/vldb-2021-labs-master/tinykv/kv/raftstore/raft_worker.go#L85-L91)。
- 尝试理解整个消息处理过程，新的输入客户端请求并响应客户端结果。由于有不同的工作线程并且 raft 共识需要几个步骤，客户端请求可能会被转发到不同的工作线程，此外还会使用回调来通知调用者结果。
