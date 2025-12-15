package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start(_ scheduler_client.Client) error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// 1. 开启badger的只读事务（false表示只读，符合StorageReader的只读要求）
	txn := s.db.NewTransaction(false)
	// 2. 创建BadgerReader实例（实现了StorageReader接口）
	reader := NewBadgerReader(txn)
	// 3. 返回读取器和空错误
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// 1. 开启badger的读写事务（true表示读写，保证批量修改的原子性）
	txn := s.db.NewTransaction(true)
	// 延迟关闭事务：如果提交失败，自动回滚并释放资源
	defer txn.Discard()

	// 2. 遍历所有Modify操作，逐个处理
	for _, m := range batch {
		// 获取列族名和key（利用Modify的便捷方法，避免重复的类型断言）
		cf := m.Cf()
		key := m.Key()
		// 用KeyWithCF编码key（模拟列族，必须做！）
		encodedKey := engine_util.KeyWithCF(cf, key)

		switch data := m.Data.(type) {
		// 3. 处理Put操作：写入键值对
		case storage.Put:
			value := data.Value
			// 调用txn.Set写入编码后的key和value
			if err := txn.Set(encodedKey, value); err != nil {
				return err
			}
		// 4. 处理Delete操作：删除键值对
		case storage.Delete:
			// 调用txn.Delete删除编码后的key
			if err := txn.Delete(encodedKey); err != nil {
				return err
			}
		}
	}

	// 5. 提交事务：将所有修改批量写入磁盘（原子性）
	if err := txn.Commit(); err != nil {
		return err
	}

	// 6. 所有操作成功，返回nil
	return nil
}

func (s *StandAloneStorage) Client() scheduler_client.Client {
	return nil
}

type BadgerReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard()
}
