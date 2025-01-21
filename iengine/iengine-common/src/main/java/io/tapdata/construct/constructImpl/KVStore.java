package io.tapdata.construct.constructImpl;

import java.util.List;
import java.util.Map;

public interface KVStore<T> {

    /**
     * 初始化存储引擎
     * @param config 配置参数
     */
    void init(Map<String, Object> config);

    /**
     * 插入一个键值对，若键已存在则抛出异常
     * @param key 键
     * @param value 值
     */
    void insert(String key, T value);

    /**
     * 更新一个键值对，若键不存在则抛出异常
     * @param key 键
     * @param value 值
     */
    void update(String key, T value);

    /**
     * 插入或更新一个键值对
     * @param key 键
     * @param value 值
     */
    void upsert(String key, T value);

    /**
     * 根据键删除值
     * @param key 键
     */
    void delete(String key);

    /**
     * 根据键获取值
     * @param key 键
     * @return 值，如果不存在返回 null
     */
    T find(String key);

    /**
     * 检查键是否存在
     * @param key 键
     * @return 是否存在
     */
    boolean exists(String key);

    /**
     * 检查存储是否为空
     * @return 是否为空
     */
    boolean isEmpty();

    /**
     * 获取存储引擎的名称
     * @return 存储名称
     */
    String getName();

    /**
     * 获取存储引擎的类型
     * @return 存储类型（如 "InMemory", "RocksDB", "MongoDB"）
     */
    String getType();

    /**
     * 根据前缀获取匹配的键值对
     * @param prefix 键的前缀
     * @return 匹配的键值对集合
     */
    Map<String, T> findByPrefix(String prefix);

    void clear();
}