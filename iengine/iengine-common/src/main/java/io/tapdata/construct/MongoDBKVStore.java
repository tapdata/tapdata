package io.tapdata.construct;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOptions;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.KVStore;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class MongoDBKVStore<T> implements KVStore<T> {
    private MongoCollection<Document> collection;
    private final String collectionName;

    public MongoDBKVStore(ExternalStorageDto externalStorageDto, String name) {
        // name hash 成数字
        int hash = name.hashCode();
        this.collectionName = externalStorageDto.getTable() + "_" + hash;
        Map<String, Object> config = new HashMap<>();
        config.put("uri", externalStorageDto.getUri());
        this.init(config);
    }

    /**
     * 初始化 MongoDB 连接
     *
     * @param config 包含连接字符串和数据库名的配置
     */
    public void init(Map<String, Object> config) {
        ConnectionString connectionString = new ConnectionString((String) config.get("uri"));
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(connectionString.getDatabase());
        collection = database.getCollection(collectionName);
        collection.createIndex(new Document("key", 1));
    }

    @Override
    public void insert(String key, T value) {
        upsert(key, value);
    }

    @Override
    public void update(String key, T value) {
        upsert(key, value);
    }

    @Override
    public void upsert(String key, T value) {
        if (value instanceof String) {
            value = value;
        } else {
            value = (T) new Document((Map<String, Object>) value);
        }
        Document document = new Document("key", key).append("value", value);
        collection.replaceOne(new Document("key", key), document, new ReplaceOptions().upsert(true));
    }

    /**
     * 删除键值对
     *
     * @param key 唯一键
     */
    public void delete(String key) {
        collection.deleteOne(new Document("key", key));
    }

    /**
     * 查找键对应的值
     *
     * @param key 唯一键
     * @return 值 (Map<String, Object>)，如果不存在返回 null
     */
    public T find(String key) {
        Document doc = collection.find(new Document("key", key)).first();
        return doc != null ? (T) doc.get("value") : null;
    }

    /**
     * 检查键是否存在
     *
     * @param key 唯一键
     * @return 是否存在
     */
    public boolean exists(String key) {
        return collection.find(new Document("key", key)).first() != null;
    }

    /**
     * 检查集合是否为空
     *
     * @return 是否为空
     */
    public boolean isEmpty() {
        return collection.countDocuments() == 0;
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public String getType() {
        return "mongodb";
    }

    /**
     * 按前缀查找所有匹配的键值对
     *
     * @param prefix 键的前缀
     * @return 匹配的键值对
     */
    public Map<String, T> findByPrefix(String prefix) {
        Map<String, T> result = new HashMap<>();
        collection.find(new Document("_id", new Document("$regex", "^" + prefix))).forEach((Block<? super Document>) doc -> {
            String key = doc.getString("key");
            T value = (T) doc.get("value");
            result.put(key, value);
        });
        return (Map<String, T>) result;
    }

    /**
     * 清空集合
     */
    public void clear() {
        collection.drop();
    }
}