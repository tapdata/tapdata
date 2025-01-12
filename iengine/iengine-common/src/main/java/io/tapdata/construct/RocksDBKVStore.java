package io.tapdata.construct;

import io.tapdata.construct.constructImpl.KVStore;
import org.rocksdb.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class RocksDBKVStore<T> implements KVStore<T> {
    private RocksDB db;
    private String path;
    private String dbName;

    public RocksDBKVStore(String path, String dbName) {
        if (!path.startsWith(".")) {
            path = "." + path;
        }
        this.path = path;
        this.dbName = dbName;
        this.init(null);
    }

    @Override
    public void init(Map<String, Object> config) {
        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, this.path + "/" + dbName);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
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
        try {
            db.put(key.getBytes(), serialize(value));
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to upsert value", e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete key", e);
        }
    }

    @Override
    public T find(String key) {
        try {
            byte[] value = db.get(key.getBytes());
            return value == null ? null : deserialize(value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to find value", e);
        }
    }

    @Override
    public boolean exists(String key) {
        return find(key) != null;
    }

    @Override
    public boolean isEmpty() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            return !iterator.isValid();
        }
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public String getType() {
        return "rocksdb";
    }

    @Override
    public Map<String, T> findByPrefix(String prefix) {
        Map<String, T> result = new HashMap<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix.getBytes()); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                if (!key.startsWith(prefix)) {
                    break;
                }
                result.put(key, deserialize(iterator.value()));
            }
        }
        return result;
    }

    @Override
    public void clear() {

    }

    private byte[] serialize(T obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private T deserialize(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}