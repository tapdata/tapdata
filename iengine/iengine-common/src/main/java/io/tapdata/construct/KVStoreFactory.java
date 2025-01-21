package io.tapdata.construct;

import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.KVStore;

public class KVStoreFactory {
    public static <T> KVStore<T> createKVStore(ExternalStorageDto externalStorageDto, String name) {
        String type = externalStorageDto.getType();
        switch (type) {
            case "rocksdb":
                return new RocksDBKVStore<>(externalStorageDto.getUri(), name);
            case "mongodb":
                return new MongoDBKVStore<>(externalStorageDto, name);
            case "memory":
            default:
                return new InMemoryKVStore<>(name);
        }
    }
}
