package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.bson.Document;

/**
 * use bytes storage
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/10/28 11:35 Create
 */
public class BytesIMap<T> extends ConstructIMap<T> {
	private static final String DATA_KEY = BytesIMap.class.getSimpleName() + "-DATA";

    public BytesIMap(HazelcastInstance hazelcastInstance, String name, ExternalStorageDto externalStorageDto) {
        super(hazelcastInstance, name, externalStorageDto);
    }

    private byte[] serialized(T data) {
        return InstanceFactory.instance(ObjectSerializable.class).fromObject(data);
    }

    private T deserialized(byte[] data) {
        return (T) InstanceFactory.instance(ObjectSerializable.class).toObject(data);
    }

    @Override
    public int insert(String key, T data) throws Exception {
		iMap.put(key, new Document(DATA_KEY, serialized(data)));
        return 1;
    }

    @Override
    public int update(String key, T data) throws Exception {
        return insert(key, data);
    }

    @Override
    public int upsert(String key, T data) throws Exception {
        return insert(key, data);
    }

    @Override
    public int delete(String key) throws Exception {
        int delete = 0;

        iMap.remove(key);
        delete++;

        return delete;
    }

    @Override
    public T find(String key) throws Exception {
        Object o = iMap.get(key);
		if(o instanceof Document && ((Document) o).containsKey(DATA_KEY)){
			o = ((Document) o).get(DATA_KEY);
		}
        if (o instanceof byte[]) {
            return deserialized((byte[]) o);
        } else {
            return (T) o; // Compatible with old data is the Map
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        return iMap.containsKey(key);
    }

    @Override
    public void clear() throws Exception {
        iMap.clear();
    }

    @Override
    public boolean isEmpty() {
        if (null == this.iMap) {
            return true;
        }
        return this.iMap.isEmpty();
    }

    @Override
    public String getName() {
        return iMap.getName();
    }

    @Override
    public String getType() {
        return "IMap";
    }
}
