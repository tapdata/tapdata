package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2021-11-23 17:09
 **/
public class ConstructIMap<T> extends BaseConstruct<T> {

	protected IMap<String, Object> iMap;

	public ConstructIMap(HazelcastInstance hazelcastInstance, String name) {
		super(name);
		this.iMap = hazelcastInstance.getMap(name);
	}

	public ConstructIMap(HazelcastInstance hazelcastInstance, String referenceId, String name) {
		super(name);
		this.iMap = hazelcastInstance.getMap(name);
	}

	public ConstructIMap(HazelcastInstance hazelcastInstance, String referenceId, String name, ExternalStorageDto externalStorageDto) {
		super(name, referenceId, externalStorageDto);
		ExternalStorageUtil.initHZMapStorage(externalStorageDto, referenceId, name, hazelcastInstance.getConfig());
		this.iMap = hazelcastInstance.getMap(name);
		Integer ttlDay = externalStorageDto.getTtlDay();
		if (ttlDay != null && ttlDay > 0) {
			convertTtlDay2Second(ttlDay);
			PersistenceStorage.getInstance().setImapTTL(this.iMap, this.ttlSecond);
		}
	}

	@Override
	public int insert(String key, T data) throws Exception {
		iMap.put(key, data);
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
		Object obj = iMap.get(key);
		if (obj == null)
			return null;
		if (!obj.getClass().getClassLoader().equals(this.getClass().getClassLoader())) {
			if (obj.getClass().getName().equals(Document.class.getName())) {
				ObjectSerializable serializable = InstanceFactory.instance(ObjectSerializable.class);
				byte[] bytes = serializable.fromObject(obj);
				obj = serializable.toObject(bytes);
			}
		}
		return (T) obj;
	}

	@Override
	public boolean exists(String key) throws Exception {
		return iMap.containsKey(key);
	}

	@Override
	public void clear() throws Exception {
		PersistenceStorage.getInstance().clear(ConstructType.IMAP, name);
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

	public IMap<String, Object> getiMap() {
		return iMap;
	}

	@Override
	public void destroy() throws Exception {
		if (PersistenceStorage.getInstance().destroy(referenceId, ConstructType.IMAP, name) && null != iMap) {
			iMap.destroy();
		}
	}
}
