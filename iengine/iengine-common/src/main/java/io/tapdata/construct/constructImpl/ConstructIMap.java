package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.CacheStatistics;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.bson.Document;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2021-11-23 17:09
 **/
public class ConstructIMap<T> extends BaseConstruct<T> {

	/**
	 * Process-wide write-side backpressure policy. It bounds the GLOBAL write-behind backlog (total pending
	 * entries across all cache IMaps) so a slow/failing external storage cannot OOM the engine. Backlog is
	 * read from the background sampler; every write path routes through {@link #applyBackpressure()}.
	 */
	private static final WriteBehindBackpressure BACKPRESSURE = WriteBehindBackpressure.fromEnv(
			new WriteBehindBackpressure.BacklogSource() {
				@Override
				public long total() {
					return WriteBehindBacklogSampler.getInstance().totalBacklog();
				}

				@Override
				public long ofMap(String name) {
					return WriteBehindBacklogSampler.getInstance().backlog(name);
				}
			});

	protected IMap<String, Object> iMap;

	public ConstructIMap(HazelcastInstance hazelcastInstance, String name) {
		super(name);
		this.iMap = hazelcastInstance.getMap(name);
	}

	public ConstructIMap(HazelcastInstance hazelcastInstance, String referenceId, String name) {
		super(referenceId, name);
		this.iMap = hazelcastInstance.getMap(name);
	}

	public ConstructIMap(HazelcastInstance hazelcastInstance, String referenceId, String name, ExternalStorageDto externalStorageDto) {
		super(referenceId, name, externalStorageDto);
		ExternalStorageUtil.initHZMapStorage(externalStorageDto, referenceId, name, hazelcastInstance.getConfig());
		this.iMap = hazelcastInstance.getMap(name);
		Integer ttlDay = externalStorageDto.getTtlDay();
		if (ttlDay != null && ttlDay > 0) {
			convertTtlDay2Second(ttlDay);
			PersistenceStorage.getInstance().setImapTTL(this.iMap, this.ttlSecond);
		}
		// Only MapStore-backed (external storage) IMaps can have a write-behind queue, so only those are sampled.
		if (BACKPRESSURE.isEnabled()) {
			WriteBehindBacklogSampler.getInstance().register(name, iMap);
		}
	}

	/** Apply global write-behind backpressure before a write. Delegates to the shared policy. */
	protected void applyBackpressure() throws Exception {
		BACKPRESSURE.apply(name);
	}

	/** Single guarded write path: every put goes through here so backpressure cannot be bypassed. */
	protected void putInternal(String key, Object value) throws Exception {
		applyBackpressure();
		iMap.put(key, value);
	}

	/** Single guarded remove path: write-behind deletes also enqueue tombstones, so throttle them too. */
	protected void removeInternal(String key) throws Exception {
		applyBackpressure();
		iMap.remove(key);
	}

	@Override
	public int insert(String key, T data) throws Exception {
		putInternal(key, data);
		return 1;
	}

	@Override
	public long insertMany(Map<String, T> data) throws Exception {
		applyBackpressure();
		iMap.putAll(data);
		return data.size();
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
		removeInternal(key);
		return 1;
	}

	@Override
	public T find(String key) throws Exception {
		Object obj = iMap.get(key);
		if(obj == null)
			return null;
		obj = handleObjectWhenDiffClassLoader(obj);
		return (T) obj;
	}

	@Override
	public Map<String, Object> findAll(Set<String> keys) {
		Map<String, Object> getMap = iMap.getAll(keys);
		Map<String, Object> result = new HashMap<>();
		Iterator<Map.Entry<String, Object>> iterator = getMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Object> entry = iterator.next();
			Object obj = entry.getValue();
			if (obj == null)
				continue;
			obj = handleObjectWhenDiffClassLoader(obj);
			result.put(entry.getKey(), obj);
		}
		return result;
	}

	private Object handleObjectWhenDiffClassLoader(Object obj) {
		if (!obj.getClass().getClassLoader().equals(this.getClass().getClassLoader())) {
			if (obj.getClass().getName().equals(Document.class.getName())) {
				ObjectSerializable serializable = InstanceFactory.instance(ObjectSerializable.class);
				byte[] bytes = serializable.fromObject(obj);
				obj = serializable.toObject(bytes);
			}
		}
		return obj;
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
		return PersistenceStorage.getInstance().isEmpty(ConstructType.IMAP, name);
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
		WriteBehindBacklogSampler.getInstance().unregister(name);
		if (PersistenceStorage.getInstance().destroy(referenceId, ConstructType.IMAP, name) && null != iMap) {
			iMap.destroy();
		}
	}

	@Override
	public CacheStatistics getStatistics(){
		Number memorySize = iMap.getLocalMapStats().getOwnedEntryMemoryCost();
		Number entryCount = iMap.getLocalMapStats().getOwnedEntryCount();
		Number entryCountLimit;
		if(memorySize.longValue() == 0 || entryCount.longValue() == 0){
			entryCountLimit = 0;
		}else{
			entryCountLimit = (externalStorageDto.getInMemSize() * 1024 * 1024) / (memorySize.longValue() / entryCount.longValue());
		}
		return CacheStatistics.createLocalCache(memorySize,entryCount,entryCountLimit);
	}
}
