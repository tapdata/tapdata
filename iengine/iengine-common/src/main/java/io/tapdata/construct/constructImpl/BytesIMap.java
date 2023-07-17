package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * use bytes storage
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/10/28 11:35 Create
 */
public class BytesIMap<T> extends ConstructIMap<T> {
	private static final String DATA_KEY = BytesIMap.class.getSimpleName() + "-DATA";

	public BytesIMap(HazelcastInstance hazelcastInstance, String referenceId, String name, ExternalStorageDto externalStorageDto) {
		super(hazelcastInstance, referenceId, name, externalStorageDto);
	}

	private byte[] serialized(T data) {
		return InstanceFactory.instance(ObjectSerializable.class).fromObject(data);
	}

	private T deserialized(byte[] data) {
		return (T) InstanceFactory.instance(ObjectSerializable.class).toObject(data);
	}

	@Override
	public int insert(String key, T data) throws Exception {
		if(data instanceof HashMap) {
			((HashMap<String, Map<String, Object>>) data).values().forEach(this::replaceDataTyp);
		}
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
		if (o instanceof Document && ((Document) o).containsKey(DATA_KEY)) {
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

	private void replaceDataTyp(Object data){
		if(data instanceof Map){
			Map<String, Object> value = ((Map<String, Object> ) data);
			value.keySet().forEach(keyObj ->{
				Object obj = value.get(keyObj);
				if(obj instanceof Instant){
					Instant instant = (Instant) obj;
					value.replace(keyObj, Date.from(instant));
				}else if(obj instanceof BigDecimal){
					BigDecimal bigDecimal = (BigDecimal) obj;
					if (bigDecimal.precision() > 34) {
						Decimal128 decimal128 = new Decimal128(bigDecimal.setScale(bigDecimal.scale() + 34 - bigDecimal.precision(), RoundingMode.HALF_UP));
						value.replace(keyObj,decimal128);
					} else {
						value.replace(keyObj,new Decimal128(bigDecimal));
					}
				}else if(obj instanceof BigInteger){
					BigInteger bigInteger = (BigInteger) obj;
					value.replace(keyObj,bigInteger.longValue());
				}else if(obj instanceof Map || obj instanceof List){
					replaceDataTyp(obj);
				}
			});
		}else if(data instanceof List){
			List<Object> value = (List<Object>)data;
			for(int i = 0;i < value.size();i++){
				Object obj = value.get(i);
				if(obj instanceof Instant){
					Instant instant = (Instant) obj;
					value.set(i,Date.from(instant));
				}else if(obj instanceof BigDecimal){
					BigDecimal bigDecimal = (BigDecimal) obj;
					if (bigDecimal.precision() > 34) {
						Decimal128 decimal128 = new Decimal128(bigDecimal.setScale(bigDecimal.scale() + 34 - bigDecimal.precision(), RoundingMode.HALF_UP));
						value.set(i,decimal128);
					} else {
						value.set(i,new Decimal128(bigDecimal));
					}
				}else if(obj instanceof BigInteger){
					BigInteger bigInteger = (BigInteger) obj;
					value.set(i,bigInteger);
				}else if(obj instanceof Map || obj instanceof List){
					replaceDataTyp(obj);
				}
			}
		}
	}
}
