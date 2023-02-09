package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import org.bson.Document;
import org.bson.types.Decimal128;

/**
 * @author samuel
 * @Description Since the current implementation of Hazelcast iMap external memory only supports the value of Document type, if non-Document type wants to use external memory, use this class to adapt
 * @create 2022-06-23 11:28
 **/
public class DocumentIMap<T> extends ConstructIMap<T> {
	private static final String DOCUMENT_KEY = DocumentIMap.class.getName().replaceAll("\\.", "-");

	public DocumentIMap(HazelcastInstance hazelcastInstance, String name) {
		super(hazelcastInstance, name);
	}

	public DocumentIMap(HazelcastInstance hazelcastInstance, String name, ExternalStorageDto externalStorageDto) {
		super(hazelcastInstance, name, externalStorageDto);
	}

	@Override
	public int insert(String key, T data) throws Exception {
		if (!(data instanceof Document)) {
			Document document = new Document(DOCUMENT_KEY, data);
			iMap.put(key, document);
			return 1;
		} else {
			return super.insert(key, data);
		}
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
	public T find(String key) throws Exception {
		Object obj = iMap.get(key);
		if (obj instanceof Document && ((Document) obj).containsKey(DOCUMENT_KEY)) {
			Object data = ((Document) obj).get(DOCUMENT_KEY);
			if (data instanceof Decimal128) {
				data = ((Decimal128) data).bigDecimalValue();
			}
			return (T) data;
		} else {
			return super.find(key);
		}
	}
}
