package io.tapdata.construct.constructImpl;

import com.hazelcast.persistence.store.MultiReferenceMap;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.HazelcastConstruct;

/**
 * @author samuel
 * @Description
 * @create 2022-02-09 14:39
 **/
public class BaseConstruct<T> implements HazelcastConstruct<T> {

	protected final String referenceId;
	protected String name;
	protected Integer ttlSecond;
	protected ExternalStorageDto externalStorageDto;

	protected BaseConstruct(String name) {
		this(MultiReferenceMap.DEFAULT_REFERENCE_ID, name);
	}

	protected BaseConstruct(String referenceId, String name) {
		this.referenceId = referenceId;
		this.name = name;
	}

	protected BaseConstruct(String referenceId, String name, ExternalStorageDto externalStorageDto) {
		this(referenceId, name);
		this.externalStorageDto = externalStorageDto;
	}

	protected void convertTtlDay2Second(Integer ttlDay) {
		if (ttlDay != null && ttlDay > 0) {
			this.ttlSecond = ttlDay * 24 * 60 * 60;
		}
	}
}
