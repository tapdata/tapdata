package io.tapdata.construct.constructImpl;

import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.HazelcastConstruct;

/**
 * @author samuel
 * @Description
 * @create 2022-02-09 14:39
 **/
public class BaseConstruct<T> implements HazelcastConstruct<T> {

	protected String name;
	protected Integer ttlSecond;
	protected ExternalStorageDto externalStorageDto;

	protected BaseConstruct(String name) {
		this.name = name;
	}

	protected BaseConstruct(String name, ExternalStorageDto externalStorageDto) {
		this.name = name;
		this.externalStorageDto = externalStorageDto;
	}

	protected void convertTtlDay2Second(Integer ttlDay) {
		if (ttlDay != null && ttlDay > 0) {
			this.ttlSecond = ttlDay * 24 * 60 * 60;
		}
	}
}
