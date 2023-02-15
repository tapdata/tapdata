package io.tapdata.entity.event.dml;


import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.map;

public class TapInsertRecordEvent extends TapRecordEvent {
	public static final int TYPE = 300;
	/**
	 * The latest record, after insert and update
	 * Value format should follow TapType formats
	 */
	private Map<String, Object> after;

	//Only for cache, should not be serialized
	private Map<String, Object> filter;
	/*
	public void from(InputStream inputStream) throws IOException {
		super.from(inputStream);
		DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		byte[] afterBytes = dataInputStreamEx.readBytes();
		if(afterBytes != null) {
			after = (Map<String, Object>) objectSerializable.toObject(afterBytes);
		}
	}
	public void to(OutputStream outputStream) throws IOException {
		super.to(outputStream);
		DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		dataOutputStreamEx.writeBytes(objectSerializable.fromObject(after));
	}*/

	public TapInsertRecordEvent() {
		super(TYPE);
	}

	public static TapInsertRecordEvent create() {
		return new TapInsertRecordEvent().init();
	}

	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapInsertRecordEvent) {
			TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) tapEvent;
			if (after != null)
				insertRecordEvent.after = InstanceFactory.instance(TapUtils.class).cloneMap(after);
		}
	}

	public TapInsertRecordEvent init() {
		time = System.currentTimeMillis();
		return this;
	}

	public TapInsertRecordEvent referenceTime(Long referenceTime) {
		this.referenceTime = referenceTime;
		return this;
	}

	public TapInsertRecordEvent after(Map<String, Object> after) {
		this.after = after;
		return this;
	}

	public TapInsertRecordEvent table(String table) {
		this.tableId = table;
		return this;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public Map<String, Object> getFilter(Collection<String> primaryKeys) {
		if(primaryKeys == null || primaryKeys.isEmpty())
			throw new CoreException(TapAPIErrorCodes.ERROR_NO_PRIMARY_KEYS, "TapInsertRecordEvent: no primary keys for tableId {} after {}", tableId, after);
		if(filter == null) {
			filter = map();
			for(String key : primaryKeys) {
				Object value = null;
				if(after != null)
					value = after.get(key);
				if(value != null)
					filter.put(key, value);
				else
					throw new CoreException(TapAPIErrorCodes.ERROR_MISSING_PRIMARY_VALUE, "TapInsertRecordEvent: primary key {} is missing value from after {}, all primary keys {} tableId {}", key, after, primaryKeys, tableId);
			}
		}
		return filter;
	}

//	@Override
//	public String toString() {
//		return "TapInsertRecordEvent{" +
//				"after=" + after +
//				"} " + super.toString();
//	}
}
