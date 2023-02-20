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
import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.map;

public class TapUpdateRecordEvent extends TapRecordEvent {
	public static final int TYPE = 302;
	/**
	 * The latest record, after insert and update
	 * Value format should follow TapType formats
	 */
	private Map<String, Object> after;
	/*
	public void from(InputStream inputStream) throws IOException {
		super.from(inputStream);
		DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		byte[] afterBytes = dataInputStreamEx.readBytes();
		if(afterBytes != null) {
			after = (Map<String, Object>) objectSerializable.toObject(afterBytes);
		}
		byte[] beforeBytes = dataInputStreamEx.readBytes();
		if(beforeBytes != null) {
			before = (Map<String, Object>) objectSerializable.toObject(beforeBytes);
		}
	}
	public void to(OutputStream outputStream) throws IOException {
		super.to(outputStream);
		DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		dataOutputStreamEx.writeBytes(objectSerializable.fromObject(after));
		dataOutputStreamEx.writeBytes(objectSerializable.fromObject(before));
	}
	*/
	public TapUpdateRecordEvent() {
		super(TYPE);
	}

	public static TapUpdateRecordEvent create() {
		return new TapUpdateRecordEvent().init();
	}

	public TapUpdateRecordEvent after(Map<String, Object> after) {
		this.after = after;
		return this;
	}

	public TapUpdateRecordEvent table(String table) {
		this.tableId = table;
		return this;
	}

	public TapUpdateRecordEvent init() {
		time = System.currentTimeMillis();
		return this;
	}

	public TapUpdateRecordEvent referenceTime(Long referenceTime) {
		this.referenceTime = referenceTime;
		return this;
	}

	/**
	 * The last record, especially before update and delete
	 * Value format should follow TapType formats
	 */
	private Map<String, Object> before;

	public TapUpdateRecordEvent before(Map<String, Object> before) {
		this.before = before;
		return this;
	}

	private Map<String, Object> filter;

	@Override
	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapUpdateRecordEvent) {
			TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) tapEvent;
			if (before != null)
				updateRecordEvent.before = InstanceFactory.instance(TapUtils.class).cloneMap(before);
			if (after != null)
				updateRecordEvent.after = InstanceFactory.instance(TapUtils.class).cloneMap(after);
		}
	}


	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public void putAfterValue(String name, Object value) {
		if (this.after == null) {
			this.after = new LinkedHashMap<>();
		}
		this.after.put(name, value);
	}

	public void removeAfterValue(String name) {
		if (this.after != null) {
			this.after.remove(name);
		}
	}

	public Map<String, Object> getFilter(Collection<String> primaryKeys) {
		if(primaryKeys == null || primaryKeys.isEmpty())
			throw new CoreException(TapAPIErrorCodes.ERROR_NO_PRIMARY_KEYS, "TapUpdateRecordEvent: no primary keys for tableId {} before {} after {}", tableId, before, after);
		if(filter == null) {
			filter = map();
			for(String key : primaryKeys) {
				Object value = null;
				if(before != null)
					value = before.get(key);
				if(after != null)
					value = after.get(key);
				if(value != null)
					filter.put(key, value);
				else
					throw new CoreException(TapAPIErrorCodes.ERROR_MISSING_PRIMARY_VALUE, "TapUpdateRecordEvent: primary key {} is missing value from before {} or after {}, all primary keys {} tableId {}", key, before, after, primaryKeys, tableId);
			}
		}
		return filter;
	}

	//	@Override
//	public String toString() {
//		return "TapUpdateRecordEvent{" +
//				"after=" + after +
//				", before=" + before +
//				"} " + super.toString();
//	}
}
