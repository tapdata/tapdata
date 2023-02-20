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

public class TapDeleteRecordEvent extends TapRecordEvent {
	public static final int TYPE = 301;
	private Map<String, Object> filter;
	private Map<String, Object> before;
	/*
	public void from(InputStream inputStream) throws IOException {
		super.from(inputStream);
		DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		byte[] beforeBytes = dataInputStreamEx.readBytes();
		if(beforeBytes != null) {
			before = (Map<String, Object>) objectSerializable.toObject(beforeBytes);
		}
	}
	public void to(OutputStream outputStream) throws IOException {
		super.to(outputStream);
		DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
		dataOutputStreamEx.writeBytes(objectSerializable.fromObject(before));
	}*/
	public TapDeleteRecordEvent() {
		super(TYPE);
	}

	public static TapDeleteRecordEvent create() {
		return new TapDeleteRecordEvent().init();
	}
	@Override
	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapDeleteRecordEvent) {
			TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) tapEvent;
			if (before != null)
				deleteRecordEvent.before = InstanceFactory.instance(TapUtils.class).cloneMap(before);
		}
	}

	public TapDeleteRecordEvent init() {
		time = System.currentTimeMillis();
		return this;
	}

	public TapDeleteRecordEvent referenceTime(Long referenceTime) {
		this.referenceTime = referenceTime;
		return this;
	}

	public TapDeleteRecordEvent before(Map<String, Object> before) {
		this.before = before;
		return this;
	}

	public TapDeleteRecordEvent table(String table) {
		this.tableId = table;
		return this;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public Map<String, Object> getFilter(Collection<String> primaryKeys) {
		if(primaryKeys == null || primaryKeys.isEmpty())
			throw new CoreException(TapAPIErrorCodes.ERROR_NO_PRIMARY_KEYS, "TapDeleteRecordEvent: no primary keys for tableId {} before {}", tableId, before);
		if(filter == null) {
			filter = map();
			for(String key : primaryKeys) {
				Object value = null;
				if(before != null)
					value = before.get(key);
				if(value != null)
					filter.put(key, value);
				else
					throw new CoreException(TapAPIErrorCodes.ERROR_MISSING_PRIMARY_VALUE, "TapDeleteRecordEvent: primary key {} is missing value from before {}, all primary keys {} tableId {}", key, before, primaryKeys, tableId);
			}
		}
		return filter;
	}

	//	@Override
//	public String toString() {
//		return "TapDeleteRecordEvent{" +
//				"before=" + before +
//				"} " + super.toString();
//	}
}
