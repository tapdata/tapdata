package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;

import com.tapdata.constant.MapUtil;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MongoSnapshotRecord implements SnapshotRecord {

	private final Document doc;
	private final Map<String, Object> dataMap;
	private boolean isAppend;
	private boolean isIgnoreCount = false;

	private MongoSnapshotRecord(Map<String, Object> dataMap) {
		this.doc = new Document(dataMap);
		this.dataMap = dataMap;
		this.isAppend = true;
	}

	public MongoSnapshotRecord(List<String> primaryKeyFieldList, Map<String, Object> dataMap) {
		if (primaryKeyFieldList.size() > 1) {
			LinkedHashMap<String, Object> idMap = new LinkedHashMap<>();
			primaryKeyFieldList.forEach(field -> idMap.put(field, dataMap.get(field)));
			dataMap.put("_id", idMap);
		} else {
			dataMap.put("_id", dataMap.get(primaryKeyFieldList.get(0)));
		}
		this.doc = new Document(dataMap);
		this.dataMap = dataMap;
		this.isAppend = true;
	}

	public MongoSnapshotRecord(Document doc) {
		this.doc = doc;
		this.dataMap = doc;
		this.isAppend = false;
	}

	@Override
	public <T> T getRecord() {
		return (T) doc;
	}

	@Override
	public <T> T getRecordID() {
		return this.getRecordValue("_id");
	}

	@Override
	public <T> T getRecordValue(String field) {
		return (T) MapUtil.getValueByKey(this.doc, field);
	}

	@Override
	public Object setRecordValue(String field, Object value) {
		Object old = MapUtil.getValueByKey(this.doc, field);
		try {
			MapUtil.putValueInMap(this.doc, field, value);
		} catch (Exception e) {
			throw new RuntimeException("put snapshot record value exception", e);
		}
		return old;
	}

	@Override
	public Map<String, Object> getDataMap() {
		return dataMap;
	}

	@Override
	public boolean isAppend() {
		return this.isAppend;
	}

	@Override
	public boolean isIngnoreCount() {
		return isIgnoreCount;
	}

	@Override
	public void setAppend(boolean isAppend) {
		this.isAppend = isAppend;
	}

	@Override
	public void setIgnoreCount(boolean isIgnoreCount) {
		this.isIgnoreCount = isIgnoreCount;
	}

	@Override
	public SnapshotRecord clone() {
		return new MongoSnapshotRecord(new LinkedHashMap<>(this.dataMap));
	}
}
