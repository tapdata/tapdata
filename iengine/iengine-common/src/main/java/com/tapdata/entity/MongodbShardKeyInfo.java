package com.tapdata.entity;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * MongoDB片健信息
 *
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/7/14 下午3:26
 * </pre>
 */
public class MongodbShardKeyInfo {
	private String id;
	private ObjectId lastmodEpoch;
	private Date lastmod;
	private Boolean dropped;
	private List<MongodbShardKey> key;
	private Boolean unique;
	private UUID uuid;

	public String databaseName() {
		return id.split("\\.")[0];
	}

	public String collectionName() {
		return id.split("\\.")[1];
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ObjectId getLastmodEpoch() {
		return lastmodEpoch;
	}

	public void setLastmodEpoch(ObjectId lastmodEpoch) {
		this.lastmodEpoch = lastmodEpoch;
	}

	public Date getLastmod() {
		return lastmod;
	}

	public void setLastmod(Date lastmod) {
		this.lastmod = lastmod;
	}

	public Boolean getDropped() {
		return dropped;
	}

	public void setDropped(Boolean dropped) {
		this.dropped = dropped;
	}

	public List<MongodbShardKey> getKey() {
		return key;
	}

	public void setKey(List<MongodbShardKey> key) {
		this.key = key;
	}

	public Boolean getUnique() {
		return unique;
	}

	public void setUnique(Boolean unique) {
		this.unique = unique;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public String toString() {
		return "MongodbShardKeyInfo{" +
				"id='" + id + '\'' +
				", lastmodEpoch=" + lastmodEpoch +
				", lastmod=" + lastmod +
				", dropped=" + dropped +
				", key=" + key +
				", unique=" + unique +
				", uuid=" + uuid +
				'}';
	}

	public Document toCreateDocument() {
		Map<String, Object> keys = new LinkedHashMap<>();
		for (MongodbShardKey shardKey : getKey()) {
			keys.put(shardKey.getName(), shardKey.getValue());
		}
		return new Document()
				.append("shardCollection", getId())
				.append("key", keys)
				.append("unique", getUnique());
	}

	public static class MongodbShardKey {
		private String name;
		private Object value;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public MongodbShardKey(String name, Object value) {
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "MongodbShardKey{name='" + name + '\'' + ", value=" + value + '}';
		}
	}

	public static MongodbShardKeyInfo parse(Document doc) {
		MongodbShardKeyInfo info = null;
		Document key = doc.get("key", Document.class);
		if (MapUtils.isEmpty(key)) {
			return info;
		}
		Object id = doc.get("_id");
		if (!(id instanceof String) || StringUtils.isBlank((String) id)) {
			return info;
		}

		info = new MongodbShardKeyInfo();
		info.setKey(new ArrayList<>());
		for (Map.Entry<String, Object> en : key.entrySet()) {
			info.getKey().add(new MongodbShardKey(en.getKey(), en.getValue()));
		}
		info.setId((String) id);
		info.setLastmodEpoch(doc.getObjectId("lastmodEpoch"));
		info.setLastmod(doc.getDate("lastmod"));
		info.setDropped(doc.getBoolean("dropped"));
		info.setUnique(doc.getBoolean("unique"));
		info.setUuid(doc.get("uuid", UUID.class));
		return info;
	}
}
