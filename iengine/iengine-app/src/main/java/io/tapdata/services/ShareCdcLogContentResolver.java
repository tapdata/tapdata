package io.tapdata.services;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.RingBufferFindParam;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.trace.ChangeLogCriteria;
import com.tapdata.tm.commons.trace.ChangeLogData;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Engine side RPC service for reading shared CDC log content from external storage.
 */
@RemoteService
@Slf4j
public class ShareCdcLogContentResolver {
	private static final int DEFAULT_LIMIT = 10;

	public ChangeLogData read(ChangeLogCriteria criteria) {
		return resolve(criteria);
	}

	public ChangeLogData resolve(ChangeLogCriteria criteria) {
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		ExternalStorageDto externalStorage = resolveExternalStorage(criteria, clientMongoOperator);
		TapTable tapTable = TapTableUtil.getTapTableByConnectionId(criteria.getConnectionId(), criteria.getTableName());
		ChangeLogReadContext context = new ChangeLogReadContext(criteria, externalStorage, tapTable, normalizedLimit(criteria));
		return readFromExternalStorage(context, buildQuery(criteria, tapTable));
	}

	private RingBufferFindParam buildQuery(ChangeLogCriteria criteria, TapTable tapTable) {
		RingBufferFindParam query = new RingBufferFindParam();
		query.setRingBuffer(criteria.getRingBuffer());
		query.setConnectionId(criteria.getConnectionId());
		query.setTableName(criteria.getTableName());
		query.setKey(criteria.getKey());
		query.setStartTime(criteria.getStartTime());
		query.setEndTime(criteria.getEndTime());
		query.setFilters(encodeFilters(criteria.getFilters(), tapTable));
		return query;
	}

	private List<Map<String, Object>> encodeFilters(List<Map<String, Object>> filters, TapTable tapTable) {
		if (filters == null) {
			return null;
		}
		List<Map<String, Object>> encoded = new ArrayList<>(filters.size());
		for (Map<String, Object> filter : filters) {
			if (filter == null) {
				continue;
			}
			Map<String, Object> encodedFilter = new LinkedHashMap<>(filter.size());
			for (Map.Entry<String, Object> entry : filter.entrySet()) {
				encodedFilter.put(entry.getKey(), encodeFilterValue(entry.getKey(), entry.getValue(), tapTable));
			}
			encoded.add(encodedFilter);
		}
		return encoded;
	}

	private Object encodeFilterValue(String fieldName, Object value, TapTable tapTable) {
		if (value == null) {
			return null;
		}
		TapField tapField = getTapField(tapTable, fieldName);
		if (isObjectIdFilterField(fieldName, tapField, value)) {
			return encodeMarkedString(String.valueOf(value), (byte) 99, (byte) 23);
		}
		if (isDateTimeField(tapField)) {
			return encodeMarkedString(toInstant(value).toString(), (byte) 98, (byte) 22);
		}
		if (isTapType(tapField, TapBoolean.class)) {
			return toBoolean(value);
		}
		if (isTapType(tapField, TapNumber.class)) {
			return toNumber(value);
		}
		return value;
	}

	private boolean isObjectIdFilterField(String fieldName, TapField tapField, Object value) {
		if (!org.bson.types.ObjectId.isValid(String.valueOf(value))) {
			return false;
		}
		return "_id".equals(fieldName)
				|| "_id".equals(StringUtils.substringAfterLast(fieldName, "."))
				|| (tapField != null && StringUtils.containsIgnoreCase(tapField.getDataType(), "objectId"));
	}

	private boolean isDateTimeField(TapField tapField) {
		return tapField != null && tapField.getTapType() instanceof TapDateTime;
	}

	private boolean isTapType(TapField tapField, Class<?> tapTypeClass) {
		return tapField != null && tapTypeClass.isInstance(tapField.getTapType());
	}

	private byte[] encodeMarkedString(String value, byte head, byte tail) {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		byte[] dest = new byte[bytes.length + 2];
		dest[0] = head;
		dest[dest.length - 1] = tail;
		System.arraycopy(bytes, 0, dest, 1, bytes.length);
		return dest;
	}

	private Instant toInstant(Object value) {
		if (value instanceof Date date) {
			return date.toInstant();
		}
		if (value instanceof Number number) {
			return Instant.ofEpochMilli(number.longValue());
		}
		String text = String.valueOf(value);
		try {
			return Instant.parse(text);
		} catch (DateTimeParseException e) {
			return Instant.parse(text.replace(" ", "T") + (StringUtils.endsWith(text, "Z") ? "" : "Z"));
		}
	}

	private Object toBoolean(Object value) {
		if (value instanceof Boolean) {
			return value;
		}
		return Boolean.parseBoolean(String.valueOf(value));
	}

	private Object toNumber(Object value) {
		if (value instanceof Number) {
			return value;
		}
		String text = String.valueOf(value);
		if (StringUtils.containsAny(text, ".", "e", "E")) {
			return new BigDecimal(text);
		}
		return Long.parseLong(text);
	}

	private TapField getTapField(TapTable tapTable, String fieldName) {
		if (tapTable == null || StringUtils.isBlank(fieldName) || tapTable.getNameFieldMap() == null) {
			return null;
		}
		return tapTable.getNameFieldMap().get(StringUtils.substringBefore(fieldName, "."));
	}


	private Map<String, Object> buildLog(Map<String, Object> document, TapTable tapTable) {
		Map<String, Object> log = new LinkedHashMap<>();
		log.put("ringBuffer", String.valueOf(document.get("ringBuffer")));
		log.put("key", readLong(document.get("key")));

		Document value = toDocument(document.get("value"));
		if (value == null) {
			log.put("missingContent", true);
			return log;
		}
		LogContent logContent = LogContent.valueOf(value);
		log.put("fromTable", logContent.getFromTable());
		log.put("tableNamespaces", logContent.getTableNamespaces());
		log.put("timestamp", logContent.getTimestamp());
		log.put("before", decodeRecord(logContent.getBefore(), tapTable));
		log.put("after", decodeRecord(logContent.getAfter(), tapTable));
		log.put("op", logContent.getOp());
		log.put("type", logContent.getType());
		log.put("connectionId", StringUtils.defaultIfBlank(logContent.getConnectionId(), value.getString("connectionId")));
		return log;
	}

	private Document toDocument(Object value) {
		if (value instanceof Document document) {
			return document;
		}
		if (value instanceof Map<?, ?> map) {
			Document document = new Document();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				document.put(String.valueOf(entry.getKey()), entry.getValue());
			}
			return document;
		}
		return null;
	}

	private Map<String, Object> decodeRecord(Map<String, Object> record, TapTable tapTable) {
		if (record == null) {
			return null;
		}
		return decodeMap(record, tapTable, null);
	}

	private Object decodeRecordValue(String fieldName, Object value, TapTable tapTable) {
		if (value instanceof Map) {
			return decodeMap((Map<?, ?>) value, tapTable, fieldName);
		}
		if (value instanceof Collection) {
			List<Object> decoded = new ArrayList<>();
			for (Object item : (Collection<?>) value) {
				decoded.add(decodeRecordValue(fieldName, item, tapTable));
			}
			return decoded;
		}
		byte[] bytes = readBinary(value);
		if (bytes == null || bytes.length < 2) {
			return value;
		}
		TapField tapField = getTapField(tapTable, fieldName);
		if (isEncodedObjectId(fieldName, tapField, bytes)) {
			return decodeMarkedString(bytes);
		}
		if (isEncodedDateTime(bytes)) {
			return decodeInstant(bytes, value);
		}
		return value;
	}

	private Map<String, Object> decodeMap(Map<?, ?> record, TapTable tapTable, String parentFieldName) {
		Map<String, Object> decoded = new LinkedHashMap<>(record.size());
		for (Map.Entry<?, ?> entry : record.entrySet()) {
			String fieldName = String.valueOf(entry.getKey());
			String nestedFieldName = StringUtils.isBlank(parentFieldName)
					? fieldName
					: parentFieldName + "." + fieldName;
			decoded.put(fieldName, decodeRecordValue(nestedFieldName, entry.getValue(), tapTable));
		}
		return decoded;
	}

	private byte[] readBinary(Object value) {
		if (value instanceof Binary) {
			return ((Binary) value).getData();
		}
		if (value instanceof byte[]) {
			return (byte[]) value;
		}
		return null;
	}

	private boolean isEncodedObjectId(String fieldName, TapField tapField, byte[] bytes) {
		if (bytes.length != 26 || bytes[0] != 99 || bytes[bytes.length - 1] != 23) {
			return false;
		}
		String decoded = decodeMarkedString(bytes);
		return org.bson.types.ObjectId.isValid(decoded)
				&& ("_id".equals(StringUtils.substringAfterLast(fieldName, "."))
				|| tapField != null && StringUtils.containsIgnoreCase(tapField.getDataType(), "objectId")
				|| "_id".equals(fieldName));
	}

	private boolean isEncodedDateTime(byte[] bytes) {
		return bytes[0] == 98 && bytes[bytes.length - 1] == 22;
	}

	private Object decodeInstant(byte[] bytes, Object originalValue) {
		try {
			return Instant.parse(decodeMarkedString(bytes)).toString();
		} catch (DateTimeParseException e) {
			return originalValue;
		}
	}

	private String decodeMarkedString(byte[] bytes) {
		return new String(bytes, 1, bytes.length - 2, StandardCharsets.UTF_8);
	}

	private Long readLong(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		return Long.parseLong(String.valueOf(value));
	}

	private int normalizedLimit(ChangeLogCriteria criteria) {
		return criteria.getLimit() <= 0 ? DEFAULT_LIMIT : criteria.getLimit();
	}

	private ChangeLogData readFromExternalStorage(ChangeLogReadContext context, RingBufferFindParam query) {
		ChangeLogData changeLogData = new ChangeLogData();
		List<Map<String, Object>> logs = new ArrayList<>();
		PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = null;
		try {
			ExternalStorageDto externalStorage = copyExternalStorage(context.externalStorage);
			if(null == externalStorage) {
				throw new UnsupportedOperationException("externalStorage no found");
			}
			externalStorage.setTable(context.criteria.getRingBuffer());
			PersistenceStorageAbstractConfig storageConfig = ExternalStorageUtil.getPersistenceConfig(
					externalStorage,
					ConstructType.RINGBUFFER,
					context.criteria.getRingBuffer()
			);
			store = PersistenceStorage.getInstance().createStore(storageConfig);
			if (store == null) {
				throw new UnsupportedOperationException("Unsupported change log external storage type: "
						+ externalStorage.getType());
			}
			for (Map<String, Object> document : store.find(query, context.limit)) {
				logs.add(buildLog(document, context.tapTable));
				changeLogData.setLastKey(readLong(document.get("key")));
			}
		} catch (Exception e) {
			throw new RuntimeException("Read share CDC log from external storage failed", e);
		} finally {
			if (store != null) {
				CommonUtils.ignoreAnyError(store::doDestroy);
			}
		}
		changeLogData.setLogs(logs);
		return changeLogData;
	}

	private static class ChangeLogReadContext {
		private final ChangeLogCriteria criteria;
		private final ExternalStorageDto externalStorage;
		private final TapTable tapTable;
		private final int limit;

		private ChangeLogReadContext(ChangeLogCriteria criteria, ExternalStorageDto externalStorage, TapTable tapTable, int limit) {
			this.criteria = criteria;
			this.externalStorage = externalStorage;
			this.tapTable = tapTable;
			this.limit = limit;
		}
	}

	private ExternalStorageDto resolveExternalStorage(ChangeLogCriteria criteria, ClientMongoOperator clientMongoOperator) {
		ExternalStorageDto externalStorageDto = null;
		if (StringUtils.isNotBlank(criteria.getExternalStorageId())) {
			externalStorageDto = clientMongoOperator.findOne(
					Query.query(where("_id").is(criteria.getExternalStorageId())),
					ConnectorConstant.EXTERNAL_STORAGE_COLLECTION,
					ExternalStorageDto.class
			);
		}
		if (externalStorageDto == null) {
			Connections connection = clientMongoOperator.findOne(
					Query.query(where("_id").is(criteria.getConnectionId())),
					ConnectorConstant.CONNECTION_COLLECTION,
					Connections.class
			);
			if (connection != null && StringUtils.isNotBlank(connection.getShareCDCExternalStorageId())) {
				externalStorageDto = clientMongoOperator.findOne(
						Query.query(where("_id").is(connection.getShareCDCExternalStorageId())),
						ConnectorConstant.EXTERNAL_STORAGE_COLLECTION,
						ExternalStorageDto.class
				);
			}
		}
		if (externalStorageDto == null) {
			externalStorageDto = ExternalStorageUtil.getDefaultExternalStorage();
		}
		return copyExternalStorage(externalStorageDto);
	}

	private ExternalStorageDto copyExternalStorage(ExternalStorageDto source) {
		if (source == null) {
			return null;
		}
		ExternalStorageDto target = new ExternalStorageDto();
		BeanUtils.copyProperties(source, target);
		return target;
	}
}
