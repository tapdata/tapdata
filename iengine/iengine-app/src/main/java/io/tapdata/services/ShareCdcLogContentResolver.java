package io.tapdata.services;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Engine side RPC service for reading shared CDC log content directly from MongoDB external storage.
 */
@RemoteService
@Slf4j
public class ShareCdcLogContentResolver {
	private static final int DEFAULT_LIMIT = 10;
	private static final String VALUE_PREFIX = "value.";
	private static final String BEFORE_PREFIX = "value.before.";
	private static final String AFTER_PREFIX = "value.after.";

	public ChangeLogData read(ChangeLogCriteria criteria) {
		return resolve(criteria);
	}

	public ChangeLogData resolve(ChangeLogCriteria criteria) {
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		ExternalStorageDto externalStorage = resolveExternalStorage(criteria, clientMongoOperator);
		if (!isMongoExternalStorage(externalStorage)) {
			throw new IllegalArgumentException("Only MongoDB external storage is supported currently");
		}

		TapTable tapTable = TapTableUtil.getTapTableByConnectionId(criteria.getConnectionId(), criteria.getTableName());
		org.bson.conversions.Bson filter = buildMongoFilter(criteria, tapTable);
		return readFromMongo(criteria.getRingBuffer(), externalStorage, filter, normalizedLimit(criteria), tapTable);
	}

	private org.bson.conversions.Bson buildMongoFilter(ChangeLogCriteria criteria, TapTable tapTable) {
		List<org.bson.conversions.Bson> filters = new ArrayList<>();
		filters.add(eq("ringBuffer", criteria.getRingBuffer()));
		filters.add(eq("value.connectionId", criteria.getConnectionId()));
		filters.add(eq("value.fromTable", criteria.getTableName()));
		if (criteria.getKey() != null) {
			filters.add(gt("key", criteria.getKey()));
		}
		if (criteria.getStartTime() > 0) {
			filters.add(gte("value.timestamp", criteria.getStartTime()));
		}
		if (criteria.getEndTime() > 0) {
			filters.add(lte("value.timestamp", criteria.getEndTime()));
		}
		if (criteria.getFilters() != null) {
			for (Map<String, Object> filter : criteria.getFilters()) {
				if (filter == null || filter.isEmpty()) {
					continue;
				}
				for (Map.Entry<String, Object> entry : filter.entrySet()) {
					filters.add(buildRecordFieldFilter(entry.getKey(), entry.getValue(), tapTable));
				}
			}
		}
		return and(filters);
	}

	private org.bson.conversions.Bson buildRecordFieldFilter(String path, Object value, TapTable tapTable) {
		if (StringUtils.isBlank(path)) {
			throw new IllegalArgumentException("Filter path cannot be blank");
		}
		String normalizedPath = path.trim();
		if (StringUtils.startsWith(normalizedPath, VALUE_PREFIX)
				|| "key".equals(normalizedPath)
				|| "ringBuffer".equals(normalizedPath)) {
			return eq(normalizedPath, convertFilterValue(normalizedPath, value, tapTable));
		}
		Object afterValue = convertFilterValue(AFTER_PREFIX + normalizedPath, value, tapTable);
		Object beforeValue = convertFilterValue(BEFORE_PREFIX + normalizedPath, value, tapTable);
		return or(
				eq(AFTER_PREFIX + normalizedPath, afterValue),
				eq(BEFORE_PREFIX + normalizedPath, beforeValue)
		);
	}

	private Object convertFilterValue(String path, Object value, TapTable tapTable) {
		if (value == null) {
			return null;
		}
		String fieldName = extractRecordFieldName(path);
		TapField tapField = getTapField(tapTable, fieldName);
		if (isObjectIdField(fieldName, tapField, value)) {
			return encodeObjectId(String.valueOf(value));
		}
		if (isDateTimeField(tapField)) {
			return encodeInstant(toInstant(value));
		}
		if (tapField != null && tapField.getTapType() instanceof TapBoolean) {
			return toBoolean(value);
		}
		if (tapField != null && tapField.getTapType() instanceof TapNumber) {
			return toNumber(value);
		}
		return value;
	}

	private String extractRecordFieldName(String path) {
		if (StringUtils.startsWith(path, BEFORE_PREFIX)) {
			return StringUtils.substringAfter(path, BEFORE_PREFIX);
		}
		if (StringUtils.startsWith(path, AFTER_PREFIX)) {
			return StringUtils.substringAfter(path, AFTER_PREFIX);
		}
		return null;
	}

	private TapField getTapField(TapTable tapTable, String fieldName) {
		if (tapTable == null || StringUtils.isBlank(fieldName) || tapTable.getNameFieldMap() == null) {
			return null;
		}
		return tapTable.getNameFieldMap().get(StringUtils.substringBefore(fieldName, "."));
	}

	private boolean isObjectIdField(String fieldName, TapField tapField, Object value) {
		if (!org.bson.types.ObjectId.isValid(String.valueOf(value))) {
			return false;
		}
		if ("_id".equals(fieldName)) {
			return true;
		}
		return tapField != null && StringUtils.containsIgnoreCase(tapField.getDataType(), "objectId");
	}

	private boolean isDateTimeField(TapField tapField) {
		return tapField != null && tapField.getTapType() instanceof TapDateTime;
	}

	private byte[] encodeObjectId(String value) {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		byte[] dest = new byte[bytes.length + 2];
		dest[0] = 99;
		dest[dest.length - 1] = 23;
		System.arraycopy(bytes, 0, dest, 1, bytes.length);
		return dest;
	}

	private byte[] encodeInstant(Instant instant) {
		byte[] bytes = instant.toString().getBytes(StandardCharsets.UTF_8);
		byte[] dest = new byte[bytes.length + 2];
		dest[0] = 98;
		dest[dest.length - 1] = 22;
		System.arraycopy(bytes, 0, dest, 1, bytes.length);
		return dest;
	}

	private Instant toInstant(Object value) {
		if (value instanceof Date) {
			return ((Date) value).toInstant();
		}
		if (value instanceof Number) {
			return Instant.ofEpochMilli(((Number) value).longValue());
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

	private ChangeLogData readFromMongo(String ringBuffer, ExternalStorageDto externalStorage, org.bson.conversions.Bson filter, int limit, TapTable tapTable) {
		ChangeLogData changeLogData = new ChangeLogData();
		List<Map<String, Object>> logs = new ArrayList<>();
		ConnectionString connectionString = new ConnectionString(externalStorage.getUri());
		String database = connectionString.getDatabase();
		if (StringUtils.isBlank(database)) {
			throw new IllegalArgumentException("External storage MongoDB uri must include database");
		}
		try (MongoClient mongoClient = MongoClients.create(connectionString)) {
			MongoCollection<Document> collection = mongoClient.getDatabase(database).getCollection(ringBuffer);
			for (Document document : collection.find(filter).limit(limit)) {
				logs.add(buildLog(document, tapTable));
				changeLogData.setLastKey(readLong(document.get("key")));
			}
		}
		changeLogData.setLogs(logs);
		return changeLogData;
	}

	private Map<String, Object> buildLog(Document document, TapTable tapTable) {
		Map<String, Object> log = new LinkedHashMap<>();
		log.put("id", String.valueOf(document.get("_id")));
		log.put("ringBuffer", document.getString("ringBuffer"));
		log.put("key", readLong(document.get("key")));

		Document value = document.get("value", Document.class);
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

	private Map<String, Object> decodeRecord(Map<String, Object> record, TapTable tapTable) {
		if (record == null) {
			return null;
		}
		Map<String, Object> decoded = new LinkedHashMap<>(record.size());
		for (Map.Entry<String, Object> entry : record.entrySet()) {
			decoded.put(entry.getKey(), decodeRecordValue(entry.getKey(), entry.getValue(), tapTable));
		}
		return decoded;
	}

	private Object decodeRecordValue(String fieldName, Object value, TapTable tapTable) {
		byte[] bytes = readBinary(value);
		if (bytes == null || bytes.length < 2) {
			return value;
		}
		TapField tapField = getTapField(tapTable, fieldName);
		if (isEncodedObjectId(fieldName, tapField, bytes)) {
			return decodeMarkedString(bytes);
		}
		if (isDateTimeField(tapField) && isEncodedDateTime(bytes)) {
			return Instant.parse(decodeMarkedString(bytes)).toString();
		}
		return value;
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
		return bytes.length == 26
				&& bytes[0] == 99
				&& bytes[bytes.length - 1] == 23
				&& ("_id".equals(fieldName) || tapField != null && StringUtils.containsIgnoreCase(tapField.getDataType(), "objectId"));
	}

	private boolean isEncodedDateTime(byte[] bytes) {
		return bytes[0] == 98 && bytes[bytes.length - 1] == 22;
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

	private boolean isMongoExternalStorage(ExternalStorageDto externalStorage) {
		return externalStorage != null
				&& StringUtils.equalsIgnoreCase("mongodb", externalStorage.getType())
				&& StringUtils.isNotBlank(externalStorage.getUri());
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
