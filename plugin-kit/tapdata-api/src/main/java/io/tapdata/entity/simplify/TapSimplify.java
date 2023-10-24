package io.tapdata.entity.simplify;

import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.field.DeleteFieldItem;
import io.tapdata.entity.event.ddl.field.InsertFieldItem;
import io.tapdata.entity.event.ddl.field.UpdateFieldItem;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TapSimplify {
	private static TapUtils tapUtils;
	private static JsonParser jsonParser;

	public static void interval(Runnable runnable, int seconds) {
		tapUtils().interval(runnable, seconds);
	}

	public static String getStackTrace(Throwable throwable) {
		return tapUtils().getStackTrace(throwable);
	}


	public static String toJsonWithClass(Object obj) {
		return jsonParser().toJsonWithClass(obj);
	}

	public static Object fromJsonWithClass(String json) {
		return jsonParser().fromJsonWithClass(json);
	}

	public static JsonParser jsonParser() {
		if(jsonParser == null) {
			jsonParser = InstanceFactory.instance(JsonParser.class);
		}
		return jsonParser;
	}

	public static TapUtils tapUtils() {
		if(tapUtils == null) {
			tapUtils = InstanceFactory.instance(TapUtils.class);
		}
		return tapUtils;
	}

	public static String toJson(Object obj, JsonParser.ToJsonFeature... features) {
		return jsonParser().toJson(obj, features);
	}

	public static Object fromJson(String json) {
		return jsonParser().fromJson(json);
	}

	public static DataMap fromJsonObject(String json) {
		return jsonParser().fromJsonObject(json);
	}

	public static List<?> fromJsonArray(String json) {
		return jsonParser().fromJsonArray(json);
	}

	public static <T> T fromJson(String json, Class<T> clazz) {
		return jsonParser().fromJson(json, clazz);
	}

	public static <T> T fromJson(String json, Type clazz) {
		return jsonParser.fromJson(json, clazz);
	}

	public static String format(String message, Object... args) {
		return FormatUtils.format(message, args);
	}

	public static TapField field(String name, String type) {
		return new TapField(name, type);
	}

	public static TapTable table(String tableName, String id) {
		return new TapTable(tableName, id);
	}

	public static TapTable table(String nameAndId) {
		return new TapTable(nameAndId);
	}

	public static TapString tapString() {
		return new TapString();
	}

	public static TapNumber tapNumber() {
		return new TapNumber();
	}

	public static TapRaw tapRaw() {
		return new TapRaw();
	}

	public static TapArray tapArray() {
		return new TapArray();
	}

	public static TapMap tapMap() {
		return new TapMap();
	}

	public static TapYear tapYear() {
		return new TapYear();
	}

	public static TapDate tapDate() {
		return new TapDate();
	}

	public static TapBoolean tapBoolean() {
		return new TapBoolean();
	}

	public static TapBinary tapBinary() {
		return new TapBinary();
	}

	public static TapTime tapTime() {
		return new TapTime();
	}

	public static TapDateTime tapDateTime() {
		return new TapDateTime();
	}

	public static Entry entry(String key, Object value) {
		return new Entry(key, value);
	}

	public static <T> List<T> list(T... ts) {
		return new ArrayList<>(Arrays.asList(ts));
	}

	public static <T> List<T> list() {
		return new ArrayList<T>();
	}

	public static Map<String, Object> map() {
		return new LinkedHashMap<>();
	}

	public static Map<String, Object> map(Entry... entries) {
		Map<String, Object> map = new LinkedHashMap<>();
		if (entries != null) {
			for (Entry entry : entries) {
				if (entry.getKey() != null && entry.getValue() != null)
					map.put(entry.getKey(), entry.getValue());
			}
		}
		return map;
	}

	public static TapDropTableEvent dropTableEvent(String tableId) {
		TapDropTableEvent dropTableEvent = new TapDropTableEvent();
		dropTableEvent.setTime(System.currentTimeMillis());
		dropTableEvent.setTableId(tableId);
		return dropTableEvent;
	}

	public static TapCreateTableEvent createTableEvent(TapTable table) {
		TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
		createTableEvent.setTime(System.currentTimeMillis());
		createTableEvent.setTable(table);
		createTableEvent.setTableId(table.getId());
		return createTableEvent;
	}

	public static TapClearTableEvent clearTableEvent(String tableId) {
		TapClearTableEvent clearTableEvent = new TapClearTableEvent();
		clearTableEvent.setTime(System.currentTimeMillis());
		clearTableEvent.setTableId(tableId);
		return clearTableEvent;
	}

	public static DeleteFieldItem deleteFieldItem() {
		return new DeleteFieldItem();
	}

	public static InsertFieldItem insertFieldItem() {
		return new InsertFieldItem();
	}

	public static UpdateFieldItem updateFieldItem() {
		return new UpdateFieldItem();
	}

	public static TapCreateIndexEvent createIndexEvent(String tableId, List<TapIndex> indexList) {
		TapCreateIndexEvent createIndexEvent = new TapCreateIndexEvent();
		createIndexEvent.setTime(System.currentTimeMillis());
		createIndexEvent.setTableId(tableId);
		createIndexEvent.setIndexList(indexList);
		return createIndexEvent;
	}

	public static TapDeleteIndexEvent deleteIndexEvent(String tableId, List<String> indexNames) {
		TapDeleteIndexEvent deleteIndexEvent = new TapDeleteIndexEvent();
		deleteIndexEvent.setTableId(tableId);
		deleteIndexEvent.setTime(System.currentTimeMillis());
		deleteIndexEvent.setIndexNames(indexNames);
		return deleteIndexEvent;
	}

	public static TapInsertRecordEvent insertRecordEvent(Map<String, Object> after, String table) {
		return new TapInsertRecordEvent().init().after(after).table(table);
	}

	public static TapAlterDatabaseTimezoneEvent alterDatabaseTimeZoneEvent() {
		return new TapAlterDatabaseTimezoneEvent();
	}

	public static <T> FieldAttrChange<T> fieldAttrChange(String name, T after) {
		return FieldAttrChange.create(name, after);
	}

	public static TapAlterFieldAttributesEvent alterFieldAttributesEvent() {
		return new TapAlterFieldAttributesEvent();
	}

	public static TapAlterFieldNameEvent alterFieldNameEvent() {
		return new TapAlterFieldNameEvent();
	}

	public static TapAlterFieldPrimaryKeyEvent alterFieldPrimaryKeyEvent() {
		return new TapAlterFieldPrimaryKeyEvent();
	}

	public static TapAlterTableCharsetEvent alterTableCharsetEvent() {
		return new TapAlterTableCharsetEvent();
	}

	public static TapClearTableEvent clearTableEvent() {
		return new TapClearTableEvent();
	}

	public static TapCreateTableEvent createTableEvent() {
		return new TapCreateTableEvent();
	}

	public static TapDropFieldEvent dropFieldEvent() {
		return new TapDropFieldEvent();
	}

	public static TapDropTableEvent dropTableEvent()  {
		return new TapDropTableEvent();
	}

	public static TapNewFieldEvent newFieldEvent() {
		return new TapNewFieldEvent();
	}

	public static TapDeleteRecordEvent deleteDMLEvent(Map<String, Object> before, String table) {
		return new TapDeleteRecordEvent().init().before(before).table(table);
	}

	public static TapUpdateRecordEvent updateDMLEvent(Map<String, Object> before, Map<String, Object> after, String table) {
		return new TapUpdateRecordEvent().init().before(before).after(after).table(table);
	}

	public static HeartbeatEvent heartbeatEvent(Long referenceTime) {
		return new HeartbeatEvent().init().referenceTime(referenceTime);
	}

	public static TapIndex index(String name) {
		return new TapIndex().name(name);
	}

	public static TapIndexField indexField(String name) {
		return new TapIndexField().name(name);
	}

	public static void sleep(long milliseconds) {
		if (milliseconds < 0)
			return;
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException interruptedException) {
//			interruptedException.printStackTrace();
		}
	}

	public static Object convertDateTimeToDate(DateTime dateTime) {
		if (dateTime != null) {
			Long milliseconds;
			Integer nano = dateTime.getNano();
			Long seconds = dateTime.getSeconds();
			if (seconds != null) {
				milliseconds = seconds * 1000;
				if (nano != null) {
					milliseconds += milliseconds + (nano / 1000 / 1000);
				}
			} else {
				return null;
			}
			return new Date(milliseconds);
		}
		return null;
	}

	public static String getStackString(Throwable throwable) {
		StringWriter sw = new StringWriter();
		try (
				PrintWriter pw = new PrintWriter(sw)
		) {
			throwable.printStackTrace(pw);
			return sw.toString();
		}
	}

	public static String formatTapDateTime(DateTime dateTime, String pattern) {
		try {
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
			LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTime.toInstant(), ZoneId.of("GMT"));
			return dateTimeFormatter.format(localDateTime);
		} catch (Throwable e) {
			throw new RuntimeException("Parse date time " + dateTime + " pattern " + pattern + ", failed, " + e.getMessage(), e);
		}
	}
}
