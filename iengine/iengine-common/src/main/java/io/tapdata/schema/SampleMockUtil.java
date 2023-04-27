package io.tapdata.schema;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.schema.bean.TapFieldEx;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SampleMockUtil {

	private static final Logger logger = LogManager.getLogger(SampleMockUtil.class);

	private static final Map<Class<? extends TapType>, TapValue> sampleDateMap = new ConcurrentHashMap<>();

	static {
		sampleDateMap.put(TapString.class, new TapStringValue("sample string"));
		sampleDateMap.put(TapNumber.class, new TapNumberValue(1d));
		sampleDateMap.put(TapBoolean.class, new TapBooleanValue(true));
		sampleDateMap.put(TapBinary.class, new TapBinaryValue(new byte[]{1, 2, 3}));
		sampleDateMap.put(TapDate.class, new TapDateValue(new DateTime(Instant.now())));
		sampleDateMap.put(TapDateTime.class, new TapDateTimeValue(new DateTime(Instant.now())));
		sampleDateMap.put(TapTime.class, new TapTimeValue(new DateTime(Instant.now())));
		sampleDateMap.put(TapYear.class, new TapYearValue(new DateTime(Instant.now())));
		sampleDateMap.put(TapArray.class, new TapArrayValue(Collections.emptyList()));
		sampleDateMap.put(TapMap.class, new TapMapValue(Collections.emptyMap()));
		sampleDateMap.put(TapRaw.class, new TapRawValue("sample raw"));
	}


	public static void mock(TapTable tapTable, Map<String, Object> map) {
		if (tapTable == null || tapTable.getNameFieldMap() == null || map == null) {
			throw new IllegalArgumentException("tapTable or map is null");
		}
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
			String name = entry.getKey();
			TapField field = entry.getValue();
			if (field instanceof TapFieldEx && ((TapFieldEx) field).isDeleted()) {
				continue;
			}
			Object v = map.get(name);
			if (v == null) {
				TapType type = field.getTapType();
				TapValue tapValue = sampleDateMap.get(type.getClass());
				map.put(name, tapValue);
				logger.info("field {} mock sample --> {}", name, tapValue.getValue());
			}
		}
		Set<String> keySet = new HashSet<>(map.keySet());
		for (String key : keySet) {
			TapField tapField = nameFieldMap.get(key);
			if (tapField == null) {
				MapUtil.removeValueByKey(map, key);
				logger.info("field {} will be removed", key);
			}
		}
	}

	/**
	 * @param tapTable
	 * @param rows
	 * @return
	 */
	public static List<TapdataEvent> mock(TapTable tapTable, int rows) {
		List<TapdataEvent> tapdataEvents = new ArrayList<>();
		for (int i = 0; i < rows; i++) {
			TapdataEvent tapdataEvent = new TapdataEvent();
			Map<String, Object> after = new HashMap<>();
			for (Map.Entry<String, TapField> fieldEntry : tapTable.getNameFieldMap().entrySet()) {
				TapField field = fieldEntry.getValue();
				TapType type = field.getTapType();
				after.put(fieldEntry.getKey(), sampleDateMap.get(type.getClass()));
			}
			TapRecordEvent tapRecordEvent = new TapInsertRecordEvent().init().after(after).table(tapTable.getId());
			tapdataEvent.setTapEvent(tapRecordEvent);
			tapdataEvent.setSyncStage(SyncStage.INITIAL_SYNC);
			tapdataEvents.add(tapdataEvent);
		}
		return tapdataEvents;
	}
}
