package io.tapdata.schema;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.schema.bean.TapFieldEx;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;
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
    sampleDateMap.put(TapArray.class, new TapArrayValue(Collections.singletonList("sample array")));
    sampleDateMap.put(TapMap.class, new TapMapValue(Collections.singletonMap("sample map key", "sample map value")));
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
   *
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
