package com.tapdata.processor.util;

import com.tapdata.entity.schema.SchemaApplyResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TapModelDeclare {

  private static final Logger logger = LogManager.getLogger(TapModelDeclare.class);

  private TapModelDeclare() {

  }

  public static void addField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    addField(tapTable, fieldName, tapType, null);
  }

  public static void addField(List<SchemaApplyResult> schemaApplyResult, String fieldName, String tapType) throws Throwable {
    addField(schemaApplyResult, fieldName, tapType, null);
  }

  public static void addField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    if (tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field {} already exists in table {}", fieldName, tapTable.getName());
      return;
    }
    TapField tapField = getTapField(fieldName, tapType, dataType);
    tapTable.add(tapField);
  }

  public static void addField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    Optional<SchemaApplyResult> optional = schemaApplyResultList.stream()
            .filter(schemaApplyResult -> StringUtils.equals(fieldName, schemaApplyResult.getFieldName())).findFirst();
    if (optional.isPresent()) {
      SchemaApplyResult schemaApplyResult = optional.get();
      if (!StringUtils.equals(schemaApplyResult.getOp(), SchemaApplyResult.OP_TYPE_REMOVE)) {
        logger.warn("field {} already exists in schemaApplyResultList", fieldName);
        return;
      }
      schemaApplyResultList.removeIf(result -> StringUtils.equals(result.getFieldName(), fieldName));
    }
    schemaApplyResultList.add(
            new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CREATE, fieldName,
                    getTapField(fieldName, tapType, dataType)));
  }

  public static void updateField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    tapTable.add(getTapField(fieldName, tapType, null));
  }

  public static void updateField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }

    tapTable.add(getTapField(fieldName, tapType, dataType));
  }

  public static void updateField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, null);
  }
  public static void updateField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    schemaApplyResultList.removeIf(schemaApplyResult -> StringUtils.equals(fieldName, schemaApplyResult.getFieldName()));
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CONVERT, fieldName,
            getTapField(fieldName, tapType, dataType)));
  }

  public static void upsertField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    tapTable.add(getTapField(fieldName, tapType, null));
  }
  public static void upsertField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    tapTable.add(getTapField(fieldName, tapType, dataType));
  }

  public static void upsertField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, null);
  }
  public static void upsertField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, dataType);
  }

  public static void removeField(TapTable tapTable, String fieldName) {
    tapTable.getNameFieldMap().remove(fieldName);
  }

  public static void removeField(List<SchemaApplyResult> schemaApplyResultList, String fieldName) {
    schemaApplyResultList.removeIf(schemaApplyResult -> schemaApplyResult.getFieldName().equals(fieldName));
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_REMOVE, fieldName, null));
  }

  public static void setPk(TapTable tapTable, String fieldName) {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }
    Optional<TapField> optionalTapField = tapTable.getNameFieldMap().entrySet().stream().filter(t -> StringUtils.equals(fieldName, t.getKey())).map(t -> t.getValue())
            .peek(f -> f.setPrimaryKey(true)).findFirst();

    optionalTapField.ifPresent(f-> tapTable.getNameFieldMap().put(fieldName, f));
  }

  public static void unSetPk(TapTable tapTable, String fieldName) {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }
    Optional<TapField> optionalTapField = tapTable.getNameFieldMap().entrySet().stream().filter(t -> StringUtils.equals(fieldName, t.getKey())).map(t -> t.getValue())
            .peek(f -> f.setPrimaryKey(false)).findFirst();

    optionalTapField.ifPresent(f-> tapTable.getNameFieldMap().put(fieldName, f));
  }

  public static void addIndex(TapTable tapTable, String indexName, List<Map<String, Object>> descMap) {
    if (StringUtils.isEmpty(indexName) || CollectionUtils.isEmpty(descMap)) {
      throw new IllegalArgumentException("The index name and the description of the index are illegal");
    }
    if (tapTable.getIndexList().stream().anyMatch(i -> i.getName().equals(indexName))) {
      throw new IllegalArgumentException("index name already exists");
    }
    TapIndex tapIndex = new TapIndex().name(indexName);
    for (Map<String, Object> map : descMap) {
      String fieldName = (String) map.get("fieldName");
      if (StringUtils.isEmpty(fieldName)) {
        throw new IllegalArgumentException("The field name of the index cannot be empty");
      }
      boolean order = (boolean) map.get("order");
      tapIndex.indexField(new TapIndexField().name(fieldName).fieldAsc(order));
    }
    tapTable.add(tapIndex);
  }

  public static void removeIndex(TapTable tapTable, String indexName) {
    if (tapTable.getIndexList().stream().noneMatch(i -> i.getName().equals(indexName))) {
      logger.warn("index does not exist");
      return;
    }
    tapTable.getIndexList().removeIf(i -> i.getName().equals(indexName));
  }

  private static TapField getTapField(String fieldName, String tapType, String dataType) throws InstantiationException, IllegalAccessException {
    Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass(tapType);
    if (tapTypeClass == null) {
      throw new IllegalArgumentException("unknown tap type: " + tapType);
    }
    TapType tapTypeInstance;
    if (tapTypeClass == TapNumber.class) {
      tapTypeInstance = new TapNumber().maxValue(new BigDecimal(1.7976931348623157e+308)).minValue(new BigDecimal(Long.MIN_VALUE));
    } else {
      tapTypeInstance = tapTypeClass.newInstance();
    }
    return new TapField(fieldName, dataType).tapType(tapTypeInstance);
  }
}
