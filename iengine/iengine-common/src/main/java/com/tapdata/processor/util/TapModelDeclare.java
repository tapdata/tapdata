package com.tapdata.processor.util;

import com.tapdata.entity.schema.SchemaApplyResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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
    tapTable.getNameFieldMap().put(fieldName, tapField);
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

  public static void updateField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }

    tapTable.getNameFieldMap().put(fieldName, getTapField(fieldName, tapType, dataType));
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
    tapTable.getNameFieldMap().put(fieldName, getTapField(fieldName, tapType, null));
  }
  public static void upsertField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    tapTable.getNameFieldMap().put(fieldName, getTapField(fieldName, tapType, dataType));
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

  private static TapField getTapField(String fieldName, String tapType, String dataType) throws InstantiationException, IllegalAccessException {
    Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass(tapType);
    if (tapTypeClass == null) {
      throw new IllegalArgumentException("unknown tap type: " + tapType);
    }
    return new TapField(fieldName, dataType).tapType(tapTypeClass.newInstance());
  }
}
