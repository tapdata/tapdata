package com.tapdata.processor.util;

import com.tapdata.entity.schema.SchemaApplyResult;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TapModelDeclare {

	private final Log logger;


  public TapModelDeclare(Log logger) {
		this.logger = logger;
  }

  public void addField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    addField(tapTable, fieldName, tapType, null);
  }

  public void addField(List<SchemaApplyResult> schemaApplyResult, String fieldName, String tapType) throws Throwable {
    addField(schemaApplyResult, fieldName, tapType, null);
  }

  public void addField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    if (tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field {} already exists in table {}", fieldName, tapTable.getName());
      return;
    }
    TapField tapField = getTapField(fieldName, tapType, dataType);
    tapTable.add(tapField);
  }

  public void addField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CREATE, fieldName, getTapField(fieldName, tapType, dataType)));
  }

  public void updateField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    updateField(tapTable, fieldName, tapType, null);
  }

  public void updateField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }

    tapTable.add(getTapField(fieldName, tapType, dataType));
  }

  public void updateField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, null);
  }
  public void updateField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CONVERT, fieldName,
            getTapField(fieldName, tapType, dataType)));
  }

  public void upsertField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
    tapTable.add(getTapField(fieldName, tapType, null));
  }
  public void upsertField(TapTable tapTable, String fieldName, String tapType, String dataType) throws Throwable {
    tapTable.add(getTapField(fieldName, tapType, dataType));
  }

  public void upsertField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, null);
  }
  public void upsertField(List<SchemaApplyResult> schemaApplyResultList, String fieldName, String tapType, String dataType) throws Throwable {
    updateField(schemaApplyResultList, fieldName, tapType, dataType);
  }

  public void removeField(TapTable tapTable, String fieldName) {
    tapTable.getNameFieldMap().remove(fieldName);
  }

  public void removeField(List<SchemaApplyResult> schemaApplyResultList, String fieldName) {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_REMOVE, fieldName, null));
  }

  public void setPk(TapTable tapTable, String fieldName) {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
			logger.warn("Field not found when setting pk: " + fieldName + ", added by default");
			tapTable.add(new TapField().name(fieldName).tapType(TapSimplify.tapRaw()));
    }
    Optional<TapField> optionalTapField = tapTable.getNameFieldMap().entrySet().stream().filter(t -> StringUtils.equals(fieldName, t.getKey())).map(Map.Entry::getValue)
            .peek(f -> {
              f.setPrimaryKey(true);
              f.setPrimaryKeyPos(tapTable.getMaxPKPos() + 1);
            }).findFirst();

    optionalTapField.ifPresent(f-> tapTable.getNameFieldMap().put(fieldName, f));
  }

  public void setPk(List<SchemaApplyResult> schemaApplyResultList, String fieldName) {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_SET_PK, fieldName));
  }

  public void unSetPk(TapTable tapTable, String fieldName) {
    if (!tapTable.getNameFieldMap().containsKey(fieldName)) {
      logger.warn("field not found: " + fieldName);
      return;
    }
    Optional<TapField> optionalTapField = tapTable.getNameFieldMap().entrySet().stream().filter(t -> StringUtils.equals(fieldName, t.getKey())).map(Map.Entry::getValue)
            .peek(f -> {
              f.setPrimaryKey(false);
              f.setPrimaryKeyPos(null);
            }).findFirst();

    optionalTapField.ifPresent(f-> tapTable.getNameFieldMap().put(fieldName, f));
  }

  public void unSetPk(List<SchemaApplyResult> schemaApplyResultList, String fieldName) {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_UN_SET_PK, fieldName));
  }

  public void addIndex(TapTable tapTable, String indexName, List<Map<String, Object>> descMap) {
    addIndex(tapTable, indexName, false, false, null, descMap);
  }

  public void addIndex(TapTable tapTable, String indexName, Boolean unique, Boolean primary, Boolean cluster, List<Map<String, Object>> descMap) {
    if (StringUtils.isEmpty(indexName) || CollectionUtils.isEmpty(descMap)) {
      throw new IllegalArgumentException("The index name and the description of the index are illegal");
    }
    if (CollectionUtils.isNotEmpty(tapTable.getIndexList())
            && tapTable.getIndexList().stream().anyMatch(i -> i.getName().equals(indexName))) {
      throw new IllegalArgumentException("index name already exists");
    }
    Set<String> fieldNameSet = tapTable.getNameFieldMap().keySet();
    TapIndex tapIndex = getTapIndex(indexName, unique, primary, cluster, descMap, fieldNameSet);
    tapTable.add(tapIndex);
  }

  public void addIndex(List<SchemaApplyResult> schemaApplyResultList, String indexName, List<Map<String, Object>> descMap) {
    addIndex(schemaApplyResultList, indexName, false, false, null, descMap);
  }

  public void addIndex(List<SchemaApplyResult> schemaApplyResultList, String indexName, Boolean unique, Boolean primary, Boolean cluster, List<Map<String, Object>> descMap) {
    if (StringUtils.isEmpty(indexName) || CollectionUtils.isEmpty(descMap)) {
      throw new IllegalArgumentException("The index name and the description of the index are illegal");
    }
    TapIndex tapIndex = getTapIndex(indexName, unique, primary, cluster, descMap);
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_ADD_INDEX, tapIndex));
  }

  public void removeIndex(TapTable tapTable, String indexName) {
    if (CollectionUtils.isEmpty(tapTable.getIndexList())
            || tapTable.getIndexList().stream().noneMatch(i -> i.getName().equals(indexName))) {
      logger.warn("index does not exist");
      return;
    }
    tapTable.getIndexList().removeIf(i -> i.getName().equals(indexName));
  }

  public void removeIndex(List<SchemaApplyResult> schemaApplyResultList, String indexName) {
    schemaApplyResultList.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_REMOVE_INDEX, new TapIndex().name(indexName)));
  }

  private TapIndex getTapIndex(String indexName, Boolean unique, Boolean primary, Boolean cluster, List<Map<String, Object>> descMap) {
    return getTapIndex(indexName, unique, primary, cluster, descMap, null);
  }

  private TapIndex getTapIndex(String indexName, Boolean unique, Boolean primary, Boolean cluster, List<Map<String, Object>> descMap, Set<String> fieldNameSet) {
    TapIndex tapIndex = new TapIndex().name(indexName);
    if (unique != null) {
      tapIndex.unique(unique);
    }
    if (primary != null) {
      tapIndex.primary(primary);
    }
    if (cluster != null) {
      tapIndex.cluster(cluster);
    }
    for (Map<String, Object> map : descMap) {
      String fieldName = (String) map.get("fieldName");
      if (StringUtils.isEmpty(fieldName)) {
        throw new IllegalArgumentException("The field name of the index cannot be empty");
      }
      if (fieldNameSet != null && !fieldNameSet.contains(fieldName)) {
        throw new IllegalArgumentException("The field name not exist: " + fieldName);
      }
      String order = (String) map.get("order");
      tapIndex.indexField(new TapIndexField().name(fieldName).fieldAsc(StringUtils.equalsIgnoreCase(order, "asc")));
    }
    return tapIndex;
  }

  private TapField getTapField(String fieldName, String tapType, String dataType) {
    Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass(tapType);
    if (tapTypeClass == null) {
      throw new IllegalArgumentException("unknown tap type: " + tapType);
    }

    TapType tapTypeInstance = JavaTypesToTapTypes.toTapType(tapTypeClass);
    return new TapField(fieldName, dataType).tapType(tapTypeInstance);
  }
}
