package io.tapdata.entity.schema;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.type.TapType;

import java.util.*;

public class TapTable extends TapItem<TapField> {
	private static final String TAG = TapTable.class.getSimpleName();

	public TapTable() {
	}

	public TapTable(String nameAndId) {
		this(nameAndId, nameAndId);
	}

	public TapTable(String id, String name) {
		this.name = name;
		this.id = id;
	}

	private Long lastUpdate;

	public TapTable lastUpdate(Long lastUpdate) {
		this.lastUpdate = lastUpdate;
		return this;
	}

	/**
	 * Please don't add or remove on this field. If you need to add or delete, please set new one.
	 * Protect ConcurrentModification.
	 */
	private LinkedHashMap<String, TapField> nameFieldMap;

	/**
	 * For the database which don't need to create table before insert records.
	 * <p>
	 * Given the default primary keys, if user don't give the specific primary key, will use defaultPrimaryKeys as primary keys.
	 */
	private List<String> defaultPrimaryKeys;

	public TapTable defaultPrimaryKeys(String defaultPrimaryKey) {
		this.defaultPrimaryKeys = Collections.singletonList(defaultPrimaryKey);
		return this;
	}

	public TapTable defaultPrimaryKeys(List<String> defaultPrimaryKeys) {
		this.defaultPrimaryKeys = defaultPrimaryKeys;
		return this;
	}

	private List<TapIndex> indexList;

	private String id;

	private String name;
	/**
	 * 存储引擎， innoDB
	 */
	private String storageEngine;
	/**
	 * 字符编码
	 */
	private String charset;

	private String comment;

	protected String pdkId;

	public TapTable pdkId(String pdkId) {
		this.pdkId = pdkId;
		return this;
	}

	protected String pdkGroup;

	public TapTable pdkGroup(String pdkGroup) {
		this.pdkGroup = pdkGroup;
		return this;
	}

	protected String pdkVersion;

	public TapTable pdkVersion(String pdkVersion) {
		this.pdkVersion = pdkVersion;
		return this;
	}

	private Collection<String> logicPrimaries;

	private TapIndexEx partitionIndex;

	public String toString() {
		return "TapTable id " + id +
				" name " + name +
				" storageEngine " + storageEngine +
				" charset " + charset +
				" number of fields " + (nameFieldMap != null ? nameFieldMap.size() : 0);
	}

	public TapTable add(TapIndex index) {
		indexCheck(index);
		if (indexList == null) {
			indexList = new ArrayList<>();
		}
		indexList.add(index);
		return this;
	}

	public TapTable add(TapField field) {
		fieldCheck(field);
		if (nameFieldMap == null) {
			nameFieldMap = new LinkedHashMap<>();
		}
		nameFieldMap.put(field.getName(), field);
		if (field.getPos() == null) {
			field.pos(nameFieldMap.size());
		}
		return this;
	}

	private void indexCheck(TapIndex index) {
		if (index == null)
			throw new IllegalArgumentException("index is null when add into table " + name);
		if (index.getIndexFields() == null || index.getIndexFields().isEmpty())
			throw new IllegalArgumentException("index fields is null or empty when add into table " + name);
	}

	private void fieldCheck(TapField field) {
		if (field == null)
			throw new IllegalArgumentException("field is null when add into table " + name);
		if (field.getName() == null)
			throw new IllegalArgumentException("field name is null when add into table " + name);
	}

	public Collection<String> primaryKeys() {
		return primaryKeys(false);
	}

	public Collection<String> primaryKeys(boolean isLogic) {
		if (isLogic) {
			if (null != logicPrimaries && logicPrimaries.size() > 0) return logicPrimaries;
		}
		LinkedHashMap<String, TapField> nameFieldMapCopyRef = this.nameFieldMap;
		if (nameFieldMapCopyRef == null)
			return Collections.emptyList();

		Map<Integer, String> posPrimaryKeyName = new TreeMap<>();
		for (String key : nameFieldMapCopyRef.keySet()) {
			TapField field = nameFieldMapCopyRef.get(key);
			if (field != null && ((field.getPrimaryKey() != null && field.getPrimaryKey())
					|| (field.getPrimaryKeyPos() != null && field.getPrimaryKeyPos() > 0))) {
				posPrimaryKeyName.put(field.getPrimaryKeyPos(), field.getName());
			}
		}

		if (!posPrimaryKeyName.isEmpty())
			return posPrimaryKeyName.values();

		if (isLogic) {
			if (indexList != null) {
				List<String> primaryKeys = new ArrayList<>();
				for (TapIndex tapIndex : indexList) {
					if (((tapIndex.getUnique() != null && tapIndex.getUnique()) || (tapIndex.getPrimary() != null && tapIndex.getPrimary())) && tapIndex.getIndexFields() != null && !tapIndex.getIndexFields().isEmpty()) {
						for (TapIndexField indexField : tapIndex.getIndexFields()) {
							primaryKeys.add(indexField.getName());
						}
						break;
					}
				}
				if (!primaryKeys.isEmpty())
					return primaryKeys;
			}
		}
		return Collections.emptyList();
	}

	public TapIndexEx getPartitionIndex() {
		return partitionIndex();
	}

	public void setPartitionIndex(TapIndexEx partitionIndex) {
		this.partitionIndex = partitionIndex;
	}

	public TapIndexEx partitionIndex() {
		if(partitionIndex != null) {
			return partitionIndex;
		}
		LinkedHashMap<String, TapField> nameFieldMapCopyRef = this.nameFieldMap;
		if (nameFieldMapCopyRef == null || nameFieldMapCopyRef.isEmpty()) {
			TapLogger.warn(TAG, "Table {} field map is empty, no partition index available. ", name);
			return null;
		}

		TapIndex bestIndex = null;

//		if (indexList != null) {
//			for (TapIndex tapIndex : indexList) {
//				if(tapIndex.getIndexFields() == null)
//					continue;
//				if((bestIndex == null || bestIndex.getIndexFields().size() > tapIndex.getIndexFields().size()))
//					bestIndex = tapIndex;
//			}
//		}
		//Use primary key only, as index field may have null value, which can not be find by AdvanceFilter.
		TapIndex primaryIndex = new TapIndex().unique(true);
		for (String key : nameFieldMapCopyRef.keySet()) {
			TapField field = nameFieldMapCopyRef.get(key);
			if (field != null && ((field.getPrimaryKey() != null && field.getPrimaryKey())
					|| (field.getPrimaryKeyPos() != null && field.getPrimaryKeyPos() > 0))) {
				primaryIndex.indexField(new TapIndexField().name(field.getName()).fieldAsc(true));
			}
		}

		if(primaryIndex.getIndexFields() != null && (bestIndex == null || bestIndex.getIndexFields().size() >= primaryIndex.getIndexFields().size())) {
			bestIndex = primaryIndex;
		}

		return bestIndex != null ? new TapIndexEx(bestIndex) : null;
	}

	/**
	 * @deprecated difficult to check by dataType.
	 *
	 * @param indexFields
	 * @param supportedSplitTypes
	 * @return
	 */
	private boolean checkFieldSupportSplit(List<TapIndexField> indexFields, Set<String> supportedSplitTypes) {
		for(TapIndexField field : indexFields) {
			TapField tapField = nameFieldMap.get(field.getName());
			if(tapField == null || tapField.getTapType() == null) {
				TapLogger.warn(TAG, "field {} is null or tapType is null, can not be partition index. ", field.getName());
				return false;
			}
			TapType tapType = tapField.getTapType();

		}
		return true;
	}

	@Override
	public Collection<TapField> childItems() {
		if (nameFieldMap != null)
			return nameFieldMap.values();
		return null;
	}

	public int getMaxPos() {
		if (null == nameFieldMap || nameFieldMap.size() <= 0) {
			return 0;
		}
		int max = 0;
		for (TapField tapField : nameFieldMap.values()) {
			if (null != tapField && tapField.getPos() != null && max < tapField.getPos()) {
				max = tapField.getPos();
			}
		}
		return max;
	}

	public int getMaxPKPos() {
		if (null == nameFieldMap || nameFieldMap.size() <= 0) {
			return 0;
		}
		int max = 0;
		for (TapField tapField : nameFieldMap.values()) {
			if (null != tapField.getPrimaryKeyPos() && max < tapField.getPrimaryKeyPos()) {
				max = tapField.getPrimaryKeyPos();
			}
		}
		return max;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void putField(String name, TapField field) {
		field.setName(name);
		add(field);
	}

	public LinkedHashMap<String, TapField> getNameFieldMap() {
		return nameFieldMap;
	}

	public void setNameFieldMap(LinkedHashMap<String, TapField> nameFieldMap) {
		this.nameFieldMap = nameFieldMap;
	}

	public String getStorageEngine() {
		return storageEngine;
	}

	public void setStorageEngine(String storageEngine) {
		this.storageEngine = storageEngine;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<TapIndex> getIndexList() {
		return indexList;
	}

	public void setIndexList(List<TapIndex> indexList) {
		this.indexList = indexList;
	}

	public List<String> getDefaultPrimaryKeys() {
		return defaultPrimaryKeys;
	}

	public void setDefaultPrimaryKeys(List<String> defaultPrimaryKeys) {
		this.defaultPrimaryKeys = defaultPrimaryKeys;
	}

	public String getPdkId() {
		return pdkId;
	}

	public void setPdkId(String pdkId) {
		this.pdkId = pdkId;
	}

	public String getPdkGroup() {
		return pdkGroup;
	}

	public void setPdkGroup(String pdkGroup) {
		this.pdkGroup = pdkGroup;
	}

	public String getPdkVersion() {
		return pdkVersion;
	}

	public void setPdkVersion(String pdkVersion) {
		this.pdkVersion = pdkVersion;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Long getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(Long lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	public void setLogicPrimaries(Collection<String> logicPrimaries) {
		this.logicPrimaries = logicPrimaries;
	}
}
