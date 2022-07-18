package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by tapdata on 16/11/2017.
 */
public class RelateDatabaseField implements Serializable {

	public final static String NULLABLE = "YES";
	public final static String NOT_NULLABEL = "NO";

	private String field_name;
	private String table_name;
	private String data_type = "";
	private int primary_key_position;
	private String foreign_key_table;
	private String foreign_key_column;
	private String key;
	private int dataType;
	private boolean is_nullable;
	private boolean editable;

	private String parent;
	private String original_field_name;

	private String node_data_type;

	/**
	 * The tap type string for the database table field.
	 * <p>
	 * See more information about Tap Type at {@link TapType}
	 */
	private String tapType;


	/**
	 * Database numbner type precision
	 * 类型为：
	 * - 数值：代表长度
	 * - 日期：代表精度
	 */
	private Integer precision;

	/**
	 * Database number type scale
	 */
	private Integer scale;

	private Integer oriPrecision;

	private Integer oriScale;

	private String default_value;

	private JavaType javaType;

	private int columnSize;

	private String autoincrement;

	private int columnPosition;

	private String pkConstraintName;

	private boolean partitionField;

	private boolean distributeField;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String comment;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Map<String, Object> properties;

	public RelateDatabaseField() {
	}

	public RelateDatabaseField(String field_name, String table_name, String data_type, int primary_key_position, String key) {
		this.field_name = field_name;
		this.table_name = table_name;
		this.data_type = data_type;
		this.primary_key_position = primary_key_position;
		this.key = key;
	}

	public RelateDatabaseField(DatabaseSchemaTableColumns column) {
		this.field_name = column.getColumnName();
		this.table_name = column.getTableName();
		this.data_type = column.getDataType();
		this.precision = column.getPrecision();
		this.scale = column.getScale();
		this.columnSize = column.getDataLength();
		this.is_nullable = column.getNullable();
		this.columnPosition = column.getColumnPosition();
		this.oriPrecision = column.getOriPrecision();
		this.oriScale = column.getOriScale();
		this.comment = column.getComment();
	}

	public RelateDatabaseField(String field_name, String table_name, String data_type) {
		this.field_name = field_name;
		this.table_name = table_name;
		this.data_type = data_type;
	}

	public RelateDatabaseField(Integer precision, Integer scale) {
		this.precision = precision;
		this.scale = scale;
	}

	public RelateDatabaseField(String field_name, String table_name, String data_type, String parent, String original_field_name) {
		this.field_name = field_name;
		this.table_name = table_name;
		this.data_type = data_type;
		this.parent = parent;
		this.original_field_name = original_field_name;
	}

	public String getField_name() {
		return field_name;
	}

	public void setField_name(String field_name) {
		this.field_name = field_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public String getData_type() {
		return data_type;
	}

	public void setData_type(String data_type) {
		this.data_type = data_type;
	}

	public int getPrimary_key_position() {
		return primary_key_position;
	}

	public void setPrimary_key_position(int primary_key_position) {
		this.primary_key_position = primary_key_position;
	}

	public String getForeign_key_table() {
		return foreign_key_table;
	}

	public void setForeign_key_table(String foreign_key_table) {
		this.foreign_key_table = foreign_key_table;
	}

	public String getForeign_key_column() {
		return foreign_key_column;
	}

	public void setForeign_key_column(String foreign_key_column) {
		this.foreign_key_column = foreign_key_column;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public boolean getIs_nullable() {
		return is_nullable;
	}

	public void setIs_nullable(boolean is_nullable) {
		this.is_nullable = is_nullable;
	}

	public String getDefault_value() {
		return default_value;
	}

	public void setDefault_value(String default_value) {
		this.default_value = default_value;
	}

	public String getNode_data_type() {
		return node_data_type;
	}

	public void setNode_data_type(String node_data_type) {
		this.node_data_type = node_data_type;
	}

	public JavaType getJavaType() {
		return javaType;
	}

	public void setJavaType(JavaType javaType) {
		this.javaType = javaType;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public String getOriginal_field_name() {
		return original_field_name;
	}

	public void setOriginal_field_name(String original_field_name) {
		this.original_field_name = original_field_name;
	}

	public String getAutoincrement() {
		return autoincrement;
	}

	public void setAutoincrement(String autoincrement) {
		this.autoincrement = autoincrement;
	}

	public int getColumnPosition() {
		return columnPosition;
	}

	public void setColumnPosition(int columnPosition) {
		this.columnPosition = columnPosition;
	}

	public String getPkConstraintName() {
		return pkConstraintName;
	}

	public void setPkConstraintName(String pkConstraintName) {
		this.pkConstraintName = pkConstraintName;
	}

	public Integer getOriPrecision() {
		return oriPrecision;
	}

	public void setOriPrecision(Integer oriPrecision) {
		this.oriPrecision = oriPrecision;
	}

	public Integer getOriScale() {
		return oriScale;
	}

	public void setOriScale(Integer oriScale) {
		this.oriScale = oriScale;
	}

	public boolean getPartitionField() {
		return partitionField;
	}

	public void setPartitionField(boolean partitionField) {
		this.partitionField = partitionField;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getTapType() {
		return tapType;
	}

	public void setTapType(String tapType) {
		this.tapType = tapType.toUpperCase();
	}

	public boolean isDistributeField() {
		return distributeField;
	}

	public void setDistributeField(boolean distributeField) {
		this.distributeField = distributeField;
	}

	public boolean isEditable() {
		return editable;
	}

	public void setEditable(boolean editable) {
		this.editable = editable;
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	@Override
	public String toString() {
		return "RelateDatabaseField{" +
				"field_name='" + field_name + '\'' +
				", table_name='" + table_name + '\'' +
				", data_type='" + data_type + '\'' +
				", primary_key_position=" + primary_key_position +
				", foreign_key_table='" + foreign_key_table + '\'' +
				", foreign_key_column='" + foreign_key_column + '\'' +
				", key='" + key + '\'' +
				", dataType=" + dataType +
				", is_nullable='" + is_nullable + '\'' +
				", parent='" + parent + '\'' +
				", original_field_name='" + original_field_name + '\'' +
				", node_data_type='" + node_data_type + '\'' +
				", precision=" + precision +
				", scale=" + scale +
				", default_value='" + default_value + '\'' +
				", javaType=" + javaType +
				", columnSize=" + columnSize +
				", is_autoincrement='" + autoincrement + '\'' +
				'}';
	}
}
