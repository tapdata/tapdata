package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;

/**
 * oracle schema table columns info
 * Created by tapdata on 14/12/2017.
 */
public class DatabaseSchemaTableColumns {

	private final static BigDecimal FLOAT_PRECISION_OPERATOR = new BigDecimal(0.30103);

	private String tableName;

	private String columnName;

	private String dataType;

	private String defValue;

	private int dataLength;

	/**
	 * oracle numbner type precision
	 */
	private Integer precision;

	/**
	 * oracle number type scale
	 */
	private Integer scale;

	private boolean nullable;

	private int columnPosition;

	private Integer oriPrecision;

	private Integer oriScale;

	private String comment;

	public DatabaseSchemaTableColumns() {
	}

	public DatabaseSchemaTableColumns(ResultSet resultSet) throws Exception {

		this.tableName = resultSet.getString(1);
		this.columnName = resultSet.getString(2);
		this.dataType = resultSet.getString(3);
		switch (dataType) {
			case "CHAR":
			case "NCHAR":
			case "VARCHAR2":
			case "NVARCHAR2":
				this.dataLength = resultSet.getInt("charLength");
				this.precision = this.dataLength;
				break;
			case "RAW":
				this.dataLength = resultSet.getInt(4);
				this.precision = this.dataLength;
				break;
			default:
				this.dataLength = resultSet.getInt(4);
				if (dataType.startsWith("TIMESTAMP(")) {
					this.dataType = dataType.replaceAll("\\(.\\)", "");
					this.precision = resultSet.getObject(6) == null ? null : resultSet.getInt(6);
					this.oriPrecision = this.precision;
				}
				break;
		}
		handlePrecisionAndScale(resultSet);
		this.nullable = "Y".equals(resultSet.getString(7));
		this.columnPosition = resultSet.getInt(8);
		this.defValue = resultSet.getString(11);
		this.comment = resultSet.getString("columnComment");
	}

	/**
	 * 由于oracle有特殊的数字精度和范围的表达方式，这里需要特殊处理
	 * oracle number(p,s)
	 * p(precision): 有效数字总位数, 范围: [1,38]
	 * s(scale): 小数位数, 范围: [-84,127]
	 * 1. p>s的情况
	 * - s>=0: number(5,2), 代表一共5位，3位整数，2位小数
	 * - s<0: number(5,-2), 代表一共7位，7位整数(p+|s|)，0位小数，整数最后2位会被四舍五入
	 * 2. p<=s的情况
	 * - s>0: number(2,3)，代表一共3位，整数必须是0，3位小数，2位有效小数，小数的第1位必须是0，如0.011可以，0.111不行
	 * <p>
	 * null值处理
	 * 1. p==null && s==null 保持null值
	 * 2. p==null && s!=null p取最大值38
	 * 3. 不存在p!=null && s==null的情况，如果遇到会抛出异常
	 */
	private void handlePrecisionAndScale(ResultSet resultSet) throws Exception {
		if (!StringUtils.equalsAny(dataType, "NUMBER", "FLOAT")) {
			return;
		}

		this.oriPrecision = resultSet.getObject(5) == null ? null : resultSet.getInt(5);
		this.oriScale = resultSet.getObject(6) == null ? null : resultSet.getInt(6);

		if (dataType.equals("FLOAT")) {
			handleFloat();
		} else if (dataType.equals("NUMBER")) {
			handleNumber();
		}
	}

	private void handleNumber() {
		if (oriPrecision == null && oriScale == null) {
			precision = null;
			scale = null;
			return;
		} else if (oriPrecision == null && oriScale != null) {
			// If precision is null, oracle uses the maximum value: 38
			oriPrecision = 38;
		} else if (oriPrecision != null && oriScale == null) {
			throw new RuntimeException("Load oracle table field failed, invalid precision and scale on type number, precision is not null and scale is null, table name: "
					+ tableName + ", field name: " + columnName + ", type: " + dataType + ", precision: " + oriPrecision + ", scale: " + oriScale);
		}

		// 根据oriPrecision, oriScale的关系，设置precision, scale
		if (oriPrecision > oriScale) {
			// p>s
			if (oriScale >= 0) {
				// s>=0
				precision = oriPrecision;
				scale = oriScale;
			} else {
				// s<0
				precision = oriPrecision + Math.abs(oriScale);
				scale = 0;
			}
		} else {
			// p<=s
			if (oriScale > 0) {
				precision = oriScale;
				scale = oriScale;
			}
		}
	}

	private void handleFloat() {
		if (oriPrecision == null) {
			oriPrecision = 126;
		}

		this.precision = new BigDecimal(oriPrecision).multiply(FLOAT_PRECISION_OPERATOR).intValue();
		this.precision = precision < 1 ? 1 : precision;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getDefValue() {
		return defValue;
	}

	public void setDefValue(String defValue) {
		this.defValue = defValue;
	}

	public int getDataLength() {
		return dataLength;
	}

	public void setDataLength(int dataLength) {
		this.dataLength = dataLength;
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

	public boolean getNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public int getColumnPosition() {
		return columnPosition;
	}

	public void setColumnPosition(int columnPosition) {
		this.columnPosition = columnPosition;
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

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "DatabaseSchemaTableColumns{" +
				"tableName='" + tableName + '\'' +
				", columnName='" + columnName + '\'' +
				", dataType='" + dataType + '\'' +
				", dataLength=" + dataLength +
				", precision=" + precision +
				", scale=" + scale +
				", nullable=" + nullable +
				", columnPosition=" + columnPosition +
				", oriPrecision=" + oriPrecision +
				", oriScale=" + oriScale +
				", comment='" + comment + '\'' +
				'}';
	}
}
