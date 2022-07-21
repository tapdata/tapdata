package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author samuel
 * @Description The mapping relationship between database type and Tapdata type
 * @create 2021-08-03 15:39
 **/
public class TypeMapping implements Serializable {

	private static final long serialVersionUID = -6435780165899966281L;
	private String databaseType;
	/**
	 * Applicable database version，* for all versions
	 */
	private String version;
	// Database original type name
	private String dbType;
	/**
	 * Length, for string/number/date type
	 * Specify a range by min and max, null represents no scope limit
	 */
	private Long minPrecision;
	private Long maxPrecision;
	/**
	 * Decimal digits, for number type
	 * Specify a range by min and max, null represents no scope limit
	 */
	private Integer minScale;
	private Integer maxScale;
	// Tapdata abstract type
	private TapType tapType;
	/**
	 * Different dbType, same precision and scale range, which dbType is selected by default
	 */
	private boolean dbTypeDefault;
	/**
	 * In the same TapType, when the length accuracy cannot be elected, the default recommended type, each TapType can only have one
	 */
	private boolean tapTypeDefault;
	/**
	 * Mapping direction {@link com.tapdata.entity.TypeMappingDirection}
	 */
	private TypeMappingDirection direction;

	/**
	 * Compatible with old version type conversion
	 */
	private Integer code;

	/**
	 * The function name of the getter function.
	 *
	 * <p> By calling a getter function, you can get the desired type of data from Tap Value Container. </p>
	 * <p> See more information about Tap Value at {@link com.tapdata.entity.values.AbstractTapValue}. </p>
	 */
	private String getter;

	/**
	 * minValue and maxValue are used to indicate the value range of integer data types.
	 * String is used since some value may be overflow the database data type(mongo).
	 */
	private String minValue;
	private String maxValue;

	/**
	 * fixed is used to differentiate char and varchar, binary and varbinary, floating numbers and fixed-point numbers
	 */
	private Boolean fixed;

	public static final class TypeMappingBuilder {
		private String databaseType;
		private String version;
		private String dbType;
		private Long minPrecision;
		private Long maxPrecision;
		private Integer minScale;
		private Integer maxScale;
		private TapType tapType;
		private boolean dbTypeDefault;
		private boolean tapTypeDefault;
		private TypeMappingDirection direction = TypeMappingDirection.ALL;
		private Integer code;
		private String getter;
		private String minValue;
		private String maxValue;
		private Boolean fixed;

		private TypeMappingBuilder() {
		}

		public static TypeMappingBuilder builder(String databaseType, String dbType, TapType tapType) {
			return new TypeMappingBuilder().withDatabaseType(databaseType).withDbType(dbType).withTapType(tapType);
		}

		private TypeMappingBuilder withDatabaseType(String databaseType) {
			this.databaseType = databaseType;
			return this;
		}

		public TypeMappingBuilder withVersion(String version) {
			this.version = version;
			return this;
		}

		private TypeMappingBuilder withDbType(String dbType) {
			this.dbType = dbType;
			return this;
		}

		public TypeMappingBuilder withMinPrecision(Long minPrecision) {
			this.minPrecision = minPrecision;
			return this;
		}

		public TypeMappingBuilder withMaxPrecision(Long maxPrecision) {
			this.maxPrecision = maxPrecision;
			return this;
		}

		public TypeMappingBuilder withMinScale(Integer minScale) {
			this.minScale = minScale;
			return this;
		}

		public TypeMappingBuilder withMaxScale(Integer maxScale) {
			this.maxScale = maxScale;
			return this;
		}

		public TypeMappingBuilder withPrecision(Long precision) {
			this.minPrecision = precision;
			this.maxPrecision = precision;
			return this;
		}

		public TypeMappingBuilder withScale(Integer scale) {
			this.minScale = scale;
			this.maxScale = scale;
			return this;
		}

		private TypeMappingBuilder withTapType(TapType tapType) {
			this.tapType = tapType;
			return this;
		}

		public TypeMappingBuilder withRangePrecision(long min, long max) {
			if (min > max) {
				throw new IllegalArgumentException("Min precision cannot be greater then max precision");
			}
			this.minPrecision = min;
			this.maxPrecision = max;
			return this;
		}

		public TypeMappingBuilder withRangeScale(int min, int max) {
			if (min > max) {
				throw new IllegalArgumentException("Min scale cannot be greater then max scale");
			}
			this.minScale = min;
			this.maxScale = max;
			return this;
		}

		public TypeMappingBuilder withDbTypeDefault(boolean dbTypeDefault) {
			this.dbTypeDefault = dbTypeDefault;
			return this;
		}

		public TypeMappingBuilder withTapTypeDefault(boolean tapTypeDefault) {
			this.tapTypeDefault = tapTypeDefault;
			return this;
		}

		public TypeMappingBuilder withDirection(TypeMappingDirection direction) {
			this.direction = direction;
			return this;
		}

		public TypeMappingBuilder withCode(Integer code) {
			this.code = code;
			return this;
		}

		public TypeMappingBuilder withGetter(String getter) {
			this.getter = getter;
			return this;
		}

		public TypeMappingBuilder withRangeValue(String minValue, String maxValue) {
			this.minValue = minValue;
			this.maxValue = maxValue;
			return this;
		}

		public TypeMappingBuilder withFixed(Boolean fixed) {
			this.fixed = fixed;
			return this;
		}

		public TypeMapping build() {
			TypeMapping typeMapping = new TypeMapping();
			typeMapping.tapType = this.tapType;
			typeMapping.dbType = this.dbType;
			typeMapping.minScale = this.minScale;
			typeMapping.databaseType = this.databaseType;
			typeMapping.minPrecision = this.minPrecision;
			typeMapping.version = StringUtils.isNotBlank(this.version) ? this.version : "*";
			typeMapping.maxPrecision = this.maxPrecision;
			typeMapping.maxScale = this.maxScale;
			typeMapping.dbTypeDefault = this.dbTypeDefault;
			typeMapping.tapTypeDefault = this.tapTypeDefault;
			typeMapping.direction = this.direction;
			typeMapping.code = this.code;
			typeMapping.getter = this.getter;
			typeMapping.minValue = this.minValue;
			typeMapping.maxValue = this.maxValue;
			if (this.fixed != null) {
				typeMapping.fixed = this.fixed;
			}
			return typeMapping;
		}
	}

	public TypeMapping() {
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public String getVersion() {
		return version;
	}

	public String getDbType() {
		return dbType;
	}

	public Long getMinPrecision() {
		return minPrecision;
	}

	public Long getMaxPrecision() {
		return maxPrecision;
	}

	public Integer getMinScale() {
		return minScale;
	}

	public Integer getMaxScale() {
		return maxScale;
	}

	public TapType getTapType() {
		return tapType;
	}

	public boolean isDbTypeDefault() {
		return dbTypeDefault;
	}

	public boolean getDbTypeDefault() {
		return dbTypeDefault;
	}

	public TypeMappingDirection getDirection() {
		return direction;
	}

	public boolean createTableNeedPrecision() {
		if (minPrecision == null || maxPrecision == null) {
			return false;
		}
		return !minPrecision.equals(maxPrecision);
	}

	public boolean createTableNeedScale() {
		if (minScale == null || maxScale == null) {
			return false;
		}
		return !minScale.equals(maxScale);
	}

	public boolean getTapTypeDefault() {
		return tapTypeDefault;
	}

	public void setTapTypeDefault(boolean tapTypeDefault) {
		this.tapTypeDefault = tapTypeDefault;
	}

	public Integer getCode() {
		return code;
	}

	public String getGetter() {
		return getter;
	}

	public void setGetter(String getter) {
		this.getter = getter;
	}

	public String getMinValue() {
		return minValue;
	}

	public String getMaxValue() {
		return maxValue;
	}

	public Boolean getFixed() {
		return fixed;
	}

	@Override
	public String toString() {
		return "TypeMapping{" +
				"databaseType='" + databaseType + '\'' +
				", version='" + version + '\'' +
				", dbType='" + dbType + '\'' +
				", minPrecision=" + minPrecision +
				", maxPrecision=" + maxPrecision +
				", minScale=" + minScale +
				", maxScale=" + maxScale +
				", tapType=" + tapType +
				", dbTypeDefault=" + dbTypeDefault +
				", tapTypeDefault=" + tapTypeDefault +
				", direction=" + direction +
				", code=" + code +
				", getter=" + getter +
				", minValue=" + minValue +
				", maxValue=" + maxValue +
				", fixed=" + fixed +
				'}';
	}
}
