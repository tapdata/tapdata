package com.tapdata.entity;

import com.tapdata.entity.values.AbstractTapValue;

import java.io.Serializable;

/**
 * @author samuel
 * @Description Type mapping for developer
 * @create 2021-08-03 18:11
 **/
public class DbType implements Serializable {

	private static final long serialVersionUID = -9144512345269930397L;
	private String version;
	private String dbType;
	private Long minPrecision;
	private Long maxPrecision;
	private Integer minScale;
	private Integer maxScale;
	private boolean dbTypeDefault;
	private boolean tapTypeDefault;
	private TypeMappingDirection direction;
	private Integer code;
	private String getter;
	private String minValue;
	private String maxValue;
	private Boolean fixed;


	public static final class DbTypeBuilder {
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
		private TypeMappingDirection direction = TypeMappingDirection.ALL;

		/**
		 * Compatible with old version type conversion
		 */
		private Integer code;

		/**
		 * The function name of the getter function.
		 *
		 * <p> By calling a getter function, you can get the desired type of data from Tap Value Container. </p>
		 * <p> See more information about Tap Value at {@link com.tapdata.entity.values.AbstractTapValue}. </p>
		 * <p>
		 * TODO(zhangxin): TO find a more elegant way to register the function instead of using String
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

		private DbTypeBuilder() {
		}

		public static DbTypeBuilder builder(String dbType) {
			return new DbTypeBuilder().withDbType(dbType);
		}

		public DbTypeBuilder withVersion(String version) {
			this.version = version;
			return this;
		}

		private DbTypeBuilder withDbType(String dbType) {
			this.dbType = dbType;
			return this;
		}


		public DbTypeBuilder withPrecision(Long precision) {
			this.minPrecision = precision;
			this.maxPrecision = precision;
			return this;
		}

		public DbTypeBuilder withScale(Integer scale) {
			this.minScale = scale;
			this.maxScale = scale;
			return this;
		}

		public DbTypeBuilder withRangePrecision(long min, long max) {
			if (min > max) {
				throw new IllegalArgumentException("Min precision cannot be greater then max precision");
			}
			this.minPrecision = min;
			this.maxPrecision = max;
			return this;
		}

		public DbTypeBuilder withRangeScale(int min, int max) {
			if (min > max) {
				throw new IllegalArgumentException("Min scale cannot be greater then max scale");
			}
			this.minScale = min;
			this.maxScale = max;
			return this;
		}

		public DbTypeBuilder withDbTypeDefault(boolean dbTypeDefault) {
			this.dbTypeDefault = dbTypeDefault;
			return this;
		}

		public DbTypeBuilder withTapTypeDefault(boolean tapTypeDefault) {
			this.tapTypeDefault = tapTypeDefault;
			return this;
		}

		public DbTypeBuilder withDirection(TypeMappingDirection direction) {
			this.direction = direction;
			return this;
		}

		public DbTypeBuilder withCode(Integer code) {
			this.code = code;
			return this;
		}

		public DbTypeBuilder withGetter(String getter) {
			this.getter = getter;
			return this;
		}

		public DbTypeBuilder withRangeValue(String minValue, String maxValue) {
			this.minValue = minValue;
			this.maxValue = maxValue;
			return this;
		}

		public DbTypeBuilder withFixed(Boolean fixed) {
			this.fixed = fixed;
			return this;
		}

		public DbType build() {
			DbType type = new DbType();
			type.maxScale = this.maxScale;
			type.version = this.version;
			type.minPrecision = this.minPrecision;
			type.minScale = this.minScale;
			type.maxPrecision = this.maxPrecision;
			type.dbType = this.dbType;
			type.dbTypeDefault = this.dbTypeDefault;
			type.tapTypeDefault = this.tapTypeDefault;
			type.direction = this.direction;
			type.code = this.code;
			if (this.getter == null) {
				// `get` method is defined in the base abstract class of `AbstractValue`
				this.getter = AbstractTapValue.GET_ORIGIN;
			}
			type.getter = this.getter;
			type.minValue = this.minValue;
			type.maxValue = this.maxValue;
			if (this.fixed != null) {
				type.fixed = this.fixed;
			}
			return type;
		}
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

	public boolean isDbTypeDefault() {
		return dbTypeDefault;
	}

	public boolean getDbTypeDefault() {
		return dbTypeDefault;
	}

	public TypeMappingDirection getDirection() {
		return direction;
	}

	public boolean getTapTypeDefault() {
		return tapTypeDefault;
	}

	public Integer getCode() {
		return code;
	}

	public String getGetter() {
		return this.getter;
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
		return "DbType{" +
				"version='" + version + '\'' +
				", dbType='" + dbType + '\'' +
				", minPrecision=" + minPrecision +
				", maxPrecision=" + maxPrecision +
				", minScale=" + minScale +
				", maxScale=" + maxScale +
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
