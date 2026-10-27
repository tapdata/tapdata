package io.tapdata.flow.engine.V2.util;

import com.tapdata.constant.DateUtil;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.pdk.core.api.impl.serialize.ObjectSerializableImplV2;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * @author jackin
 * @Description
 * @date 2023/3/10 17:42
 **/
public class TapCodecUtil {

	protected static final byte TAP_YEAR_BYTE_TAG_V2 = 1;
	protected static final byte TAP_TIME_BYTE_TAG_V2 = 2;
	protected static final byte TAP_DATE_BYTE_TAG_V2 = 3;
	protected static final byte TAP_DATETIME_BYTE_TAG_V2 = 4;
	protected static final String TAP_YEAR_BYTE_TAG = "$tapYear$";
	protected static final String TAP_TIME_BYTE_TAG = "$tapTime$";
	protected static final String TAP_DATE_BYTE_TAG = "$tapDate$";
	public static final ObjectSerializable OBJECT_SERIALIZABLE = new ObjectSerializableImplV2();
	public static final String TAG = TapCodecUtil.class.getSimpleName();
	protected static final byte[] TAG_BYTES = TAG.getBytes(StandardCharsets.UTF_8);
	private static final TapCodecsRegistry TARGET_VALUE_CODECS = registerTargetAwareDateTimeCodecs(TapCodecsRegistry.create());

	public static TapCodecsFilterManager createEngineCodecsFilterManger() {
		TapCodecsFilterManager tapCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
		registerFromTapValue(tapCodecsFilterManager.getCodecsRegistry());
		registerToTapValue(tapCodecsFilterManager.getCodecsRegistry());
		return tapCodecsFilterManager;
	}

	public static void registerFromTapValue(TapCodecsRegistry tapCodecsRegistry) {
		tapCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, TapCodecUtil::fromTapDateTimeValue);
		tapCodecsRegistry.registerFromTapValue(TapYearValue.class, tapValue -> addTag(OBJECT_SERIALIZABLE.fromObject(tapValue.getValue()), TAP_YEAR_BYTE_TAG_V2));
		tapCodecsRegistry.registerFromTapValue(TapDateValue.class, TapCodecUtil::fromTapDateValue);
		tapCodecsRegistry.registerFromTapValue(TapTimeValue.class, tapValue -> addTag(OBJECT_SERIALIZABLE.fromObject(tapValue.getValue()), TAP_TIME_BYTE_TAG_V2));
		registerTargetAwareDateTimeCodecs(tapCodecsRegistry);
	}

	private static TapCodecsRegistry registerTargetAwareDateTimeCodecs(TapCodecsRegistry registry) {
		return registry.registerFromTapValueV2(TapDateTimeValue.class, TapCodecUtil::fromTapDateTimeValue)
				.registerFromTapValueV2(TapDateValue.class, TapCodecUtil::fromTapDateValue);
	}

	/** Convert fields only when the discovered target type differs from the logical model. */
	public static void convertForPhysicalTarget(Map<String, Object> values, Map<String, TapField> logicalFields,
			Map<String, TapField> physicalFields, TapCodecsRegistry connectorCodecs) {
		if (values == null || values.isEmpty() || physicalFields == null || physicalFields.isEmpty()) return;
		values.forEach((fieldName, value) -> {
			// CDC fields may already be TapValue before this node; the set of fields
			// converted during this pass cannot be used to filter them.
			TapField targetField = physicalFields.get(fieldName);
			if (targetField == null) return;
			TapField logicalField = logicalFields == null ? null : logicalFields.get(fieldName);
			if (logicalField != null && sameTargetType(logicalField, targetField)) return;
			if (value instanceof TapDateValue) {
				if (connectorCodecs != null && connectorCodecs.getCustomFromTapValueCodecV2(TapDateValue.class) != null) return;
				TapDateValue dateValue = (TapDateValue) value;
				if (dateValue.getValue() != null && !dateValue.getValue().isContainsIllegal()
						&& isDateType(targetField.getDataType())) {
					values.put(fieldName, TARGET_VALUE_CODECS.getCustomFromTapValueCodecV2(TapDateValue.class)
							.fromTapValue(dateValue, targetField));
				}
			} else if (value instanceof TapDateTimeValue) {
				if (connectorCodecs != null && connectorCodecs.getCustomFromTapValueCodecV2(TapDateTimeValue.class) != null) return;
				TapDateTimeValue dateTimeValue = (TapDateTimeValue) value;
				Integer scale = targetField.getScale();
				if (dateTimeValue.getValue() != null && !dateTimeValue.getValue().isContainsIllegal()
						&& isDateTimeType(targetField.getDataType()) && scale != null && scale >= 0 && scale <= 9) {
					FromTapValueCodec<TapDateTimeValue> connectorCodec = connectorCodecs == null ? null
							: connectorCodecs.getCustomFromTapValueCodec(TapDateTimeValue.class);
					Object connectorValue = connectorCodec == null ? null : connectorCodec.fromTapValue(dateTimeValue);
					values.put(fieldName, fromTapDateTimeValue(dateTimeValue, targetField, connectorValue));
				}
			}
		});
	}

	private static boolean sameTargetType(TapField logicalField, TapField physicalField) {
		String logicalType = logicalField.getDataType() == null ? "" : logicalField.getDataType().trim().toLowerCase(Locale.ROOT);
		String physicalType = physicalField.getDataType() == null ? "" : physicalField.getDataType().trim().toLowerCase(Locale.ROOT);
		if (!logicalType.equals(physicalType)) return false;
		return !isDateTimeType(physicalType) || Objects.equals(logicalField.getScale(), physicalField.getScale());
	}

	private static Object fromTapDateTimeValue(TapDateTimeValue tapValue) {
		if (tapValue == null || tapValue.getValue() == null) return null;
		if (tapValue.getValue().isContainsIllegal()) {
			tapValue.getValue().setTimeZone(TimeZone.getDefault());
			return addTag(OBJECT_SERIALIZABLE.fromObject(tapValue.getValue()), TAP_DATETIME_BYTE_TAG_V2);
		}
		return tapValue.getValue().toInstant();
	}

	private static Object fromTapDateTimeValue(TapDateTimeValue tapValue, TapField targetField) {
		return fromTapDateTimeValue(tapValue, targetField, null);
	}

	private static Object fromTapDateTimeValue(TapDateTimeValue tapValue, TapField targetField, Object connectorValue) {
		if (tapValue == null || tapValue.getValue() == null) return null;
		if (targetField == null || !isDateTimeType(targetField.getDataType())
				|| tapValue.getValue().isContainsIllegal()) {
			return fromTapDateTimeValue(tapValue);
		}
		Integer scale = targetField.getScale();
		if (scale == null || scale < 0 || scale > 9) {
			return fromTapDateTimeValue(tapValue);
		}
		// Keep the wall-clock value. SQL Server's CDC SQL formatter writes
		// LocalDateTime with only the significant fractional digits; formatting
		// a Timestamp there produces six digits even for millisecond values.
		LocalDateTime dateTime;
		if (connectorValue instanceof LocalDateTime) {
			dateTime = (LocalDateTime) connectorValue;
		} else if (connectorValue instanceof Timestamp) {
			dateTime = ((Timestamp) connectorValue).toLocalDateTime();
		} else {
			dateTime = tapValue.getValue().toLocalDateTime();
		}
		int quantum = (int) Math.pow(10, 9 - scale);
		return dateTime.withNano((dateTime.getNano() / quantum) * quantum);
	}

	private static Object fromTapDateValue(TapDateValue tapValue) {
		if (tapValue == null || tapValue.getValue() == null) return null;
		return addTag(OBJECT_SERIALIZABLE.fromObject(tapValue.getValue()), TAP_DATE_BYTE_TAG_V2);
	}

	private static Object fromTapDateValue(TapDateValue tapValue, TapField targetField) {
		if (tapValue == null || tapValue.getValue() == null) return null;
		if (targetField != null && isDateType(targetField.getDataType())
				&& !tapValue.getValue().isContainsIllegal()) {
			return tapValue.getValue().toSqlDate();
		}
		return fromTapDateValue(tapValue);
	}

	private static boolean isDateType(String dataType) {
		return baseType(dataType).equals("date");
	}

	private static boolean isDateTimeType(String dataType) {
		String type = baseType(dataType);
		return type.equals("datetime") || type.equals("datetime2");
	}

	private static String baseType(String dataType) {
		if (dataType == null) return "";
		String normalized = dataType.trim().toLowerCase(Locale.ROOT);
		int parameterStart = normalized.indexOf('(');
		return parameterStart < 0 ? normalized : normalized.substring(0, parameterStart).trim();
	}

	public static void registerToTapValue(TapCodecsRegistry tapCodecsRegistry) {
		tapCodecsRegistry.registerToTapValue(byte[].class, (o, tapType) -> {
			byte[] bytes = (byte[]) o;
			Object result = toTapValueV2(bytes);
			if (result instanceof TapValue) {
				return (TapValue<?, ?>) result;
			}
			result = toTapValueV1(bytes);
			if (result instanceof TapValue) {
				return (TapValue<?, ?>) result;
			}

			return new TapBinaryValue((byte[]) o);
		});
	}

	/**
	 * Do not use this method
	 * Retaining this method is for unit testing purposes
	 *
	 * @param tapValue
	 * @return
	 */
	protected static Object fromTapValueV1(TapValue<?, ?> tapValue) {
		if (tapValue instanceof TapDateTimeValue) {
			return ((TapDateTimeValue) tapValue).getValue().toInstant();
		} else if (tapValue instanceof TapYearValue) {
			final String yyyy = ((TapYearValue) tapValue).getValue().toFormatString("yyyy");
			StringBuilder sb = new StringBuilder(TAP_YEAR_BYTE_TAG);
			sb.append(yyyy);
			return sb.toString().getBytes();
		} else if (tapValue instanceof TapDateValue) {
			((TapDateValue) tapValue).getValue().setTimeZone(TimeZone.getDefault());
			final String dateString = ((TapDateValue) tapValue).getValue().toFormatString("yyyy-MM-dd");
			StringBuilder sb = new StringBuilder(TAP_DATE_BYTE_TAG);
			sb.append(dateString);
			return sb.toString().getBytes();
		} else if (tapValue instanceof TapTimeValue) {
			final String timeString = ((TapTimeValue) tapValue).getValue().toTimeStr();
			StringBuilder sb = new StringBuilder(TAP_TIME_BYTE_TAG);
			sb.append(timeString);
			return sb.toString().getBytes();
		}
		return tapValue;
	}

	private static Object toTapValueV1(byte[] bytes) {
		if (null == bytes) {
			return bytes;
		}
		// TapYear judgment
		if (bytes.length > TAP_YEAR_BYTE_TAG.length()) {
			byte[] yearTagBytes = new byte[TAP_YEAR_BYTE_TAG.length()];
			System.arraycopy(bytes, 0, yearTagBytes, 0, TAP_YEAR_BYTE_TAG.length());
			if (Arrays.equals(yearTagBytes, TAP_YEAR_BYTE_TAG.getBytes())) {

				byte[] yearStringBytes = new byte[bytes.length - TAP_YEAR_BYTE_TAG.length()];
				System.arraycopy(bytes, TAP_YEAR_BYTE_TAG.length(), yearStringBytes, 0, yearStringBytes.length);
				String yearString = new String(yearStringBytes);
				try {
					return new TapYearValue(new DateTime(DateUtil.parse(yearString, "yyyy", null)));
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}
		}

		// TapDate judgment
		if (bytes.length > TAP_DATE_BYTE_TAG.length()) {
			byte[] dateTagBytes = new byte[TAP_DATE_BYTE_TAG.length()];
			System.arraycopy(bytes, 0, dateTagBytes, 0, TAP_DATE_BYTE_TAG.length());
			if (Arrays.equals(dateTagBytes, TAP_DATE_BYTE_TAG.getBytes())) {

				byte[] dateStringBytes = new byte[bytes.length - TAP_DATE_BYTE_TAG.length()];
				System.arraycopy(bytes, TAP_DATE_BYTE_TAG.length(), dateStringBytes, 0, dateStringBytes.length);
				String dateString = new String(dateStringBytes);
				try {
					return new TapDateValue(new DateTime(DateUtil.parse(dateString, "yyyy-MM-dd", null)));
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}
		}

		// TapTime judgment
		if (bytes.length > TAP_TIME_BYTE_TAG.length()) {
			byte[] timeTagBytes = new byte[TAP_TIME_BYTE_TAG.length()];
			System.arraycopy(bytes, 0, timeTagBytes, 0, TAP_TIME_BYTE_TAG.length());
			if (Arrays.equals(timeTagBytes, TAP_TIME_BYTE_TAG.getBytes())) {

				byte[] timeStringBytes = new byte[bytes.length - TAP_TIME_BYTE_TAG.length()];
				System.arraycopy(bytes, TAP_TIME_BYTE_TAG.length(), timeStringBytes, 0, timeStringBytes.length);
				String timeString = new String(timeStringBytes);
				return new TapTimeValue(DateTime.withTimeStr(timeString));
			}
		}
		return bytes;
	}

	private static Object toTapValueV2(byte[] bytes) {
		int tagLength = TAG_BYTES.length;
		if (null == bytes || bytes.length <= tagLength) {
			return bytes;
		}
		byte[] tagBytes = new byte[tagLength];
		System.arraycopy(bytes, 0, tagBytes, 0, tagLength);
		if (!Arrays.equals(TAG_BYTES, tagBytes)) {
			return bytes;
		}
		byte valueTag = bytes[tagLength];
		byte[] valueBytes = new byte[bytes.length - tagLength - 1];
		System.arraycopy(bytes, tagLength + 1, valueBytes, 0, valueBytes.length);
		Object valueObj = OBJECT_SERIALIZABLE.toObject(valueBytes);
		if (!(valueObj instanceof DateTime)) {
			return bytes;
		}
		DateTime dateTime = (DateTime) valueObj;
		TapValue<?, ?> tapValue;
		switch (valueTag) {
			case TAP_DATE_BYTE_TAG_V2:
				tapValue = new TapDateValue(dateTime);
				break;
			case TAP_DATETIME_BYTE_TAG_V2:
				tapValue = new TapDateTimeValue(dateTime);
				break;
			case TAP_YEAR_BYTE_TAG_V2:
				tapValue = new TapYearValue(dateTime);
				break;
			case TAP_TIME_BYTE_TAG_V2:
				tapValue = new TapTimeValue(dateTime);
				break;
			default:
				return bytes;
		}
		return tapValue;
	}

	protected static byte[] addTag(byte[] value, byte valueTag) {
		if (null == value || value.length == 0) {
			return value;
		}
		byte[] tagBytes = new byte[TAG_BYTES.length + 1];
		System.arraycopy(TAG_BYTES, 0, tagBytes, 0, TAG_BYTES.length);
		tagBytes[TAG_BYTES.length] = valueTag;
		int tagLength = tagBytes.length;
		byte[] result = new byte[tagLength + value.length];
		System.arraycopy(tagBytes, 0, result, 0, tagLength);
		System.arraycopy(value, 0, result, tagLength, value.length);
		return result;
	}

}
