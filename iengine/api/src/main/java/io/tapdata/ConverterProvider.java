package io.tapdata;

import com.tapdata.constant.ConnectorContext;
import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.values.*;
import io.tapdata.common.SettingService;
import io.tapdata.entity.ConvertLog;
import io.tapdata.exception.ConvertException;
//import jdk.nashorn.internal.runtime.Undefined;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public interface ConverterProvider {

	void init(ConverterContext context);

	/**
	 * convert field type to java type
	 *
	 * @param relateDatabaseField the column definition in tapdata
	 * @return the column definition in tapdata, after convert data type
	 * @throws ConvertException
	 */
	RelateDatabaseField schemaConverter(RelateDatabaseField relateDatabaseField) throws ConvertException;

	/**
	 * convert java type to date type
	 *
	 * @param relateDatabaseField
	 * @return
	 * @throws ConvertException
	 */
	default void javaTypeConverter(RelateDatabaseField relateDatabaseField) throws ConvertException {

	}

	/**
	 * convert field type to java type
	 *
	 * @param relateDatabaseField tapdata schema list
	 * @return kafka schema builder
	 * @throws ConvertException
	 */
	SchemaBuilder kafkaSchemaBuilder(RelateDatabaseField relateDatabaseField) throws ConvertException;

	/**
	 * returns a Object, convert the data from database type to a java type
	 *
	 * @param relateDatabaseField
	 * @param data
	 * @return
	 * @throws ConvertException
	 */
	Object sourceValueConverter(RelateDatabaseField relateDatabaseField, Object data) throws ConvertException;

	/**
	 * returns a Object, convert the data from java type to target database type
	 *
	 * @param data
	 * @return
	 * @throws ConvertException
	 */
	Object targetValueConverter(Object data) throws ConvertException;

	default Object commTargetValueConverter(Object data) throws ConvertException {

		try {
			/*if (data instanceof Undefined) {
				data = null;
			}*/
		} catch (Exception e) {
			throw new ConvertException(ConvertLog.ERR_COMMON_TARGET_0001.getErrCode(),
					String.format(ConvertLog.ERR_COMMON_TARGET_0001.getMessage(), data, "Undefined", "null", e.getMessage()));
		}

		return data;
	}

	default Map<String, Object> targetFieldNameConverter(Map<String, Object> dataMap) throws ConvertException {
		return MapUtils.isNotEmpty(dataMap) ? new HashMap<String, Object>() {{
			putAll(dataMap);
		}} : null;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Boolean}. The default container
	 * for {@code Boolean} values is {@link TapBoolean}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Boolean}, you should
	 * overwrite this function and return the extended class. You can find more information on how
	 * to extend the container  at {@link TapBoolean}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Boolean}.
	 */
	default Class<? extends TapBoolean> getTapBooleanClass() {
		return TapBoolean.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Bytes}. The default container for
	 * {@code Bytes} values is {@link TapBytes}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Bytes}, you should
	 * overwrite this function and return the extended class. You can find more information on
	 * how to extend the container at {@link TapBytes}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Bytes}.
	 */
	default Class<? extends TapBytes> getTapBytesClass() {
		return TapBytes.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Date}. The default container for
	 * {@code Date} values is {@link TapDate}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Date}, you should
	 * overwrite this function and return the extended class. You can find more information on
	 * how to extend the container at {@link TapDate}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Date}.
	 */
	default Class<? extends TapDate> getTapDateClass() {
		return TapDate.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Datetime}. The default container
	 * for {@code Datetime} values is {@link TapDatetime}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Datetime}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapDatetime}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Datetime}.
	 */
	default Class<? extends TapDatetime> getTapDatetimeClass() {
		return TapDatetime.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Number}. The default container
	 * for {@code Number} values is {@link TapNumber}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Number}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapNumber}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Number}.
	 */
	default Class<? extends TapNumber> getTapNumberClass() {
		return TapNumber.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code String}. The default container
	 * for {@code String} values is {@link TapString}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code String}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapString}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code String}.
	 */
	default Class<? extends TapString> getTapStringClass() {
		return TapString.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Time}. The default container
	 * for {@code Time} values is {@link TapTime}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Time}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapTime}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Time}.
	 */
	default Class<? extends TapTime> getTapTimeClass() {
		return TapTime.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Array}. The default container
	 * for {@code Array} values is {@link TapArray}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Array}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapArray}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Array}.
	 */
	default Class<? extends TapArray> getTapArrayClass() {
		return TapArray.class;
	}

	/**
	 * Get the Tap Value container class for Tap Type {@code Map}. The default container
	 * for {@code Map} values is {@link TapMap}.
	 *
	 * <p> If you want to extend the Tap Value container for Tap Type {@code Map}, you
	 * should overwrite this function and return the extended class. You can find more
	 * information on how to extend the container at {@link TapMap}. </p>
	 *
	 * @return Tap Value container class for Tap Type {@code Map}.
	 */
	default Class<? extends TapMap> getTapMapClass() {
		return TapMap.class;
	}

	/**
	 * Convert origin value get from databases into Tap Value container according to the
	 * Tap Type.
	 *
	 * @param field  Field descriptions.
	 * @param origin Origin value get from databases.
	 * @return Tap Value container.
	 */
	default AbstractTapValue<?> convertToTapValue(ConnectorContext ctx, RelateDatabaseField field, Object origin)
			throws Exception {
		String tapType = field.getTapType().toLowerCase();
		Class<? extends AbstractTapValue<?>> tapValueClass;
		switch (tapType) {
			case "boolean":
				tapValueClass = getTapBooleanClass();
				break;
			case "bytes":
				tapValueClass = getTapBytesClass();
				break;
			case "date":
				tapValueClass = getTapDateClass();
				break;
			case "datetime":
			case "datetime_with_timezone":
				tapValueClass = getTapDatetimeClass();
				break;
			case "number":
				tapValueClass = getTapNumberClass();
				break;
			case "string":
				tapValueClass = getTapStringClass();
				break;
			case "time":
			case "time_with_timezone":
				tapValueClass = getTapTimeClass();
				break;
			case "array":
				tapValueClass = getTapArrayClass();
				break;
			case "map":
				tapValueClass = getTapMapClass();
				break;
			default:
				throw new IllegalStateException("Unexpected tapType value: " + tapType + ".");
		}
		// TODO(zhangxin): use more elegant way to invoke the function, such as builders?
		AbstractTapValue<?> tapValue = TapValueFactory.newTapValueContainer(tapValueClass, origin);
		Method setCtx = tapValueClass.getMethod("setContext", ConnectorContext.class);
		setCtx.invoke(tapValue, ctx);
		Method setField = tapValueClass.getMethod("setField", RelateDatabaseField.class);
		setField.invoke(tapValue, field);

		return tapValue;
	}

	/**
	 * Convert Tap Value container into value which can be accepted by the target
	 * database according to the database data type.
	 *
	 * @param container Tap Value container.
	 * @param getter    The getter function name.
	 * @return Value can be accepted by target database.
	 */
	default Object convertFromTapValue(AbstractTapValue<?> container, String getter)
			throws Exception {
		String tapType = container.getField().getTapType().toLowerCase();
		Class<? extends AbstractTapValue<?>> tapValueClass;
		Class<? extends AbstractTapValue<?>> tapValueOriginClass;
		switch (tapType) {
			case "boolean":
				tapValueClass = getTapBooleanClass();
				tapValueOriginClass = TapBoolean.class;
				break;
			case "bytes":
				tapValueClass = getTapBytesClass();
				tapValueOriginClass = TapBytes.class;
				break;
			case "date":
				tapValueClass = getTapDateClass();
				tapValueOriginClass = TapDate.class;
				break;
			case "datetime":
			case "datetime_with_timezone":
				tapValueClass = getTapDatetimeClass();
				tapValueOriginClass = TapDatetime.class;
				break;
			case "number":
				tapValueClass = getTapNumberClass();
				tapValueOriginClass = TapNumber.class;
				break;
			case "string":
				tapValueClass = getTapStringClass();
				tapValueOriginClass = TapString.class;
				break;
			case "time":
			case "time_with_timezone":
				tapValueClass = getTapTimeClass();
				tapValueOriginClass = TapTime.class;
				break;
			case "array":
				tapValueClass = getTapArrayClass();
				tapValueOriginClass = TapArray.class;
				break;
			case "map":
				tapValueClass = getTapMapClass();
				tapValueOriginClass = TapMap.class;
				break;
			default:
				throw new IllegalStateException("Unexpected tapType value: " + tapType + ".");
		}
		if (!tapValueOriginClass.isInstance(container)) {
			throw new ConvertException("Unexpected arg type for getter " + getter + ", " + container.getClass().getName() +
					"is provided while " + tapValueOriginClass.getName() + " is expected.");
		}
		Method getterMethod = tapValueClass.getMethod(getter, AbstractTapValue.class);
		return getterMethod.invoke(tapValueClass.newInstance(), container);
	}

	class ConverterContext {
		Connections sourceConn;
		Connections targetConn;
		SettingService settingService;

		public ConverterContext(Connections sourceConn, Connections targetConn) {
			this.sourceConn = sourceConn;
			this.targetConn = targetConn;
		}

		public ConverterContext(Connections sourceConn, Connections targetConn, SettingService settingService) {
			this.sourceConn = sourceConn;
			this.targetConn = targetConn;
			this.settingService = settingService;
		}

		// 2.0 以后 HazelcastTypeConverterNode 目标数据节点不一定能取到源的连接
		// 遇到目标需要使用源 Connections 时，需要将其逻辑移除，使用其它方式实现
		@Deprecated
		public Connections getSourceConn() {
			return sourceConn;
		}

		// 2.0 以后 HazelcastTypeConverterNode 源数据节点不一定能取到目标的连接
		// 遇到源需要使用源 Connections 时，需要将其逻辑移除，使用其它方式实现
		@Deprecated
		public Connections getTargetConn() {
			return targetConn;
		}

		public SettingService getSettingService() {
			return settingService;
		}
	}
}
