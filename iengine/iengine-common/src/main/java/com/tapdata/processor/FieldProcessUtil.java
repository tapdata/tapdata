package com.tapdata.processor;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.JdbcUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.constant.StringUtil;
import com.tapdata.constant.TapList;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.processor.error.FieldProcessException;
import com.tapdata.processor.error.FieldProcessRuntimeException;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.value.DateTime;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.comment.Comment;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author jackin
 */
public class FieldProcessUtil {

	private static final Logger logger = LogManager.getLogger(FieldProcessUtil.class);
	private static final String CONVERT_ERROR_TEMPLATE = "Convert type %s to %s does not supported, value: %s";

	private FieldProcessUtil() {
	}

	public static void fieldProcess(Map<String, Object> data, List<FieldProcess> fieldsProcess) throws Exception {
		fieldProcess(data, fieldsProcess, new HashSet<>(), false);
	}

	public static void fieldProcess(Map<String, Object> data, List<FieldProcess> fieldsProcess, Set<String> rollbackRemoveFields, boolean deleteAllFields) throws Exception {
		// 记录字段改名的隐射关系
		Map<String, String> renameMapping = new HashMap<>();

		if (deleteAllFields) {
			Map<String, Object> rollbackRemoveRecord = new HashMap<>();
			rollbackRemoveFields.forEach(f -> rollbackRemoveRecord.put(f, MapUtilV2.getValueByKeyV2(data, f)));
			data.clear();
			for (Map.Entry<String, Object> entry : rollbackRemoveRecord.entrySet()) {
				MapUtilV2.putValueInMap(data, entry.getKey(), entry.getValue());
			}
		}

		for (FieldProcess process : fieldsProcess) {
			String field = process.getField();
			String op = process.getOp();
			FieldProcess.FieldOp fieldOp = FieldProcess.FieldOp.fromOperation(op);
			switch (fieldOp) {
				case OP_CONVERT:

					convertDataTyeProcess(data, process, renameMapping);
					break;

				case OP_REMOVE:
					if (!rollbackRemoveFields.contains(field)) {
						MapUtilV2.removeValueByKey(data, field);
					}
					break;

				case OP_RENAME:

					renameField(data, renameMapping, process, field);
					break;

				case OP_CREATE:

					try {
						addFieldDefaultValue(process, field, data, renameMapping);
					} catch (ParseException e) {
						logger.warn("Add new field failed, err: {}, field name: {}", e.getMessage(), field);
					}
					break;

				default:
					break;
			}
		}
	}

	public static boolean fieldProcess(TableIndex tableIndex, List<FieldProcess> fieldsProcess) throws FieldProcessException {
		for (FieldProcess process : fieldsProcess) {
			FieldProcess.FieldOp fieldOp = FieldProcess.FieldOp.fromOperation(process.getOp());
			try {
				switch (fieldOp) {
					case OP_REMOVE:
						if (null != tableIndex.getColumns()) {
							for (TableIndexColumn tic : tableIndex.getColumns()) {
								if (tic.getColumnName().equals(process.getField())) {
									logger.warn("Index field '{}' is remove, ignore index: {}", tic.getColumnName(), tableIndex);
									return false;
								}
							}
						}
						break;
					case OP_RENAME:
						if (null != tableIndex.getColumns()) {
							for (TableIndexColumn tic : tableIndex.getColumns()) {
								if (tic.getColumnName().equals(process.getField())) {
									tic.setColumnName(process.getOperand());
								}
							}
						}
						break;
					default:
						break;
				}
			} catch (Exception e) {
				throw new FieldProcessException(String.format("Field process failed, op: %s, field name: %s, operand: %s", fieldOp.name(), process.getField(), process.getOperand()), e);
			}
		}
		return true;
	}

	/**
	 * alter语句处理
	 *
	 * @param alter
	 * @param fieldProcessMap
	 * @param databaseTypeEnum
	 * @return true 继续处理，false 不需要执行的sql
	 */
	public static boolean alterProcess(Alter alter, Map<String, FieldProcess> fieldProcessMap, DatabaseTypeEnum databaseTypeEnum) {
		List<AlterExpression> alterExpressions = alter.getAlterExpressions();
		Iterator<AlterExpression> iterator = alterExpressions.iterator();
		while (iterator.hasNext()) {
			AlterExpression alterExpression = iterator.next();
			switch (alterExpression.getOperation()) {
				case RENAME:
					String columnOldName = alterExpression.getColumnOldName();
					FieldProcess fieldProcess = fieldProcessMap.get(columnOldName);
					if (fieldProcess != null && FieldProcess.FieldOp.fromOperation(fieldProcess.getOp()) == FieldProcess.FieldOp.OP_RENAME) {
						alterExpression.setColumnOldName(JdbcUtil.formatFieldName(fieldProcess.getOperand(), databaseTypeEnum.getType()));
					} else {
						iterator.remove();
					}
					break;
				case DROP:
					List<String> pkColumns = alterExpression.getPkColumns();
					ListIterator<String> pkIterator = pkColumns.listIterator();
					while (pkIterator.hasNext()) {
						String pkColumnName = pkIterator.next();
						fieldProcess = fieldProcessMap.get(pkColumnName);
						if (fieldProcess != null && FieldProcess.FieldOp.fromOperation(fieldProcess.getOp()) == FieldProcess.FieldOp.OP_RENAME) {
							pkIterator.set(JdbcUtil.formatFieldName(fieldProcess.getOperand(), databaseTypeEnum.getType()));
						} else {
							pkIterator.remove();
						}
					}
					if (CollectionUtils.isEmpty(pkColumns)) {
						iterator.remove();
					}
					break;
				case MODIFY:
					List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
					Iterator<AlterExpression.ColumnDataType> columnDataTypeIterator = colDataTypeList.iterator();
					while (columnDataTypeIterator.hasNext()) {
						AlterExpression.ColumnDataType columnDataType = columnDataTypeIterator.next();
						String columnDataTypeColumnName = columnDataType.getColumnName();
						fieldProcess = fieldProcessMap.get(columnDataTypeColumnName);
						if (fieldProcess != null && FieldProcess.FieldOp.fromOperation(fieldProcess.getOp()) == FieldProcess.FieldOp.OP_RENAME) {
							columnDataType.setColumnName(JdbcUtil.formatFieldName(fieldProcess.getOperand(), databaseTypeEnum.getType()));
						} else {
							columnDataTypeIterator.remove();
						}
					}
					if (CollectionUtils.isEmpty(colDataTypeList)) {
						iterator.remove();
					}
					break;
				default:
					break;
			}
		}
		return !CollectionUtils.isEmpty(alterExpressions);
	}

	/**
	 * comment语句处理
	 *
	 * @param comment
	 * @param fieldProcessMap
	 * @param databaseTypeEnum
	 * @return
	 */
	public static boolean commentProcess(Comment comment, Map<String, FieldProcess> fieldProcessMap, DatabaseTypeEnum databaseTypeEnum) {

		Column column = comment.getColumn();
		String columnName = column.getColumnName();
		FieldProcess fieldProcess = fieldProcessMap.get(columnName);
		if (fieldProcess != null && FieldProcess.FieldOp.fromOperation(fieldProcess.getOp()) == FieldProcess.FieldOp.OP_RENAME) {
			column.setColumnName(JdbcUtil.formatFieldName(fieldProcess.getOperand(), databaseTypeEnum.getType()));
			return true;
		} else {
			return false;
		}
	}

	private static void renameField(Map<String, Object> data, Map<String, String> renameMapping, FieldProcess process, String field) throws Exception {
		Object value = MapUtilV2.getValueByKey(data, field);
		if (value instanceof TapList || value instanceof NotExistsNode) {
			return;
		}

		String operand = process.getOperand();
		if (StringUtils.isBlank(operand)) {
			return;
		}
		if (StringUtils.isNotBlank(field)) {
			int index = field.lastIndexOf(".");
			if (index > 0) {
				operand = field.substring(0, index + 1) + operand;
			}
		}

		if (!renameMapping.containsKey(operand)) {
			MapUtilV2.removeValueByKey(data, field);
		}
		renameMapping.put(field, operand);
		MapUtilV2.putValueInMap(data, operand, value);
	}

	public static void sortFieldProcess(List<FieldProcess> fieldsProcess) {
		if (CollectionUtils.isNotEmpty(fieldsProcess)) {
			Collections.sort(fieldsProcess, (f1, f2) -> {
				FieldProcess.FieldOp field1Op = FieldProcess.FieldOp.fromOperation(f1.getOp());
				FieldProcess.FieldOp field2Op = FieldProcess.FieldOp.fromOperation(f2.getOp());

				if (field1Op == FieldProcess.FieldOp.OP_CREATE && field2Op == FieldProcess.FieldOp.OP_CREATE) {
					String fieldName1 = f1.getField();
					String fieldName2 = f2.getField();
					String[] split1 = fieldName1.split("\\.");
					String[] split2 = fieldName2.split("\\.");

					return Integer.compare(split1.length, split2.length);
				}
				if (field1Op == FieldProcess.FieldOp.OP_RENAME && field2Op == FieldProcess.FieldOp.OP_RENAME) {
					String fieldName1 = f1.getField();
					String fieldName2 = f2.getField();
					String[] split1 = fieldName1.split("\\.");
					String[] split2 = fieldName2.split("\\.");

					return Integer.compare(split2.length, split1.length);
				}

				return Integer.compare(field1Op.getSort(), field2Op.getSort());
			});
		}
	}

	private static void addFieldDefaultValue(FieldProcess fieldProcess, String field, Map<String, Object> data, Map<String, String> renameMapping) throws Exception {
		String javaType = fieldProcess.getJavaType();
		Object valueByKey = MapUtilV2.getValueByKeyV2(data, field);
		Object defaultValue = getDefaultValue(javaType);
		valueByKey = valueByKey != null ? valueByKey : defaultValue;
		field = handleRename(field, renameMapping);
		if (valueByKey instanceof TapList && CollectionUtils.isNotEmpty((TapList) valueByKey)) {

			if (MapUtil.needSplit(field)) {
				int lastIndexOf = field.lastIndexOf(".");
				if (lastIndexOf > 0) {
					String parentKey = field.substring(0, lastIndexOf);
					String addKey = field.substring(lastIndexOf + 1);

					Object parentValue = MapUtilV2.getValueByKey(data, parentKey);

					if (parentValue instanceof TapList) {

						CollectionUtil.putInTapList((TapList) parentValue, addKey, defaultValue);
						MapUtilV2.putValueInMap(data, parentKey, parentValue);
					}
				}
			}

		} else {
			MapUtilV2.putValueInMap(data, field, valueByKey);
		}
	}

	private static Object getDefaultValue(String javaType) {
		Object result = null;
		switch (javaType) {
			case "Map":
				result = new HashMap<>();
				break;
			case "String":
				result = "";
				break;
			case "Array":
				result = new ArrayList<>();
				break;
			case "Double":
				result = 0D;
				break;
			case "Integer":
				result = 0;
				break;
			case "Long":
				result = 0L;
				break;
			case "Date":
				result = new Date();
				break;
			default:
				break;
		}

		return result;
	}

	private static void convertDataTyeProcess(Map<String, Object> data, FieldProcess filedProcess,
											  Map<String, String> renameMapping) throws Exception {

		if (MapUtils.isNotEmpty(data)) {
			String field = filedProcess.getField();

			// 从renameMapping里面，找到改名后的字段
			// 防止改名后无法获取到值，导致无法正确进行类型转换
			field = handleRename(field, renameMapping);

			String newDataType = filedProcess.getOperand();
			String dataType = filedProcess.getOriginalDataType();

			Object value = MapUtilV2.getValueByKeyV2(data, field);

			Object afterConvertValue = convertType(newDataType, dataType, value);

			if (afterConvertValue == null) {
				return;
			}

			MapUtilV2.putValueInMap(data, field, afterConvertValue);
		}
	}

	/**
	 * 从renameMapping里面，找到改名后的字段
	 * 防止改名后无法获取到值，导致无法正确进行类型转换
	 *
	 * @param field
	 * @param renameMapping
	 * @return
	 */
	private static String handleRename(String field, Map<String, String> renameMapping) {

		if (StringUtils.isBlank(field) || MapUtils.isEmpty(renameMapping)) {
			return field;
		}

		if (MapUtil.needSplit(field)) {

			List<String> keys = StringUtil.splitKey2List(field, "\\.");
			List<String> newKeys = new ArrayList<>();

			if (CollectionUtils.isEmpty(keys)) {
				return field;
			}

			for (int i = 0; i < keys.size(); i++) {
				String key = keys.get(i);
				String keyConcatPreKey = keys.subList(0, i + 1).stream().collect(Collectors.joining("."));

				if (renameMapping.containsKey(keyConcatPreKey)) {
					keyConcatPreKey = renameMapping.get(keyConcatPreKey);
					if (MapUtil.needSplit(keyConcatPreKey)) {
						List<String> splitKeys = StringUtil.splitKey2List(keyConcatPreKey, "\\.");
						if (CollectionUtils.isNotEmpty(splitKeys)) {
							newKeys.add(splitKeys.get(i));
						}
					} else {
						newKeys.add(keyConcatPreKey);
					}
				} else {
					newKeys.add(key);
				}
			}

			field = newKeys.stream().collect(Collectors.joining("."));

		} else {
			field = renameMapping.getOrDefault(field, field);
		}

		return field;
	}

	private static Object convertType(String newDataType, String dataType, Object value) throws FieldProcessException {
		Object afterConvertValue = null;
		if (value instanceof TapList) {
			try {
				convertTapList((TapList) value, newDataType);
			} catch (Exception e) {
				throw new FieldProcessException(String.format("Convert embedded list value %s to %s failed: %s, value: %s",
						e.getMessage(), dataType, newDataType, value), e);
			}
		} else {
			try {
				afterConvertValue = convert(value, newDataType);
			} catch (Exception e) {
				throw new FieldProcessException(String.format("Convert value %s to %s failed: %s, value: %s", dataType, newDataType, e.getMessage(), value), e);
			}
		}
		return afterConvertValue == null ? value : afterConvertValue;
	}

	private static void convertTapList(TapList value, String newDataType) {
		if (CollectionUtils.isEmpty(value) || StringUtils.isBlank(newDataType)) {
			return;
		}

		for (int i = 0; i < value.size(); i++) {
			Object tapValue = value.getValue(i);
			if (tapValue instanceof TapList) {
				convertTapList((TapList) tapValue, newDataType);
			} else {
				tapValue = convert(tapValue, newDataType);
				value.setValue(i, tapValue);
			}
		}
	}

	protected static Object convert(Object value, String newDataType) {
		if (value == null || StringUtils.isBlank(newDataType)) {
			return value;
		}
		switch (newDataType.toUpperCase()) {
			case "STRING":
				if (value instanceof Map || value instanceof List) {
					try {
						value = JSONUtil.obj2Json(value);
					} catch (Throwable e) {
						throw new FieldProcessRuntimeException(String.format("Convert %s to json string failed, value: %s", value.getClass().getSimpleName(), value), e);
					}
				} else if (value instanceof DateTime) {
					Instant instant = ((DateTime) value).toInstant();
					value = instant.toString();
				} else {
					value = String.valueOf(value);
				}
				break;
			case "INT":
			case "INTEGER":
				value = convertNumber(value, s -> new BigDecimal(s).intValue());
				break;
			case "DOUBLE":
				value = convertNumber(value, s -> new BigDecimal(s).doubleValue());
				break;
			case "SHORT":
				value = convertNumber(value, s -> new BigDecimal(s).shortValue());
				break;
			case "FLOAT":
				value = convertNumber(value, s -> new BigDecimal(s).floatValue());
				break;
			case "LONG":
				value = convertNumber(value, s -> new BigDecimal(s).longValue());
				break;
			case "DATE":
				value = convert2Date(value);
				break;
			case "BOOLEAN":
				if (value instanceof String) {
					value = Boolean.valueOf((String) value);
				} else if (value instanceof Number) {
					value = (((Number) value).intValue() != 0);
				} else {
					throw new FieldProcessRuntimeException(String.format(CONVERT_ERROR_TEMPLATE, value.getClass().getSimpleName(), newDataType, value));
				}
				break;
			case "BIGDECIMAL":
				value = new BigDecimal(new BigDecimal(String.valueOf(value)).stripTrailingZeros().toPlainString());
				break;
			case "MAP":
			case "ARRAY":
				if (value instanceof String) {
					String json = String.valueOf(value);
					if (StringUtils.isNotBlank(json)) {
						String firstChar = json.substring(0, 1);
						if (firstChar.equals("{")) {
							try {
								value = JSONUtil.json2Map(json);
							} catch (Throwable e) {
								throw new FieldProcessRuntimeException("Convert json string to map failed, value: " + StringUtils.substring(json, 0, 20) + ", error message: " + e.getMessage(), e);
							}
						} else if (firstChar.equals("[")) {
							try {
								value = JSONUtil.json2List(json, Object.class);
							} catch (Throwable e) {
								throw new FieldProcessRuntimeException("Convert string to array failed, value: " + StringUtils.substring(json, 0, 20) + ", error message: " + e.getMessage(), e);
							}
						} else {
							throw new FieldProcessRuntimeException("Value is not a json, cannot convert to " + newDataType);
						}
					} else {
						// convert "" to empty map
						value = new HashMap<>();
					}

				} else {
					throw new FieldProcessRuntimeException(String.format(CONVERT_ERROR_TEMPLATE, value.getClass().getSimpleName(), newDataType, value));
				}
				break;
			default:
				break;
		}

		return value;
	}

	public static Date convert2Date(Object value) {
		if (value == null) {
			return null;
		}
		Date result;
		try {
			if (value instanceof String) {
				result = AnyTimeToDateTime.withDateStr((String) value).toDate();
			} else {
				DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
				result = dateTime.toDate();
			}
		} catch (Throwable e) {
			throw new FieldProcessRuntimeException(String.format("Convert value %s to Date failed , error message: %s ", value, e.getMessage()), e);
		}
		return result;
	}

	protected static  <T> T convertNumber(Object value, Function<String, T> fn) {
		value = handleDateTime(value);
		String str = String.valueOf(value);
		if (StringUtils.isBlank(str)) {
			return null;
		}
		return fn.apply(str);
	}

	protected static Object handleDateTime(Object value) {
		if (value instanceof DateTime) {
			value = ((DateTime) value).toEpochMilli();
		}
		return value;
	}
}
