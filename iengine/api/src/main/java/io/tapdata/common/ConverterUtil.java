package io.tapdata.common;

import com.tapdata.entity.Connections;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.values.AbstractTapValue;
import io.tapdata.ConverterProvider;
import io.tapdata.exception.ConvertException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConverterUtil {
	private static final Logger logger = LogManager.getLogger(ConverterUtil.class);

	public static void schemaConvert(List<RelateDataBaseTable> schema, String databaseType) throws ConvertException {
		if (StringUtils.isNotBlank(databaseType) && CollectionUtils.isNotEmpty(schema)) {
			ConverterProvider converterProvider;
			Class<?> converterByDatabaseType = ClassScanner.getConverterByDatabaseType(databaseType);

			if (converterByDatabaseType != null) {
				try {
					converterProvider = (ConverterProvider) converterByDatabaseType.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					logger.error("Instantiation converter error, message: {}.", e.getMessage(), e);
					return;
				}

				if (converterProvider != null) {
					for (RelateDataBaseTable relateDataBaseTable : schema) {
						if (relateDataBaseTable != null) {
							List<RelateDatabaseField> fields = relateDataBaseTable.getFields();
							if (CollectionUtils.isNotEmpty(fields)) {
								List<RelateDatabaseField> newFields = new ArrayList<>();
								for (RelateDatabaseField field : fields) {
									if (field != null) {
										RelateDatabaseField relateDatabaseField = converterProvider.schemaConverter(field);
										newFields.add(relateDatabaseField);
									} else {
										continue;
									}
								}

								relateDataBaseTable.setFields(newFields);
							} else {
								continue;
							}
						} else {
							continue;
						}
					}
				}
			}
		}
	}

	public static ConverterProvider buildConverterProvider(Connections sourceConn, Connections targetConn, SettingService settingService, String databaseType) throws IllegalAccessException, InstantiationException {
		ConverterProvider converterProvider = null;

		if (StringUtils.isNotBlank(databaseType)) {
			Class<?> converterByDatabaseType = ClassScanner.getConverterByDatabaseType(databaseType);

			if (converterByDatabaseType == null) {
				return converterProvider;
			}
			converterProvider = (ConverterProvider) converterByDatabaseType.newInstance();
			if (converterProvider != null) {
				converterProvider.init(new ConverterProvider.ConverterContext(sourceConn, targetConn, settingService));
			}

		}

		return converterProvider;
	}

	public static void targetValueConvert(List<MessageEntity> msgs, ConverterProvider converterProvider) throws ConvertException {
		if (CollectionUtils.isNotEmpty(msgs) && converterProvider != null) {
			for (MessageEntity msg : msgs) {
				Map<String, Object> before = msg.getBefore();
				Map<String, Object> after = msg.getAfter();

				targetValueConvert(before, converterProvider);
				targetValueConvert(after, converterProvider);
			}
		}
	}

	public static void targetValueConvert(Map<String, Object> map, ConverterProvider converterProvider) throws ConvertException {
		if (converterProvider != null && MapUtils.isNotEmpty(map)) {

			Map<String, Object> tempBefore = converterProvider.targetFieldNameConverter(map);
			if (MapUtils.isNotEmpty(map)) {
				map.clear();
				map.putAll(tempBefore);
			}

			for (Map.Entry<String, Object> entry : map.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				try {
					if (StringUtils.isNotBlank(fieldName)) {
						value = converterProvider.commTargetValueConverter(value);
						value = converterProvider.targetValueConverter(value);

						map.put(fieldName, value);
					} else {
						continue;
					}
				} catch (ConvertException e) {
					logger.error("Convert field type failed, field name: {}, value: {}, type: {}, message: {}",
							fieldName, value, value.getClass().getName(), e.getMessage());
				}
			}
		}
	}

	public static void targetTapValueConvert(Map<String, String> fieldGetters, Map<String, Map<String, String>> tblFieldDbDataTypes, List<MessageEntity> msgs, ConverterProvider converterProvider) throws ConvertException {
		if (CollectionUtils.isNotEmpty(msgs) && converterProvider != null) {
			for (MessageEntity msg : msgs) {
				Map<String, Object> before = msg.getBefore();
				Map<String, Object> after = msg.getAfter();

				Map<String, Object> tempBefore = converterProvider.targetFieldNameConverter(before);
				if (MapUtils.isNotEmpty(before)) {
					before.clear();
					before.putAll(tempBefore);
				}

				Map<String, Object> tempAfter = converterProvider.targetFieldNameConverter(after);
				if (MapUtils.isNotEmpty(after)) {
					after.clear();
					after.putAll(tempAfter);
				}

				String tableName = msg.getTableName();
				if (msg.getMapping() != null) {
					tableName = msg.getMapping().getTo_table();
				}

				msg.setBefore(targetTapValueConvert(fieldGetters, tblFieldDbDataTypes.get(tableName), before, converterProvider));
				msg.setAfter(targetTapValueConvert(fieldGetters, tblFieldDbDataTypes.get(tableName), after, converterProvider));

			}
		}
	}

	public static Map<String, Object> targetTapValueConvert(Map<String, String> fieldGetters, Map<String, String> fieldDbDataTypes, Map<String, Object> fieldValueMap, ConverterProvider converterProvider)
			throws ConvertException {
		if (converterProvider != null && MapUtils.isNotEmpty(fieldValueMap)) {
			for (Map.Entry<String, Object> entry : fieldValueMap.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				ConvertVersion convertVersion = ConvertVersion.V2;
				String fieldType = null;
				if (fieldDbDataTypes == null) {
					convertVersion = ConvertVersion.V1;
				} else {
					fieldType = fieldDbDataTypes.getOrDefault(fieldName, "");
					if (StringUtils.isBlank(fieldType)) {
						convertVersion = ConvertVersion.V1;
					}
				}
				switch (convertVersion) {
					case V1:
						// old logic when the value is not wrapped
						try {
							if (value instanceof AbstractTapValue<?>) {
								value = ((AbstractTapValue<?>) value).getOrigin();
							}
							if (StringUtils.isNotBlank(fieldName)) {
								value = converterProvider.commTargetValueConverter(value);
								value = converterProvider.targetValueConverter(value);

								fieldValueMap.put(fieldName, value);
							} else {
								continue;
							}
						} catch (ConvertException e) {
							logger.error("Convert field type failed, field name: {}, value: {}, type: {}, message: {}",
									fieldName, value, value.getClass().getName(), e.getMessage());
						}
						break;
					case V2:
						// String get the getters
						String getter = fieldGetters.get(fieldType);
						if (getter == null) {
							continue;
						}
						try {
							value = converterProvider.convertFromTapValue((AbstractTapValue<?>) value, getter);
							fieldValueMap.put(fieldName, value);
						} catch (Exception e) {
							String errMsg = String.format("Failed to convert data at field %s with getter %s， fieldGetters: %s, filedDbDataTypes: %s, err: %s", fieldName, getter, fieldGetters, fieldDbDataTypes, e.getMessage());
							throw new ConvertException(e, errMsg);
						}
						break;
					default:
						break;
				}
			}
		}
		return fieldValueMap;
	}

	enum ConvertVersion {
		V1("v1"),
		V2("v2");

		private String version;

		ConvertVersion(String version) {
			this.version = version;
		}

		public String getVersion() {
			return version;
		}
	}
}
