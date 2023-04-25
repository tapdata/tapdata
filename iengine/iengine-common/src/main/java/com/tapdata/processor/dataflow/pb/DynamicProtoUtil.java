package com.tapdata.processor.dataflow.pb;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.tapdata.constant.DateUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.constant.TapList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DynamicProtoUtil {

	private static Logger logger = LogManager.getLogger(DynamicProtoUtil.class);

	private DynamicProtoUtil() {

	}

	public static void main(String[] args) {
		String str = "Unit.login.encryptionRules";

		System.out.println(str.substring(0, str.lastIndexOf(".")));
		System.out.println(str.substring(str.lastIndexOf(".") + 1));
	}

	/**
	 * @param dataMap
	 * @param pbConfiguration
	 * @param dataFieldMappingMap 数据字段名和模板中的字段名的映射关系
	 * @return
	 */
	public static byte[] getPbMsgByteArray(Map<String, Object> dataMap,
										   PbConfiguration pbConfiguration,
										   Map<String, String> dataFieldMappingMap) {

		//设置基础属性值
		Map<String, String> filedMsgDefNameMappingMap = pbConfiguration.getFiledMsgDefNameMappingMap();
		DynamicSchema pbSchema = pbConfiguration.getSchema();
		Map<String, Object> msgBuilderMap = getMsgBuilderMap(dataMap, dataFieldMappingMap,
				filedMsgDefNameMappingMap, pbSchema);

		DynamicMessage.Builder dynMsgBuilder = getDynMsgBuilder(filedMsgDefNameMappingMap, pbSchema, msgBuilderMap);
		return dynMsgBuilder.build().toByteArray();
	}

	private static Map<String, Object> getMsgBuilderMap(Map<String, Object> dataMap,
														Map<String, String> dataFieldMappingMap,
														Map<String, String> filedMsgDefNameMappingMap,
														DynamicSchema pbSchema) {
		/**
		 * msgBuilderMap中的值示例：
		 *  1、Unit.login: DynamicMessage.Builder
		 *  2、Unit.list: Map<Integer, DynamicMessage.Builder
		 */


		Map<String, Object> msgBuilderMap = new HashMap<>();
		for (Map.Entry<String, String> entry : dataFieldMappingMap.entrySet()) {
			//eg: Unit.login.a
			String entryKey = entry.getKey();
			//eg: Unit.login
			String msgFieldFullName = entryKey.substring(0, entryKey.lastIndexOf("."));
			//eg: Unit.Login
			String msgTypeName = filedMsgDefNameMappingMap.get(msgFieldFullName);
			//eg: a
			String msgFieldShortName = entryKey.substring(entryKey.lastIndexOf(".") + 1);

			Object value = MapUtilV2.getValueByKey(dataMap, entry.getValue());
			if (value instanceof List) {
				//数组格式
				TapList tapList = (TapList) value;
//        Map<Integer, DynamicMessage.Builder> listMsgBuilderMap = (Map<Integer, DynamicMessage.Builder>) msgBuilderMap.get(entryKey);
//        if (listMsgBuilderMap == null) {
//          listMsgBuilderMap = new HashMap<>();
//          msgBuilderMap.put(entryKey, listMsgBuilderMap);
//        }
				for (Object o : tapList) {
					Map<Integer, Object> map = (Map<Integer, Object>) o;
					//增加索引
//          Object index = map.get("index");
					DynamicMessage.Builder dynamicMessageBuilder = (DynamicMessage.Builder) msgBuilderMap.get(msgTypeName);
					if (dynamicMessageBuilder == null) {
						dynamicMessageBuilder = pbSchema.newMessageBuilder(msgTypeName);
						msgBuilderMap.put(msgFieldFullName, dynamicMessageBuilder);
//            listMsgBuilderMap.put((Integer) index, dynamicMessageBuilder);
					}
					Descriptors.Descriptor messageDescriptor = pbSchema.getMessageDescriptor(msgTypeName);
					Descriptors.FieldDescriptor fieldByName = messageDescriptor.findFieldByName(msgFieldShortName);
					if (fieldByName.isRepeated()) {
						dynamicMessageBuilder.addRepeatedField(fieldByName, convertValue(map.get("value"), fieldByName));
					} else {
						dynamicMessageBuilder.setField(fieldByName, convertValue(map.get("value"), fieldByName));
					}
				}
			} else {
				//非数组形式
				DynamicMessage.Builder dynamicMessageBuilder = (DynamicMessage.Builder) msgBuilderMap.get(msgFieldFullName);
				if (dynamicMessageBuilder == null) {
					dynamicMessageBuilder = pbSchema.newMessageBuilder(msgTypeName);
					msgBuilderMap.put(msgFieldFullName, dynamicMessageBuilder);
				}
				Descriptors.Descriptor messageDescriptor = pbSchema.getMessageDescriptor(msgTypeName);
				Descriptors.FieldDescriptor fieldDescByName = messageDescriptor.findFieldByName(msgFieldShortName);
				if (fieldDescByName.isRepeated()) {
					dynamicMessageBuilder.addRepeatedField(fieldDescByName, convertValue(value, fieldDescByName));
				} else {
					dynamicMessageBuilder.setField(fieldDescByName, convertValue(value, fieldDescByName));
				}
			}

		}

		//msgBuilderMap 补充message属性值
		/**
		 * "Unit.login": "Unit.Login",
		 */
		Map<String, String> messageTypeMap = filedMsgDefNameMappingMap.entrySet().stream()
				.filter(e -> !dataFieldMappingMap.containsKey(e.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		for (Map.Entry<String, String> entry : messageTypeMap.entrySet()) {
			String entryKey = entry.getKey();
			//eg: Unit.login
			int lastIndexOf = entryKey.lastIndexOf(".");
			String msgFieldName = entryKey.substring(0, lastIndexOf < 0 ? entryKey.length() : lastIndexOf);
			//eg: Unit.Login
			String msgTypeName = filedMsgDefNameMappingMap.get(msgFieldName);
			if (!msgBuilderMap.containsKey(msgFieldName)) {
				DynamicMessage.Builder dynamicMessageBuilder = pbSchema.newMessageBuilder(msgTypeName);
				msgBuilderMap.put(msgFieldName, dynamicMessageBuilder);
			}
		}
		return msgBuilderMap;
	}

	/**
	 * 类型转换
	 *
	 * @param value
	 * @param fieldDescByName
	 * @return
	 */
	private static Object convertValue(Object value, Descriptors.FieldDescriptor fieldDescByName) {
		Descriptors.FieldDescriptor.Type fieldType = fieldDescByName.getType();
		Descriptors.FieldDescriptor.JavaType fieldJavaType = fieldType.getJavaType();
		Object defaultValue = getDefaultValue(fieldJavaType);
		if (value == null || value instanceof NotExistsNode) {
			//为空时设置默认值
			return defaultValue;
		}
		Class<?> aClass = value.getClass();
		if (aClass == Short.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					return ((Short) value).intValue();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					return ((Short) value).longValue();
				case DOUBLE:
					return ((Short) value).doubleValue();
				case FLOAT:
					return ((Short) value).floatValue();
				case STRING:
					return value.toString();
				case BOOL:
					return Boolean.parseBoolean(value.toString());
				case BYTES:
					return value.toString().getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Byte.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					return ((Byte) value).intValue();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					return ((Byte) value).longValue();
				case DOUBLE:
					return ((Byte) value).doubleValue();
				case FLOAT:
					return ((Byte) value).floatValue();
				case STRING:
					return value.toString();
				case BOOL:
					return Boolean.parseBoolean(value.toString());
				case BYTES:
					return value.toString().getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Long.class) {
			// 类型转换
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) value).intValue();
				case DOUBLE:
					return ((Long) value).doubleValue();
				case FLOAT:
					return ((Long) value).floatValue();
				case STRING:
					return ((Long) value).toString();
				case BOOL:
					return Boolean.parseBoolean(value.toString());
				case BYTES:
					return value.toString().getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Float.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Float) value).intValue();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Float) value).longValue();
				case DOUBLE:
					return ((Float) value).doubleValue();
				case STRING:
					return value.toString();
				case BOOL:
					return Boolean.parseBoolean(value.toString());
				case BYTES:
					return value.toString().getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Double.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Double) value).intValue();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Double) value).longValue();
				case FLOAT:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Double) value).floatValue();
				case STRING:
					return value.toString();
				case BOOL:
					return Boolean.parseBoolean(value.toString());
				case BYTES:
					return value.toString().getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Date.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) ((Date) value).getTime()).intValue();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Date) value).getTime();
				case FLOAT:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) ((Date) value).getTime()).floatValue();
				case DOUBLE:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) ((Date) value).getTime()).doubleValue();
				case STRING:
					return DateUtil.microseondsToStringTime(value);
				case BYTES:
					return DateUtil.microseondsToStringTime(value).getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == Instant.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Instant) value).getNano();
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Instant) value).getEpochSecond();
				case FLOAT:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) ((Instant) value).getEpochSecond()).floatValue();
				case DOUBLE:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return ((Long) ((Instant) value).getEpochSecond()).doubleValue();
				case STRING:
					return ((Long) ((Instant) value).getEpochSecond()).toString();
				case BYTES:
					return DateUtil.microseondsToStringTime(((Long) ((Instant) value).getEpochSecond()).toString()).getBytes();
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else if (aClass == String.class) {
			switch (fieldType) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return Integer.parseInt((String) value);
				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return Long.parseLong((String) value);
				case FLOAT:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return Float.parseFloat((String) value);
				case DOUBLE:
					logger.warn("value type [{}-{}] convert to fieldType[{}] ,May lose accuracy",
							value.getClass().toString(), value, fieldType.toString());
					return Double.parseDouble((String) value);
				case STRING:
				case BYTES:
					return value;
				default:
					logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
							value.getClass().toString(),
							fieldType.toString(),
							defaultValue);
					return defaultValue;
			}
		} else {
			logger.warn("value type [{}] can not convert to protobuf fieldType[{}], set defaultValue [{}]",
					value.getClass().toString(),
					fieldType.toString(),
					defaultValue);
			return defaultValue;

		}
	}

	/**
	 * 获取默认值
	 *
	 * @param fieldJavaType
	 * @return
	 */
	private static Object getDefaultValue(Descriptors.FieldDescriptor.JavaType fieldJavaType) {
		switch (fieldJavaType) {
			case INT:
				return 0;
			case BYTE_STRING:
				return ByteString.EMPTY;
			case LONG:
				return 0L;
			case FLOAT:
				return 0.0F;
			case DOUBLE:
				return 0.0D;
			case STRING:
				return "";
			case BOOLEAN:
				return false;
			case ENUM:
			case MESSAGE:
			default:
				return null;
		}
	}

	private static DynamicMessage.Builder getDynMsgBuilder(Map<String, String> filedMsgDefNameMappingMap,
														   DynamicSchema pbSchema,
														   Map<String, Object> msgBuilderMap) {
		//按照层数进行排序
		Set<String> msgFieldNameSet = msgBuilderMap.keySet().stream()
				.sorted(Comparator.comparing((Function<String, Integer>) value -> value.split("\\.").length).reversed())
				.collect(Collectors.toCollection(LinkedHashSet::new));

		//组装pb的层级关系
		for (String msgFieldName : msgFieldNameSet) {
			/**
			 * {
			 * 		"Unit.login.a": $unitLoginAMsgBuilder,
			 * 		"Unit.login" : $unitLoginMsgBuilder,
			 *     "Unit": $unitMsgBuilder,
			 * }
			 */

			int lastIndexOf = msgFieldName.lastIndexOf(".");
			if (lastIndexOf < 0) {
				break;
			}
			String parentFieldName = msgFieldName.substring(0, lastIndexOf);
			String parentMsgTypeName = filedMsgDefNameMappingMap.get(parentFieldName);

			String fieldName = msgFieldName.substring(lastIndexOf + 1);
			Object value = msgBuilderMap.get(msgFieldName);
			Object parentDynMsgBuilderObj = msgBuilderMap.get(parentFieldName);
			Descriptors.Descriptor messageDescriptor = pbSchema.getMessageDescriptor(parentMsgTypeName);
			Descriptors.FieldDescriptor fieldDesc = messageDescriptor.findFieldByName(fieldName);
			if (parentDynMsgBuilderObj instanceof Map) {
				Map<Integer, DynamicMessage.Builder> parentDynMsgBuilderMap = (Map<Integer, DynamicMessage.Builder>) parentDynMsgBuilderObj;
				if (value instanceof Map) {
					//数组格式
					Map<Integer, DynamicMessage.Builder> map = (Map<Integer, DynamicMessage.Builder>) value;
					for (Map.Entry<Integer, DynamicMessage.Builder> entry : map.entrySet()) {
						DynamicMessage.Builder parentDynMsgBuilder = parentDynMsgBuilderMap.get(entry.getKey());
						if (fieldDesc.isRepeated()) {
							parentDynMsgBuilder.addRepeatedField(fieldDesc, entry.getValue().build());
						} else {
							parentDynMsgBuilder.setField(fieldDesc, entry.getValue().build());
						}
					}
				} else {
					//非数组格式
					for (Map.Entry<Integer, DynamicMessage.Builder> parentDynMsgBuilderEntry : parentDynMsgBuilderMap.entrySet()) {
						if (fieldDesc.isRepeated()) {
							parentDynMsgBuilderEntry.getValue().addRepeatedField(fieldDesc, ((DynamicMessage.Builder) value).build());
						} else {
							parentDynMsgBuilderEntry.getValue().setField(fieldDesc, ((DynamicMessage.Builder) value).build());
						}
					}
				}

			} else {
				DynamicMessage.Builder parentDynMsgBuilder = (DynamicMessage.Builder) parentDynMsgBuilderObj;
				if (value instanceof Map) {
					//数组格式
					Map<Integer, DynamicMessage.Builder> map = (Map<Integer, DynamicMessage.Builder>) value;
					for (Map.Entry<Integer, DynamicMessage.Builder> entry : map.entrySet()) {
						parentDynMsgBuilder.addRepeatedField(fieldDesc, entry.getValue().build());
					}
				} else {
					//非数组格式
					if (fieldDesc.isRepeated()) {
						parentDynMsgBuilder.addRepeatedField(fieldDesc, ((DynamicMessage.Builder) value).build());
					} else {
						parentDynMsgBuilder.setField(fieldDesc, ((DynamicMessage.Builder) value).build());
					}
				}
			}

			msgBuilderMap.remove(msgFieldName);
		}

		if (msgBuilderMap.size() != 1) {
			throw new RuntimeException("error");
		}

		Optional<Object> first = msgBuilderMap.values().stream().findFirst();
		return (DynamicMessage.Builder) first.get();
	}

	/**
	 * 生成pb的schema
	 *
	 * @param model
	 * @return
	 * @throws Descriptors.DescriptorValidationException
	 */
	public static DynamicSchema generateSchema(PbModel model) throws Descriptors.DescriptorValidationException {
		DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
		schemaBuilder.setName(model.getName() + ".proto");
		schemaBuilder.addMessageDefinition(getMsgDef(model));

		return schemaBuilder.build();
	}

	/**
	 * 生成属性名和类型的映射map，如：
	 * <p>
	 * {
	 * "Unit.login": "Unit.Login",
	 * "Unit.login.encryptionRules": "string",
	 * "Unit.login.loginTime": "string",
	 * "Unit.login.platformName": "string",
	 * "Unit.login.loginSerialNumber": "string",
	 * "Unit.login.platformPassword": "string"
	 * }
	 *
	 * @param pbModel
	 * @return
	 */
	public static Map<String, String> getFieldTypeMappingMap(PbModel pbModel) {

		//返回的Map值为 类似 Login -> "Unit.Login"
		Map<String, String> typeNameFullMappingMap = new HashMap<>();
		//返回的Map值为 类似 Login -> $LoginPbModel
		Map<String, PbModel> typeNameModelMappingMap = new HashMap<>();
		setFullTypeNameMappingMap(pbModel, "", typeNameFullMappingMap, typeNameModelMappingMap);

//		Map<String, PbModel>
		//需要返回  类似  login -> Unit.Login、  login.loginTime -> string
		Map<String, String> propertyTypeMappingMap = new HashMap<>();
		propertyTypeMappingMap.put(pbModel.getName(), pbModel.getName());
		for (PbProperty pbProperty : pbModel.getPropertyList()) {
			//设置属性和类型的映射关系
			setPropertyTypeMapping(pbProperty, pbModel.getName() + ".",
					propertyTypeMappingMap, typeNameFullMappingMap, typeNameModelMappingMap);
		}
		return propertyTypeMappingMap;

	}

	private static void setPropertyTypeMapping(PbProperty pbProperty, String fieldNamePrefix,
											   Map<String, String> propertyTypeMappingMap,
											   Map<String, String> typeNameFullMappingMap,
											   Map<String, PbModel> typeNameModelMappingMap) {
		//fullTypeName不为空说明是复杂类型，需要继续遍历子节点的类型
		String fullTypeName = typeNameFullMappingMap.get(pbProperty.getType());
		propertyTypeMappingMap.put(fieldNamePrefix + pbProperty.getName(),
				StringUtils.isEmpty(fullTypeName) ? pbProperty.getType() : fullTypeName);
		if (StringUtils.isNotEmpty(fullTypeName)) {
			//fullTypeName不为空说明是复杂类型，需要继续遍历子节点的类型
			PbModel innerPbModel = typeNameModelMappingMap.get(fullTypeName);
			if (CollectionUtils.isNotEmpty(innerPbModel.getPropertyList())) {
				for (PbProperty innerProperty : innerPbModel.getPropertyList()) {
					String innerFieldNamePrefix = fieldNamePrefix + pbProperty.getName() + ".";
					setPropertyTypeMapping(innerProperty, innerFieldNamePrefix, propertyTypeMappingMap,
							typeNameFullMappingMap, typeNameModelMappingMap);
				}
			}

		}

	}

	private static void setFullTypeNameMappingMap(PbModel pbModel, String parentTypeName,
												  Map<String, String> typeNameFullMappingMap,
												  Map<String, PbModel> typeNameModelMappingMap) {
		if (typeNameFullMappingMap == null) {
			throw new RuntimeException("typeNameFullMappingMap is not allow null");
		}
		if (StringUtils.isEmpty(parentTypeName)) {
			parentTypeName = pbModel.getName();
		} else {
			parentTypeName = parentTypeName + "." + pbModel.getName();
		}
		typeNameFullMappingMap.put(pbModel.getName(), parentTypeName);
		typeNameModelMappingMap.put(parentTypeName, pbModel);
		if (CollectionUtils.isNotEmpty(pbModel.getNestedList())) {
			for (PbModel innerModel : pbModel.getNestedList()) {
				setFullTypeNameMappingMap(innerModel, parentTypeName, typeNameFullMappingMap, typeNameModelMappingMap);
			}
		}
	}


	private static MessageDefinition getMsgDef(PbModel pbModel) {
		MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder(pbModel.getName());
		//添加消息定义
		List<PbModel> nestedList = pbModel.getNestedList();
		if (CollectionUtils.isNotEmpty(nestedList)) {
			for (PbModel innerModel : nestedList) {
				MessageDefinition innerMsgDef = getMsgDef(innerModel);
				msgDefBuilder.addMessageDefinition(innerMsgDef);
			}
		}

		//添加属性
		if (CollectionUtils.isNotEmpty(pbModel.getPropertyList())) {
			for (PbProperty pbProperty : pbModel.getPropertyList()) {
				msgDefBuilder.addField(pbProperty.getLabel(), pbProperty.getType(), pbProperty.getName(), pbProperty.getNumber());
			}
		}

		return msgDefBuilder.build();
	}

}
