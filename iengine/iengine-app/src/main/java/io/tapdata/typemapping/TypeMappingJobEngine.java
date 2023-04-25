package io.tapdata.typemapping;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.DbType;
import com.tapdata.entity.TapType;
import com.tapdata.entity.TypeMapping;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.annotation.DatabaseTypeAnnotations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2021-08-04 14:26
 **/
public class TypeMappingJobEngine extends BaseTypeMapping {

	public static Logger logger = LogManager.getLogger(TypeMappingJobEngine.class);

	public static Map<DatabaseTypeEnum, Class<TypeMappingProvider>> typeEnumClassMap;

	static {
		try {
			typeEnumClassMap = TypeMappingJobEngine.scanTypeMappingImpl();
		} catch (Exception e) {
			logger.error("init type mapping failed {}", e.getMessage(), e);
		}
	}

	public TypeMappingJobEngine() {
	}

	public TypeMappingJobEngine(ClientMongoOperator clientMongoOperator) throws Exception {
		super(clientMongoOperator);
	}

	@Override
	protected List<TypeMapping> initTypeMappings() throws Exception {
		if (clientMongoOperator == null) {
			throw new Exception("ClientMongoOperator cannot be null");
		}
		Map<DatabaseTypeEnum, Class<TypeMappingProvider>> databaseTypeMappingImplMap = scanTypeMappingImpl();
		readAndWriteTypeMapping(
				databaseTypeMappingImplMap,
				this::clearTypeMappingByDatabaseTypeFromMongo,
				this::writeTypeMappingToMongo
		);

		return typeMappings;
	}

	private void readAndWriteTypeMapping(Map<DatabaseTypeEnum, Class<TypeMappingProvider>> databaseTypeMappingImplMap,
										 Consumer<DatabaseTypeEnum> clearTypeMapping,
										 Consumer<List<TypeMapping>> writeTypeMapping) throws Exception {
		if (MapUtils.isNotEmpty(databaseTypeMappingImplMap)) {
			ExecutorUtil executorUtil = new ExecutorUtil();
			executorUtil.queueMultithreading(new ArrayList<>(databaseTypeMappingImplMap.keySet()), null, databaseTypeEnum -> {
				try {
					List<TypeMapping> typeMappings = new ArrayList<>();
					Class<TypeMappingProvider> typeMappingProviderImplClz = databaseTypeMappingImplMap.get(databaseTypeEnum);
					Object instance = typeMappingProviderImplClz.newInstance();
					Method[] methods = typeMappingProviderImplClz.getMethods();
					Map<String, List<TypeMapping>> tapTypeMap = new HashMap<>();
					for (Method method : methods) {
						String methodName = method.getName();
						if (!checkMethodName(methodName)) {
							continue;
						}

						Object result = method.invoke(instance);
						if (!(result instanceof List)) {
							continue;
						}
						for (Object dbType : ((List<?>) result)) {
							if (!(dbType instanceof DbType)) {
								continue;
							}

							TapType tapType = getTapType(methodName);
							if (tapType == null) {
								continue;
							}

							TypeMapping typeMapping = TypeMapping.TypeMappingBuilder
									.builder(databaseTypeEnum.getType(), ((DbType) dbType).getDbType(), tapType)
									.withMinPrecision(((DbType) dbType).getMinPrecision())
									.withMaxPrecision(((DbType) dbType).getMaxPrecision())
									.withMinScale(((DbType) dbType).getMinScale())
									.withMaxScale(((DbType) dbType).getMaxScale())
									.withVersion(((DbType) dbType).getVersion())
									.withDbTypeDefault(((DbType) dbType).getDbTypeDefault())
									.withDirection(((DbType) dbType).getDirection())
									.withTapTypeDefault(((DbType) dbType).getTapTypeDefault())
									.withCode(((DbType) dbType).getCode())
									.withGetter(((DbType) dbType).getGetter())
									.withRangeValue(((DbType) dbType).getMinValue(), ((DbType) dbType).getMaxValue())
									.withFixed(((DbType) dbType).getFixed())
									.build();

							typeMappings.add(typeMapping);

							if (!tapTypeMap.containsKey(typeMapping.getTapType().name())) {
								List<TypeMapping> list = new ArrayList<>();
								tapTypeMap.put(typeMapping.getTapType().name(), list);
							}
							tapTypeMap.get(typeMapping.getTapType().name()).add(typeMapping);
						}
					}

					handleTapTypeDefault(tapTypeMap);

					if (CollectionUtils.isNotEmpty(typeMappings)) {
						clearTypeMapping.accept(databaseTypeEnum);
						writeTypeMapping.accept(typeMappings);
//            typeMappings.clear();
					}
				} catch (Exception e) {
					logger.error("Init type mappings failed; Database type: " + databaseTypeEnum.getType() + "; Cause: " + e.getMessage(), e);
				}
			}, "INIT-TYPE-MAPPINGS");
		}
	}

	/**
	 * 处理tapTypeDefault属性
	 * 1. 如果某个TapType里面只有一个类型，将该属性设置为true
	 * 2. 如果某个TapType里面有多个类型，该属性都为true，则报错
	 *
	 * @param tapTypeMap
	 * @throws Exception
	 */
	private void handleTapTypeDefault(Map<String, List<TypeMapping>> tapTypeMap) throws Exception {
		if (MapUtils.isEmpty(tapTypeMap)) {
			return;
		}
		for (String key : tapTypeMap.keySet()) {
			List<TypeMapping> list = tapTypeMap.get(key);
			if (CollectionUtils.isEmpty(list)) {
				continue;
			}
			if (list.size() == 1) {
				list.get(0).setTapTypeDefault(true);
			}
			if (list.stream().filter(TypeMapping::getTapTypeDefault).count() > 1) {
				throw new Exception("Tap type must only have one tapTypeDefault, database type: " + list.get(0).getDatabaseType() + ", tap type: " + key);
			}
		}
	}

	private void clearTypeMappingByDatabaseTypeFromMongo(DatabaseTypeEnum databaseTypeEnum) {
		Map<String, Object> params = new HashMap<>();
		params.put("databaseType", databaseTypeEnum.getType());
		clientMongoOperator.deleteAll(params, ConnectorConstant.TYPE_MAPPINGS_COLLECTION);
		logger.info("Clear " + databaseTypeEnum.getType() + "'s type mappings");
	}

	private void writeTypeMappingToMongo(List<TypeMapping> typeMappings) {
		if (CollectionUtils.isNotEmpty(typeMappings)) {
			clientMongoOperator.insertList(typeMappings, ConnectorConstant.TYPE_MAPPINGS_COLLECTION);
			logger.info("Write " + typeMappings.get(0).getDatabaseType() + "'s type mapping");
		}
	}

	public static Map<DatabaseTypeEnum, Class<TypeMappingProvider>> scanTypeMappingImpl() throws Exception {
		Map<DatabaseTypeEnum, Class<TypeMappingProvider>> databaseTypeMappingImplMap = new HashMap<>();
		Set<Class<?>> matchComponents;
		try {
			matchComponents = BeanUtil.findMatchComponents(TYPE_PACKAGE_NAME);
		} catch (IOException e) {
			String err = "Scan class failed, package: " + TYPE_PACKAGE_NAME + ", cause: " + e.getMessage();
			throw new Exception(err, e);
		}

		if (CollectionUtils.isNotEmpty(matchComponents)) {
			matchComponents.forEach(matchComponent -> {
				if (!BeanUtil.hasInterface(matchComponent, TypeMappingProvider.class, true)) {
					return;
				}

				DatabaseTypeAnnotation databaseTypeAnnotation = matchComponent.getAnnotation(DatabaseTypeAnnotation.class);
				DatabaseTypeAnnotations databaseTypeAnnotations = matchComponent.getAnnotation(DatabaseTypeAnnotations.class);
				if (null == databaseTypeAnnotation && null == databaseTypeAnnotations) {
					return;
				}

				if (databaseTypeAnnotation != null) {
					databaseTypeMappingImplMap.put(databaseTypeAnnotation.type(), (Class<TypeMappingProvider>) matchComponent);
				} else {
					DatabaseTypeAnnotation[] value = databaseTypeAnnotations.value();
					for (DatabaseTypeAnnotation typeAnnotation : value) {
						databaseTypeMappingImplMap.put(typeAnnotation.type(), (Class<TypeMappingProvider>) matchComponent);
					}
				}
			});
		}
		return databaseTypeMappingImplMap;
	}

	@Override
	public Class<TypeMappingProvider> getTypeMappingClazzByDatabaseType(DatabaseTypeEnum databaseTypeEnum) throws Exception {
		Map<DatabaseTypeEnum, Class<TypeMappingProvider>> typeMappings = scanTypeMappingImpl();
		return typeMappings.get(databaseTypeEnum);
	}
}
