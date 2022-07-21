package com.tapdata.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.PkgAnnoUtil;
import com.tapdata.entity.*;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.annotation.DatabaseTypeAnnotations;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.springframework.beans.factory.config.BeanDefinition;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;

/**
 * @author jackin
 */
public class ScriptUtil {

	private static Logger logger = LogManager.getLogger(ScriptUtil.class);

	public static final String FUNCTION_NAME = "process";

	public static final String SCRIPT_FUNCTION_NAME = "validate";

	/**
	 * 获取js引擎
	 *
	 * @param jsEngineName
	 * @return
	 */
	public static ScriptEngine getScriptEngine(String jsEngineName) {
		JSEngineEnum jsEngineEnum = JSEngineEnum.getByEngineName(jsEngineName);
		ScriptEngine scriptEngine;
		if (jsEngineEnum == JSEngineEnum.GRAALVM_JS) {
			scriptEngine = GraalJSScriptEngine
					.create(null,
							Context.newBuilder("js")
									.allowAllAccess(true)
									.allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
											.targetTypeMapping(Value.class, Object.class
													, v -> v.hasArrayElements() && v.hasMembers()
													, v -> v.as(List.class)
											).build()
									)
					);
		} else {
			scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.getEngineName());
		}
		return scriptEngine;
	}

	public static Invocable getScriptEngine(String jsEngineName, String script, List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator) throws ScriptException {

		if (StringUtils.isBlank(script)) {
			return null;
		}

		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			String buildInMethod = initBuildInMethod(javaScriptFunctions, clientMongoOperator);
			String scripts = new StringBuilder(script).append(System.lineSeparator()).append(buildInMethod).toString();
			ScriptEngine e = getScriptEngine(jsEngineName);
			e.eval(scripts);
			return (Invocable) e;
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	public static Invocable getScriptEngine(String jsEngineName, String script, List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator, ScriptConnection source, ScriptConnection target, ICacheGetter memoryCacheGetter) throws ScriptException {

		if (StringUtils.isBlank(script)) {
			return null;
		}

		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			String buildInMethod = initBuildInMethod(javaScriptFunctions, clientMongoOperator);
			String scripts = new StringBuilder(script).append(System.lineSeparator()).append(buildInMethod).toString();

			ScriptEngine e = getScriptEngine(jsEngineName);
			e.eval(scripts);
			e.put("source", source);
			e.put("target", target);
			e.put("CacheService", memoryCacheGetter);

			return (Invocable) e;
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	public static Object invokeScript(
			Invocable engine,
			String functionName,
			MessageEntity message,
			Connections sourceConn,
			Connections targetConn,
			Job job,
			Map<String, Object> context,
			Logger logger
	) throws Exception {
		ProcessContext processContext = message.getProcessContext();
		if (message.getProcessContext() == null) {
			processContext = new ProcessContext(message.getOp(), message.getTableName(), sourceConn, targetConn, job, message.getOffset());
		} else {
			processContext.setOp(message.getOp());
			processContext.setTableName(message.getTableName());
			processContext.setSource(sourceConn);
			processContext.setTarget(targetConn);
			processContext.setJob(job);
			processContext.convertOffsetToSyncType(message.getOffset());
		}

		processContext.setEventTime(message.getTimestamp() != null ? message.getTimestamp() : 0);
		processContext.setTs(message.getTimestamp() != null ? message.getTimestamp() : 0);

		Map<String, Object> contextMap = MapUtil.obj2Map(processContext);
		context.putAll(contextMap);

		if (processContext.getEvent() == null) {
			ProcessContextEvent event = new ProcessContextEvent(
					message.getOp(),
					message.getTableName(),
					processContext.getSyncType(),
					message.getTimestamp() == null ? 0L : message.getTimestamp()
			);
			processContext.setEvent(event);
		}

		processContext.getEvent().setBefore(message.getBefore());
		Map<String, Object> eventMap = MapUtil.obj2Map(processContext.getEvent());
		context.put("event", eventMap);

		((ScriptEngine) engine).put("context", context);
		((ScriptEngine) engine).put("log", logger);

		Object o;
		Map<String, Object> record = MapUtils.isNotEmpty(message.getAfter()) ? message.getAfter() : message.getBefore();
		try {
			if (engine instanceof GraalJSScriptEngine) {
				o = engine.invokeFunction(functionName, ProxyObject.fromMap(record));
			} else {
				o = engine.invokeFunction(functionName, record);
			}
		} catch (Throwable e) {
			throw new RuntimeException(String.format("Invoke function %s error: %s", functionName, e.getMessage(), e), e);
		}

		return o;
	}

	public static List<Map<String, Object>> executeMongoQuery(ScriptConnection connection, String database, String table, String fieldsStr, Object... values) {

		List<Map<String, Object>> results = new ArrayList<>();
		Document filter = new Document();
		String[] fields = fieldsStr.split(",");
		if (values == null) {
			values = new Object[0];
		}
		for (int i = 0; i < fields.length; i++) {
			if (i < values.length) {
				filter.append(fields[i], values[i]);
			} else {
				filter.append(fields[i], null);
			}
		}
		Document executeMap = new Document();
		executeMap.append("database", database);
		executeMap.append("collection", table);
		executeMap.append("filter", filter);
		results = connection.executeQuery(executeMap);

		return results;
	}

	public static void scriptSort(List<Map> list, String sortKey, int sort) {

		if (CollectionUtils.isEmpty(list)) {
			return;
		}

		Collections.sort(list, (item1, item2) -> {
			Object o1 = item1.get(sortKey);
			Object o2 = item2.get(sortKey);
			if (o1 instanceof Comparable && o2 instanceof Comparable) {
				Comparable comparable1 = (Comparable) o1;
				Comparable comparable2 = (Comparable) o2;

				if (sort > 0) {
					return comparable1.compareTo(comparable2);
				} else {
					return comparable2.compareTo(comparable1);
				}
			}

			return 0;
		});
	}

	public static String initBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator) {
		StringBuilder buildInMethod = new StringBuilder();
		buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
		buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
		buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
		buildInMethod.append("var Date = Java.type(\"java.util.Date\");\n");
		buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
		buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
		buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
		buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
		buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
		buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
		buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
		buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");
		buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
		buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
		buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
		buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
		buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
		buildInMethod.append("var sleep = function(ms){\n" +
				"var Thread = Java.type(\"java.lang.Thread\");\n" +
				"Thread.sleep(ms);\n" +
				"}\n");

		if (CollectionUtils.isNotEmpty(javaScriptFunctions)) {
			List<URL> urlList = new ArrayList<>();
			for (JavaScriptFunctions javaScriptFunction : javaScriptFunctions) {
				if (javaScriptFunction.isSystem()) {
					continue;
				}
				String jsFunction = javaScriptFunction.getJSFunction();
				if (StringUtils.isNotBlank(jsFunction)) {
					buildInMethod.append(jsFunction).append("\n");
					if (javaScriptFunction.isJar() && AppType.init().isDaas()) {
						//定义类加载器
						String fileId = javaScriptFunction.getFileId();
						final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
						if (Files.notExists(filePath)) {
							if (clientMongoOperator instanceof HttpClientMongoOperator) {
								File file = ((HttpClientMongoOperator) clientMongoOperator).downloadFile(null, "file/" + fileId, filePath.toString(), true);
								if (null == file) {
									throw new RuntimeException("not found");
								}
							} else {
								GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
								try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
									if (Files.notExists(filePath.getParent())) {
										Files.createDirectories(filePath.getParent());
									}
									Files.createFile(filePath);
									Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
								} catch (Exception e) {
									throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
								}
							}
						}
						try {
							URL url = filePath.toUri().toURL();
							urlList.add(url);
						} catch (Exception e) {
							throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
						}
					}
				}
			}
			if (CollectionUtils.isNotEmpty(urlList)) {
				logger.debug("urlClassLoader will load: {}", urlList);
				final URLClassLoader urlClassLoader = new CustomerClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
				Thread.currentThread().setContextClassLoader(urlClassLoader);
			}
		}
		if (Thread.currentThread().getContextClassLoader() == null) {
			final URLClassLoader urlClassLoader = new CustomerClassLoader(new URL[0], ScriptUtil.class.getClassLoader());
			Thread.currentThread().setContextClassLoader(urlClassLoader);
		}

		return buildInMethod.toString();
	}

	public static ScriptConnection initScriptConnection(Connections connection) throws ClassNotFoundException, InstantiationException, IllegalAccessException {

		ScriptConnection scriptConnection = null;
		Set<BeanDefinition> fileDetectorDefinition = PkgAnnoUtil.getBeanSetWithAnno(Arrays.asList("com.tapdata.processor"),
				Arrays.asList(DatabaseTypeAnnotations.class, DatabaseTypeAnnotation.class));

		for (BeanDefinition beanDefinition : fileDetectorDefinition) {
			Class<ScriptConnection> aClass = (Class<ScriptConnection>) Class.forName(beanDefinition.getBeanClassName());
			DatabaseTypeAnnotation[] annotations = aClass.getAnnotationsByType(DatabaseTypeAnnotation.class);
			if (annotations != null && annotations.length > 0) {
				for (DatabaseTypeAnnotation annotation : annotations) {
					if (connection != null && annotation.type().getType().equals(connection.getDatabase_type())) {
						scriptConnection = aClass.newInstance();
						scriptConnection.initialize(connection);
						return scriptConnection;
					}
				}
			}
		}

		return scriptConnection;
	}

	public static class CustomerClassLoader extends URLClassLoader {

		public CustomerClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, parent);
		}
	}

	public static void main(String[] args) throws ScriptException, NoSuchMethodException, JsonProcessingException {
		String script = initBuildInMethod(null, null);

//		script += "function process(record){\n" +
//			"var cDate = DateUtil.toCalendar(Date.from(record.instant));\n" +
//			"record.year = cDate.get(1)\n" +
//			"record.month = cDate.get(2) + 1;\n" +
//			"record.day = cDate.get(5);\n" +
//			"record.undefined = record.undefined.toString();" +
//			"record.keys = {};" +
//			"for(var key in record){record.keys[key] = key}\n" +
//			"return record;\n" +
//			"\n}";

		script += "function process(record){\n" +
				"var pb_schema = {\n" +
				"    \"name\": \"Unit\",\n" +
				"    \"nestedList\": [\n" +
				"        {\n" +
				"            \"name\": \"TargetsObject\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"targetsObjectData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"TargetsObjectData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"TargetsObjectData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"targetObjectNum\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"repeated\",\n" +
				"                            \"name\": \"targetObject\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"TargetObject\"\n" +
				"                        }\n" +
				"                    ],\n" +
				"                    \"nestedList\": [\n" +
				"                        {\n" +
				"                            \"name\": \"TargetObject\",\n" +
				"                            \"propertyList\": [\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"type\",\n" +
				"                                    \"number\": 1,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"riskStatus\",\n" +
				"                                    \"number\": 2,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"relativeLateralPosition\",\n" +
				"                                    \"number\": 3,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"relativeLongitudinalPosition\",\n" +
				"                                    \"number\": 4,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"relativeLateralVelocity\",\n" +
				"                                    \"number\": 5,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"relativeLongitudinalVelocity\",\n" +
				"                                    \"number\": 6,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"length\",\n" +
				"                                    \"number\": 7,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"height\",\n" +
				"                                    \"number\": 8,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                },\n" +
				"                                {\n" +
				"                                    \"label\": \"optional\",\n" +
				"                                    \"name\": \"width\",\n" +
				"                                    \"number\": 9,\n" +
				"                                    \"type\": \"string\"\n" +
				"                                }\n" +
				"                            ]\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"Position\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"positionData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"PositionData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"PositionData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"longitude\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"latitude\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"height\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"validMark\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"Decision\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"decisionData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"DecisionData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"DecisionData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"gear\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"acceleratorPedal\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"brakePedal\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"steeringAngle\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adReqGear\",\n" +
				"                            \"number\": 5,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqRelativeLateralVelocity\",\n" +
				"                            \"number\": 6,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqRelativeLongitudinalVelocity\",\n" +
				"                            \"number\": 7,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqSteeringAngle\",\n" +
				"                            \"number\": 8,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqSteeringTorque\",\n" +
				"                            \"number\": 9,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqLongitudinalMoment\",\n" +
				"                            \"number\": 10,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqFlashLampStatus\",\n" +
				"                            \"number\": 11,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"adSysReqWiperStatus\",\n" +
				"                            \"number\": 12,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"driverTakeOverAbility\",\n" +
				"                            \"number\": 13,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"VehiclePerformance\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"vehiclePerformanceData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"VehiclePerformanceData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"VehiclePerformanceData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"instantaneousVelocity\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"lateralAcceleration\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"longitudinalAcceleration\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"headingAngle\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"yawRate\",\n" +
				"                            \"number\": 5,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"rollSpeed\",\n" +
				"                            \"number\": 6,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"pitchAngularVelocity\",\n" +
				"                            \"number\": 7,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"SendingDataExternally\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"sendingDataExternallyData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"SendingDataExternallyData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"SendingDataExternallyData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"id\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"RoadInfo\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"roadInfoData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"RoadInfoData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"RoadInfoData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"trafficSigns\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"laneNumber\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"laneType\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"roadSpeedLimit\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"abnormalRoadConditions\",\n" +
				"                            \"number\": 5,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"trafficControlInfo\",\n" +
				"                            \"number\": 6,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"frontSignalSign\",\n" +
				"                            \"number\": 7,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"Environment\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"environmentData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"EnvironmentData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"EnvironmentData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"externalLightInfo\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"weatherInfo\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"externalTemperatureInfo\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"externalHumidityInfo\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"VehicleStatus\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"vehicleStatusData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"VehicleStatusData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"VehicleStatusData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"powerOnStatus\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"controlModel\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"dynamicModel\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"chargeStatus\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"gear\",\n" +
				"                            \"number\": 5,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"brakingStatus\",\n" +
				"                            \"number\": 6,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"lightSwitch\",\n" +
				"                            \"number\": 7,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"batterySoh\",\n" +
				"                            \"number\": 8,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"currentOilVolume\",\n" +
				"                            \"number\": 9,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"currentCapacity\",\n" +
				"                            \"number\": 10,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"accumulatedMileage\",\n" +
				"                            \"number\": 11,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"wiperStatus\",\n" +
				"                            \"number\": 12,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"networkShape\",\n" +
				"                            \"number\": 13,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"signalStrengthLevel\",\n" +
				"                            \"number\": 14,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"uplinkRate\",\n" +
				"                            \"number\": 15,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"downlinkRate\",\n" +
				"                            \"number\": 16,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"afs\",\n" +
				"                            \"number\": 17,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"esc\",\n" +
				"                            \"number\": 18,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"dcBusVoltage\",\n" +
				"                            \"number\": 19,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"igbtTemperature\",\n" +
				"                            \"number\": 20,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"threePhaseCurrent\",\n" +
				"                            \"number\": 21,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"coolantFlow\",\n" +
				"                            \"number\": 22,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"coolantTemperature\",\n" +
				"                            \"number\": 23,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"allChargeAndDischargeValue\",\n" +
				"                            \"number\": 24,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"thermalRunawayState\",\n" +
				"                            \"number\": 25,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"equalizingCellStatus\",\n" +
				"                            \"number\": 26,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"currentSignal\",\n" +
				"                            \"number\": 27,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"cellVoltageSignal\",\n" +
				"                            \"number\": 28,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"batteryTemperature\",\n" +
				"                            \"number\": 29,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"Personnel\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"personnelData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"PersonnelData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"PersonnelData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"seatBeltStatus\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"steeringWheelStatus\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"driverSeatStatus\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        },\n" +
				"        {\n" +
				"            \"name\": \"VehicleComponent\",\n" +
				"            \"propertyList\": [\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"density\",\n" +
				"                    \"number\": 1,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"optional\",\n" +
				"                    \"name\": \"contentLength\",\n" +
				"                    \"number\": 2,\n" +
				"                    \"type\": \"string\"\n" +
				"                },\n" +
				"                {\n" +
				"                    \"label\": \"repeated\",\n" +
				"                    \"name\": \"vehicleComponentData\",\n" +
				"                    \"number\": 3,\n" +
				"                    \"type\": \"VehicleComponentData\"\n" +
				"                }\n" +
				"            ],\n" +
				"            \"nestedList\": [\n" +
				"                {\n" +
				"                    \"name\": \"VehicleComponentData\",\n" +
				"                    \"propertyList\": [\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"airbagStatus\",\n" +
				"                            \"number\": 1,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"gnssStatus\",\n" +
				"                            \"number\": 2,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"imuStatus\",\n" +
				"                            \"number\": 3,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"drivingAutomationSystemStatus\",\n" +
				"                            \"number\": 4,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"highPrecisionMapStatus\",\n" +
				"                            \"number\": 5,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"obuStatus\",\n" +
				"                            \"number\": 6,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"cameraStatus\",\n" +
				"                            \"number\": 7,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"lidarStatus\",\n" +
				"                            \"number\": 8,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"ultrasonicRadarStatus\",\n" +
				"                            \"number\": 9,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"millimeterWaveRadarStatus\",\n" +
				"                            \"number\": 10,\n" +
				"                            \"type\": \"string\"\n" +
				"                        },\n" +
				"                        {\n" +
				"                            \"label\": \"optional\",\n" +
				"                            \"name\": \"nightVisionSystemStatus\",\n" +
				"                            \"number\": 11,\n" +
				"                            \"type\": \"string\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                }\n" +
				"            ]\n" +
				"        }\n" +
				"    ],\n" +
				"    \"propertyList\": [\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"targetsObject\",\n" +
				"            \"number\": 1,\n" +
				"            \"type\": \"TargetsObject\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"position\",\n" +
				"            \"number\": 2,\n" +
				"            \"type\": \"Position\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"decision\",\n" +
				"            \"number\": 3,\n" +
				"            \"type\": \"Decision\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"vehiclePerformance\",\n" +
				"            \"number\": 4,\n" +
				"            \"type\": \"VehiclePerformance\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"sendingDataExternally\",\n" +
				"            \"number\": 5,\n" +
				"            \"type\": \"SendingDataExternally\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"roadInfo\",\n" +
				"            \"number\": 6,\n" +
				"            \"type\": \"RoadInfo\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"environment\",\n" +
				"            \"number\": 7,\n" +
				"            \"type\": \"Environment\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"vehicleStatus\",\n" +
				"            \"number\": 8,\n" +
				"            \"type\": \"VehicleStatus\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"personnel\",\n" +
				"            \"number\": 9,\n" +
				"            \"type\": \"Personnel\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"label\": \"optional\",\n" +
				"            \"name\": \"vehicleComponent\",\n" +
				"            \"number\": 10,\n" +
				"            \"type\": \"VehicleComponent\"\n" +
				"        }\n" +
				"    ]\n" +
				"};\n" +
				"var pb_mapping = {\n" +
				"    \"Unit.targetsObject.density\":\"unit0.targetsObject.density\",\n" +
				"    \"Unit.targetsObject.contentLength\":\"unit0.targetsObject.contentLength\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.type\":\"unit0.targetsObject.targetsObjectData.type\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.riskStatus\":\"unit0.targetsObject.targetsObjectData.riskStatus\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.relativeLateralPosition\":\"unit0.targetsObject.targetsObjectData.relativeLateralPosition\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.relativeLongitudinalPosition\":\"unit0.targetsObject.targetsObjectData.relativeLongitudinalPosition\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.relativeLateralVelocity\":\"unit0.targetsObject.targetsObjectData.relativeLateralVelocity\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.relativeLongitudinalVelocity\":\"unit0.targetsObject.targetsObjectData.relativeLongitudinalVelocity\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.length\":\"unit0.targetsObject.targetsObjectData.length\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.height\":\"unit0.targetsObject.targetsObjectData.height\",\n" +
				"    \"Unit.targetsObject.targetsObjectData.width\":\"unit0.targetsObject.targetsObjectData.width\",\n" +
				"    \"Unit.position.density\":\"unit0.position.density\",\n" +
				"    \"Unit.position.contentLength\":\"unit0.position.contentLength\",\n" +
				"    \"Unit.position.positionData.longitude\":\"unit0.position.positionData.longitude\",\n" +
				"    \"Unit.position.positionData.latitude\":\"unit0.position.positionData.latitude\",\n" +
				"    \"Unit.position.positionData.validMark\":\"unit0.position.positionData.validMark\",\n" +
				"    \"Unit.decision.density\":\"unit0.decision.density\",\n" +
				"    \"Unit.decision.contentLength\":\"unit0.decision.contentLength\",\n" +
				"    \"Unit.decision.decisionData.gear\":\"unit0.decision.decisionData.gear\",\n" +
				"    \"Unit.decision.decisionData.acceleratorPedal\":\"unit0.decision.decisionData.acceleratorPedal\",\n" +
				"    \"Unit.decision.decisionData.brakePedal\":\"unit0.decision.decisionData.brakePedal\",\n" +
				"    \"Unit.decision.decisionData.steeringAngle\":\"unit0.decision.decisionData.steeringAngle\",\n" +
				"    \"Unit.decision.decisionData.adReqGear\":\"unit0.decision.decisionData.adReqGear\",\n" +
				"    \"Unit.decision.decisionData.adSysReqRelativeLateralVelocity\":\"unit0.decision.decisionData.adSysReqRelativeLateralVelocity\",\n" +
				"    \"Unit.decision.decisionData.adSysReqRelativeLongitudinalVelocity\":\"unit0.decision.decisionData.adSysReqRelativeLongitudinalVelocity\",\n" +
				"    \"Unit.decision.decisionData.adSysReqSteeringAngle\":\"unit0.decision.decisionData.adSysReqSteeringAngle\",\n" +
				"    \"Unit.decision.decisionData.adSysReqSteeringTorque\":\"unit0.decision.decisionData.adSysReqSteeringTorque\",\n" +
				"    \"Unit.decision.decisionData.adSysReqLongitudinalMoment\":\"unit0.decision.decisionData.adSysReqLongitudinalMoment\",\n" +
				"    \"Unit.decision.decisionData.adSysReqFlashLampStatus\":\"unit0.decision.decisionData.adSysReqFlashLampStatus\",\n" +
				"    \"Unit.decision.decisionData.adSysReqWiperStatus\":\"unit0.decision.decisionData.adSysReqWiperStatus\",\n" +
				"    \"Unit.decision.decisionData.driverTakeOverAbility\":\"unit0.decision.decisionData.driverTakeOverAbility\",\n" +
				"    \"Unit.vehiclePerformance.density\":\"unit0.vehiclePerformance.density\",\n" +
				"    \"Unit.vehiclePerformance.contentLength\":\"unit0.vehiclePerformance.contentLength\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.instantaneousVelocity\":\"unit0.vehiclePerformance.vehiclePerformanceData.instantaneousVelocity\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.lateralAcceleration\":\"unit0.vehiclePerformance.vehiclePerformanceData.lateralAcceleration\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.longitudinalAcceleration\":\"unit0.vehiclePerformance.vehiclePerformanceData.longitudinalAcceleration\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.headingAngle\":\"unit0.vehiclePerformance.vehiclePerformanceData.headingAngle\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.yawRate\":\"unit0.vehiclePerformance.vehiclePerformanceData.yawRate\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.rollSpeed\":\"unit0.vehiclePerformance.vehiclePerformanceData.rollSpeed\",\n" +
				"    \"Unit.vehiclePerformance.vehiclePerformanceData.pitchAngularVelocity\":\"unit0.vehiclePerformance.vehiclePerformanceData.pitchAngularVelocity\",\n" +
				"    \"Unit.sendingDataExternally.density\":\"unit0.sendingDataExternally.density\",\n" +
				"    \"Unit.sendingDataExternally.contentLength\":\"unit0.sendingDataExternally.contentLength\",\n" +
				"    \"Unit.sendingDataExternally.sendingDataExternallyData.id\":\"unit0.sendingDataExternally.sendingDataExternallyData.id\",\n" +
				"    \"Unit.roadInfo.density\":\"unit0.roadInfo.density\",\n" +
				"    \"Unit.roadInfo.contentLength\":\"unit0.roadInfo.contentLength\",\n" +
				"    \"Unit.roadInfo.roadInfoData.trafficSigns\":\"unit0.roadInfo.roadInfoData.trafficSigns\",\n" +
				"    \"Unit.roadInfo.roadInfoData.laneNumber\":\"unit0.roadInfo.roadInfoData.laneNumber\",\n" +
				"    \"Unit.roadInfo.roadInfoData.laneType\":\"unit0.roadInfo.roadInfoData.laneType\",\n" +
				"    \"Unit.roadInfo.roadInfoData.roadSpeedLimit\":\"unit0.roadInfo.roadInfoData.roadSpeedLimit\",\n" +
				"    \"Unit.roadInfo.roadInfoData.abnormalRoadConditions\":\"unit0.roadInfo.roadInfoData.abnormalRoadConditions\",\n" +
				"    \"Unit.roadInfo.roadInfoData.trafficControlInfo\":\"unit0.roadInfo.roadInfoData.trafficControlInfo\",\n" +
				"    \"Unit.roadInfo.roadInfoData.frontSignalSign\":\"unit0.roadInfo.roadInfoData.frontSignalSign\",\n" +
				"    \"Unit.environment.density\":\"unit0.environment.density\",\n" +
				"    \"Unit.environment.contentLength\":\"unit0.environment.contentLength\",\n" +
				"    \"Unit.environment.environmentData.externalLightInfo\":\"unit0.environment.environmentData.externalLightInfo\",\n" +
				"    \"Unit.environment.environmentData.weatherInfo\":\"unit0.environment.environmentData.weatherInfo\",\n" +
				"    \"Unit.environment.environmentData.externalTemperatureInfo\":\"unit0.environment.environmentData.externalTemperatureInfo\",\n" +
				"    \"Unit.environment.environmentData.externalHumidityInfo\":\"unit0.environment.environmentData.externalHumidityInfo\",\n" +
				"    \"Unit.vehicleStatus.density\":\"unit0.vehicleStatus.density\",\n" +
				"    \"Unit.vehicleStatus.contentLength\":\"unit0.vehicleStatus.contentLength\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.powerOnStatus\":\"unit0.vehicleStatus.vehicleStatusData.powerOnStatus\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.controlModel\":\"unit0.vehicleStatus.vehicleStatusData.controlModel\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.dynamicModel\":\"unit0.vehicleStatus.vehicleStatusData.dynamicModel\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.chargeStatus\":\"unit0.vehicleStatus.vehicleStatusData.chargeStatus\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.gear\":\"unit0.vehicleStatus.vehicleStatusData.gear\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.brakingStatus\":\"unit0.vehicleStatus.vehicleStatusData.brakingStatus\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.lightSwitch\":\"unit0.vehicleStatus.vehicleStatusData.lightSwitch\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.batterySoh\":\"unit0.vehicleStatus.vehicleStatusData.batterySoh\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.currentOilVolume\":\"unit0.vehicleStatus.vehicleStatusData.currentOilVolume\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.currentCapacity\":\"unit0.vehicleStatus.vehicleStatusData.currentCapacity\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.accumulatedMileage\":\"unit0.vehicleStatus.vehicleStatusData.accumulatedMileage\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.wiperStatus\":\"unit0.vehicleStatus.vehicleStatusData.wiperStatus\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.networkShape\":\"unit0.vehicleStatus.vehicleStatusData.networkShape\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.signalStrengthLevel\":\"unit0.vehicleStatus.vehicleStatusData.signalStrengthLevel\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.uplinkRate\":\"unit0.vehicleStatus.vehicleStatusData.uplinkRate\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.downlinkRate\":\"unit0.vehicleStatus.vehicleStatusData.downlinkRate\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.afs\":\"unit0.vehicleStatus.vehicleStatusData.afs\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.esc\":\"unit0.vehicleStatus.vehicleStatusData.esc\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.dcBusVoltage\":\"unit0.vehicleStatus.vehicleStatusData.dcBusVoltage\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.igbtTemperature\":\"unit0.vehicleStatus.vehicleStatusData.igbtTemperature\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.threePhaseCurrent\":\"unit0.vehicleStatus.vehicleStatusData.threePhaseCurrent\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.coolantFlow\":\"unit0.vehicleStatus.vehicleStatusData.coolantFlow\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.coolantTemperature\":\"unit0.vehicleStatus.vehicleStatusData.coolantTemperature\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.allChargeAndDischargeValue\":\"unit0.vehicleStatus.vehicleStatusData.allChargeAndDischargeValue\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.thermalRunawayState\":\"unit0.vehicleStatus.vehicleStatusData.thermalRunawayState\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.equalizingCellStatus\":\"unit0.vehicleStatus.vehicleStatusData.equalizingCellStatus\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.currentSignal\":\"unit0.vehicleStatus.vehicleStatusData.currentSignal\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.cellVoltageSignal\":\"unit0.vehicleStatus.vehicleStatusData.cellVoltageSignal\",\n" +
				"    \"Unit.vehicleStatus.vehicleStatusData.batteryTemperature\":\"unit0.vehicleStatus.vehicleStatusData.batteryTemperature\",\n" +
				"    \"Unit.personnel.density\":\"unit0.personnel.density\",\n" +
				"    \"Unit.personnel.contentLength\":\"unit0.personnel.contentLength\",\n" +
				"    \"Unit.personnel.personnelData.seatBeltStatus\":\"unit0.personnel.personnelData.seatBeltStatus\",\n" +
				"    \"Unit.personnel.personnelData.steeringWheelStatus\":\"unit0.personnel.personnelData.steeringWheelStatus\",\n" +
				"    \"Unit.personnel.personnelData.driverSeatStatus\":\"unit0.personnel.personnelData.driverSeatStatus\",\n" +
				"    \"Unit.vehicleComponent.density\":\"unit0.vehicleComponent.density\",\n" +
				"    \"Unit.vehicleComponent.contentLength\":\"unit0.vehicleComponent.contentLength\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.airbagStatus\":\"unit0.vehicleComponent.vehicleComponentData.airbagStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.gnssStatus\":\"unit0.vehicleComponent.vehicleComponentData.gnssStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.imuStatus\":\"unit0.vehicleComponent.vehicleComponentData.imuStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.drivingAutomationSystemStatus\":\"unit0.vehicleComponent.vehicleComponentData.drivingAutomationSystemStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.highPrecisionMapStatus\":\"unit0.vehicleComponent.vehicleComponentData.highPrecisionMapStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.obuStatus\":\"unit0.vehicleComponent.vehicleComponentData.obuStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.cameraStatus\":\"unit0.vehicleComponent.vehicleComponentData.cameraStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.lidarStatus\":\"unit0.vehicleComponent.vehicleComponentData.lidarStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.ultrasonicRadarStatus\":\"unit0.vehicleComponent.vehicleComponentData.ultrasonicRadarStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.millimeterWaveRadarStatus\":\"unit0.vehicleComponent.vehicleComponentData.millimeterWaveRadarStatus\",\n" +
				"    \"Unit.vehicleComponent.vehicleComponentData.nightVisionSystemStatus\":\"unit0.vehicleComponent.vehicleComponentData.nightVisionSystemStatus\"                                                        \n" +
				"};\n" +
				"\n" +
				"record.unit = tcp.pbConvert(record.unit0,pb_mapping,pb_schema);\n" +
				"    return record;\n" +
				"}";

//    String s = JSONUtil.obj2Json(new HashMap() {{
//      put("a", 1);
//      put("instant", Instant.now());
//      put("undefined", new BsonUndefined());
//    }});
//
//    System.out.println(s);

		Invocable scriptEngine = getScriptEngine(JSEngineEnum.NASHORN.getEngineName(), script, null, null, null, null, null);
		Object a = scriptEngine.invokeFunction(FUNCTION_NAME, new HashMap() {{
			put("a", 1);
			put("instant", Instant.now());
			put("undefined", new BsonUndefined());
		}});

		System.out.println(a);
	}
}
