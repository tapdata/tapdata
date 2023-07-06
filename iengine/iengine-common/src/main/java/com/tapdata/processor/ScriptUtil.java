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
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.js.connector.base.JsUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.io.ClassPathResource;

import javax.script.*;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jackin
 */
public class ScriptUtil {

	private static final Logger logger = LogManager.getLogger(ScriptUtil.class);

	public static final String FUNCTION_NAME = "process";

	public static final String SCRIPT_FUNCTION_NAME = "validate";

	public static ScriptEngine getScriptEngine(String jsEngineName) {
		return getScriptEngine(jsEngineName,
				new LoggingOutputStream(new Log4jScriptLogger(logger), Level.INFO),
				new LoggingOutputStream(new Log4jScriptLogger(logger), Level.ERROR));
	}

	/**
	 * 获取js引擎
	 *
	 * @param jsEngineName
	 * @return
	 */
	private static ScriptEngine getScriptEngine(String jsEngineName, OutputStream out, OutputStream err) {
		JSEngineEnum jsEngineEnum = JSEngineEnum.getByEngineName(jsEngineName);
		ScriptEngine scriptEngine;
		if (jsEngineEnum == JSEngineEnum.GRAALVM_JS) {
			scriptEngine = GraalJSScriptEngine
					.create(Engine.newBuilder()
									.allowExperimentalOptions(true)
									.option("engine.WarnInterpreterOnly", "false")
									.out(out)
									.err(err)
									.build(),
							Context.newBuilder("js")
									.allowAllAccess(true)
									.allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
											.targetTypeMapping(Value.class, Object.class,
													v -> v.hasArrayElements() && v.hasMembers(), v -> v.as(List.class)).build())
									.out(out)
									.err(err)
					);
			SimpleScriptContext scriptContext = new SimpleScriptContext();
			scriptContext.setWriter(new OutputStreamWriter(out));
			scriptContext.setErrorWriter(new OutputStreamWriter(err));
			scriptEngine.setContext(scriptContext);
		} else {
			scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.getEngineName());
		}
		return scriptEngine;
	}

	public static Invocable getScriptEngine(String script,
											List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator,
											ICacheGetter memoryCacheGetter,
											Log logger) throws ScriptException {
		return getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName(), script, javaScriptFunctions, clientMongoOperator,
				null, null, memoryCacheGetter, logger);
	}

	public static Invocable getScriptEngine(String jsEngineName,
											String script,
											List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator,
											ScriptConnection source,
											ScriptConnection target,
											ICacheGetter memoryCacheGetter,
											Logger logger) throws ScriptException {
		return getScriptEngine(jsEngineName, script, javaScriptFunctions, clientMongoOperator, source, target, memoryCacheGetter, new Log4jScriptLogger(logger));
	}

	public static Invocable getScriptEngine(String jsEngineName,
											String script,
											List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator,
											ScriptConnection source,
											ScriptConnection target,
											ICacheGetter memoryCacheGetter,
											Log logger) throws ScriptException {
		return getScriptEngine(jsEngineName, script, javaScriptFunctions, clientMongoOperator, source, target, memoryCacheGetter, logger, false);
	}

	public static Invocable getScriptEngine(String jsEngineName,
											String script,
											List<JavaScriptFunctions> javaScriptFunctions,
											ClientMongoOperator clientMongoOperator,
											ScriptConnection source,
											ScriptConnection target,
											ICacheGetter memoryCacheGetter,
											Log logger,
											boolean standard) throws ScriptException {

		if (StringUtils.isBlank(script)) {
			script = "function process(record){\n" +
					"\treturn record;\n" +
					"}";
		}

		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {

			ScriptEngine e = getScriptEngine(jsEngineName,
					new LoggingOutputStream(logger, Level.INFO),
					new LoggingOutputStream(logger, Level.ERROR));
			final ClassLoader[] externalClassLoader = new ClassLoader[1];
			String buildInMethod = initBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalClassLoader[0] = urlClassLoader, standard);
			if (externalClassLoader[0] != null) {
				Thread.currentThread().setContextClassLoader(externalClassLoader[0]);
			}
			if (Thread.currentThread().getContextClassLoader() == null) {
				Thread.currentThread().setContextClassLoader(ScriptUtil.class.getClassLoader());
			}
			String scripts = script + System.lineSeparator() + buildInMethod;

			try {
				e.eval(scripts);
			} catch (Throwable ex) {
				throw new RuntimeException(String.format("script eval error: %s, %s, %s, %s", jsEngineName, e, scripts, contextClassLoader), ex);
			}
			if (source != null) {
				e.put("source", source);
			}
			if (target != null) {
				e.put("target", target);
			}
			if (memoryCacheGetter != null) {
				e.put("CacheService", memoryCacheGetter);
			}

			if (logger != null) {
				e.put("log", logger);
			}

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
		if (engine == null) {
			logger.error("script engine is null, {}", Arrays.asList(Thread.currentThread().getStackTrace()));
		}

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
		return initBuildInMethod(javaScriptFunctions, clientMongoOperator, null);
	}

	public static String initBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator, Consumer<URLClassLoader> consumer) {
		return initBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, false);
	}

	public static String initBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator, Consumer<URLClassLoader> consumer, boolean standard) {
		StringBuilder buildInMethod = new StringBuilder();
		buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
		buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
		buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
		buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
		buildInMethod.append("var Date = Java.type(\"java.util.Date\");\n");
		buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
		buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
		buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
		buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
		buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
		buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
		buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
		buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
		buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
		buildInMethod.append("var sleep = function(ms){\n" +
				"var Thread = Java.type(\"java.lang.Thread\");\n" +
				"Thread.sleep(ms);\n" +
				"}\n");

		if (standard) {
			return buildInMethod.toString();
		}
		buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
		buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
		buildInMethod.append("var httpUtil = Java.type(\"cn.hutool.http.HttpUtil\");\n");
		buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
		buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");

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
				final URLClassLoader urlClassLoader = new CustomerClassLoader(urlList.toArray(new URL[0]), ScriptUtil.class.getClassLoader());
				if (consumer != null) {
					consumer.accept(urlClassLoader);
				}
			}
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


	//标准化JS节点相关=====================================================================================================
	public static Invocable getScriptStandardizationEngine(
			String jsEngineName,
			String script,
			List<JavaScriptFunctions> javaScriptFunctions,
			ClientMongoOperator clientMongoOperator,
			ScriptConnection source,
			ScriptConnection target,
			ICacheGetter memoryCacheGetter,
			Log logger,
			boolean standard ) {
		if (StringUtils.isBlank(script)) {
			script = "function process(record){\n\treturn record;\n}";
		}
		final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
		final ClassLoader[] externalClassLoader = new ClassLoader[1];
		String buildInMethod = initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalClassLoader[0] = urlClassLoader, standard);
		ScriptEngine e = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(jsEngineName).classLoader(externalClassLoader[0]));
		String scripts = script + System.lineSeparator() + buildInMethod;

		e.put("tapUtil", new JsUtil());
		e.put("tapLog", logger);
		try {
			e.eval("tapLog.info('Init standardized JS engine...');");
		}catch (Exception es){
			throw new RuntimeException(String.format("Can not init standardized JS engine, %s", es.getMessage()), es);
		}
		evalImportSources(e,
				"js/csvUtils.js",
				"js/arrayUtils.js",
				"js/dateUtils.js",
				"js/exceptionUtils.js",
				"js/stringUtils.js",
				"js/mapUtils.js",
				"js/log.js");
		try {
			e.eval(scripts);
		} catch (Throwable ex) {
			throw new CoreException(String.format("Incorrect JS code, syntax error found: %s, please check your javascript code", ex.getMessage()));
		}
		Optional.ofNullable(source).ifPresent(s -> e.put("source", s));
		Optional.ofNullable(target).ifPresent(s -> e.put("target", s));
		Optional.ofNullable(memoryCacheGetter).ifPresent(s -> e.put("CacheService", s));
		Optional.ofNullable(logger).ifPresent(s -> e.put("log", s));
		return (Invocable) e;
	}

	public static String initStandardizationBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator, Consumer<URLClassLoader> consumer, boolean standard) {
		StringBuilder buildInMethod = new StringBuilder();

		//Expired, will be ignored in the near future
		buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
		buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
		buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
		buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
		buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
		buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
		buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
		buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
		buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
		buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
		buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
		buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
		buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");


		buildInMethod.append("var sleep = function(ms){\n" +
				"var Thread = Java.type(\"java.lang.Thread\");\n" +
				"Thread.sleep(ms);\n" +
				"}\n");
		if (standard) {
			return buildInMethod.toString();
		}
		buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
		buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
		buildInMethod.append("var httpUtil = Java.type(\"cn.hutool.http.HttpUtil\");\n");
		buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
		buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");

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
				final URLClassLoader urlClassLoader = new URLClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
				if (consumer != null) {
					consumer.accept(urlClassLoader);
				}
			}
		}
		return buildInMethod.toString();
	}



	//py节点相关==========================================================================================================
	/**
	 * context : {
	 *     "event" : {},
	 *     "before" : {},
	 *     "info" : {},
	 *     "global" : {}
	 * }
	 * */
	public static final String DEFAULT_PY_SCRIPT_START = "import json, random, time, datetime, uuid, types;\n" + //", yaml"
			"import urllib, urllib2;\n" + //", requests"
			"import math, hashlib, base64;\n" +
			"def process(record, context):\n";
	public static final String DEFAULT_PY_SCRIPT = DEFAULT_PY_SCRIPT_START + "\treturn record;\n";

	public static Invocable getPyEngine(String script, ICacheGetter memoryCacheGetter, Log logger) throws ScriptException {
		return getPyEngine("python", script, null, null, null, null, memoryCacheGetter, logger);
	}
	public static Invocable getPyEngine(
			String engineName,
			String script,
			List<JavaScriptFunctions> functions,
			ClientMongoOperator clientMongoOperator,
			ScriptConnection source,
			ScriptConnection target,
			ICacheGetter memoryCacheGetter,
			Log logger) {
		if (StringUtils.isBlank(script)) {
			script = DEFAULT_PY_SCRIPT;
		}
		final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
		final ClassLoader[] externalClassLoader = new ClassLoader[1];
		String buildInMethod = "";
		String globalScript = initPythonBuildInMethod(
				functions,
				clientMongoOperator,
				urlClassLoader -> externalClassLoader[0] = urlClassLoader
		);
		ScriptEngine e = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions().engineName(engineName).classLoader(externalClassLoader[0]));

		String scripts = buildInMethod + System.lineSeparator() + handlePyScript(script);

		e.put("tapUtil", new JsUtil());
		e.put("tapLog", logger);
		try {
			e.eval(globalScript);
			e.eval("tapLog.info('Init python engine...');");
		}catch (Exception es){
			throw new RuntimeException(String.format("Can not init python engine, %s", es.getMessage()), es);
		}
		evalImportSources(e, "");

		try {
			e.eval(scripts);
		} catch (Throwable ex) {
			throw new CoreException(String.format("Incorrect python code, syntax error found: %s, please check your python code", ex.getMessage()));
		}
		Optional.ofNullable(source).ifPresent(s -> e.put("source", s));
		Optional.ofNullable(target).ifPresent(s -> e.put("target", s));
		Optional.ofNullable(memoryCacheGetter).ifPresent(s -> e.put("CacheService", s));
		Optional.ofNullable(logger).ifPresent(s -> e.put("log", s));
		return (Invocable) e;
	}

	public static String initPythonBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator, Consumer<URLClassLoader> consumer) {
		//Expired, will be ignored in the near future
		//buildInMethod.append("global DateUtil = Java.type(\"com.tapdata.constant.DateUtil\")\n");
		//buildInMethod.append("global UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\")\n");
		//buildInMethod.append("global idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\")\n");
		//buildInMethod.append("global HashMap = Java.type(\"java.util.HashMap\")\n");
		//buildInMethod.append("global LinkedHashMap = Java.type(\"java.util.LinkedHashMap\")\n");
		//buildInMethod.append("global ArrayList = Java.type(\"java.util.ArrayList\")\n");
		//buildInMethod.append("global uuid = UUIDGenerator.uuid\n");
		//buildInMethod.append("global JSONUtil = Java.type('com.tapdata.constant.JSONUtil')\n");
		//buildInMethod.append("global HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\")\n");
		//buildInMethod.append("global split_chinese = HanLPUtil.hanLPParticiple\n");
		//buildInMethod.append("global util = Java.type(\"com.tapdata.processor.util.Util\")\n");
		//buildInMethod.append("global MD5Util = Java.type(\"com.tapdata.constant.MD5Util\")\n");
		//buildInMethod.append("def MD5(str):\n\treturn MD5Util.crypt(str, true)\n");
		//buildInMethod.append("global Collections = Java.type(\"java.util.Collections\")\n");
		//buildInMethod.append("global MapUtils = Java.type(\"com.tapdata.constant.MapUtil\")\n");
		//buildInMethod.append("def sleep(ms):\n\tJava.type(\"java.lang.Thread\").sleep(ms)\n\n");
		//buildInMethod.append("global networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\")\n");
		//buildInMethod.append("global rest = Java.type(\"com.tapdata.processor.util.CustomRest\")\n");
		//buildInMethod.append("global httpUtil = Java.type(\"cn.hutool.http.HttpUtil\")\n");
		//buildInMethod.append("global tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\")\n");
		//buildInMethod.append("global mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\")\n");
		//@todo initPythonBuildInMethod and add python function from mongo db
		//if (CollectionUtils.isNotEmpty(javaScriptFunctions)) {
		//	List<URL> urlList = new ArrayList<>();
		//	for (JavaScriptFunctions javaScriptFunction : javaScriptFunctions) {
		//		if (javaScriptFunction.isSystem()) {
		//			continue;
		//		}
		//		String jsFunction = javaScriptFunction.getJSFunction();
		//		if (StringUtils.isNotBlank(jsFunction)) {
		//			buildInMethod.append(jsFunction).append("\n");
		//			if (javaScriptFunction.isJar() && AppType.init().isDaas()) {
		//				//定义类加载器
		//				String fileId = javaScriptFunction.getFileId();
		//				final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
		//				if (Files.notExists(filePath)) {
		//					if (clientMongoOperator instanceof HttpClientMongoOperator) {
		//						File file = ((HttpClientMongoOperator) clientMongoOperator).downloadFile(null, "file/" + fileId, filePath.toString(), true);
		//						if (null == file) {
		//							throw new RuntimeException("not found");
		//						}
		//					} else {
		//						GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
		//						try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
		//							if (Files.notExists(filePath.getParent())) {
		//								Files.createDirectories(filePath.getParent());
		//							}
		//							Files.createFile(filePath);
		//							Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
		//						} catch (Exception e) {
		//							throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
		//						}
		//					}
		//				}
		//				try {
		//					URL url = filePath.toUri().toURL();
		//					urlList.add(url);
		//				} catch (Exception e) {
		//					throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
		//				}
		//			}
		//		}
		//	}
		//	if (CollectionUtils.isNotEmpty(urlList)) {
		//		logger.debug("urlClassLoader will load: {}", urlList);
		//		final URLClassLoader urlClassLoader = new URLClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
		//		if (consumer != null) {
		//			consumer.accept(urlClassLoader);
		//		}
		//	}
		//}
		return  "import com.tapdata.constant.DateUtil as DateUtil\n" +
				"import com.tapdata.constant.UUIDGenerator as UUIDGenerator\n" +
				"import com.tapdata.constant.UUIDGenerator as idGen\n" +
				"import java.util.HashMap as HashMap\n" +
				"import java.util.LinkedHashMap as LinkedHashMap\n" +
				"import java.util.ArrayList as ArrayList\n" +
				"import com.tapdata.constant.JSONUtil as JSONUtil\n" +
				"import com.tapdata.constant.HanLPUtil as HanLPUtil\n" +
				"import com.tapdata.processor.util.Util as util\n" +
				"import com.tapdata.constant.MD5Util as MD5Util\n" +
				"import java.util.Collections as Collections\n" +
				"import com.tapdata.constant.MapUtil as MapUtils\n" +
				"import com.tapdata.constant.NetworkUtil as networkUtil\n" +
				"import com.tapdata.processor.util.CustomRest as rest\n" +
				"import cn.hutool.http.HttpUtil as httpUtil\n" +
				"import com.tapdata.processor.util.CustomTcp as tcp\n" +
				"import com.tapdata.processor.util.CustomMongodb as mongo\n" +

				"uuid = UUIDGenerator.uuid\n" +
				"split_chinese = HanLPUtil.hanLPParticiple\n" +
				"def MD5(str):\n" +
				"\treturn MD5Util.crypt(str, True)\n";
	}

	public static String handlePyScript(String script){
		if (null == script || "".equals(script.trim())) return DEFAULT_PY_SCRIPT;
		Pattern p = Pattern.compile("(.*)(def\\s+)(.*)(\\()(.*)(\\))(\\s*)(:)(\\s*)");
		if (!p.matcher(script).find()) {
			throw new CoreException("Python process function is non compliant, error script: " + script);
		}
		String[] split = p.split(script);
		if (split.length < 1){
			throw new CoreException("Python process function is non compliant, error script: " + script);
		}

		String scriptItem = split[split.length-1];//script.substring(script.indexOf("def process(record, context):") + 29);
		char[] chars = scriptItem.toCharArray();
		StringBuilder builder = new StringBuilder();
//		if (split.length > 1){
//			for (int index = 0; index < split.length - 1; index++) {
//				builder.append(split[index]);
//			}
//		}
		for (char aChar : chars) {
			builder.append(aChar);
			if (aChar == '\n') {
				builder.append("\t");
			}
		}
		String toString = builder.toString();
		return script.replace(scriptItem, toString.startsWith("\n") ? toString : "\n\t" + toString);
	}








	private static void evalSource(ScriptEngine engine, String fileClassPath){
		try {
			ClassPathResource classPathResource = new ClassPathResource(fileClassPath);
			engine.eval(IOUtils.toString(classPathResource.getInputStream(), StandardCharsets.UTF_8));
		}catch (Throwable ex){
			throw new RuntimeException(String.format("script eval js util error: %s, %s", fileClassPath, ex.getMessage()), ex);
		}
	}

	private static void evalImportSources(ScriptEngine engine, String ... fileClassPaths){
		if (null == fileClassPaths || fileClassPaths.length < 1) return;
		for (String fileClassPath : fileClassPaths) {
			if (null == fileClassPath || "".equals(fileClassPath.trim())) continue;
			evalSource(engine, fileClassPath);
		}
	}




	public static void main(String[] args) throws ScriptException, NoSuchMethodException, JsonProcessingException {
//		String script = initBuildInMethod(null, null);
		Pattern p = Pattern.compile("(def\\s+)(.*)(\\()(.*)(\\))(\\s*)(:)");
		Matcher m = p.matcher("def declare(tapTable):\n\treturn tapTable");
		System.out.println(m.find());
		System.out.println(p.matcher("dsef process(record, context):\n\treturn record").find());
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

//		script += "function process(record){\n" +
//				"    return record;\n" +
//				"}";

//    String s = JSONUtil.obj2Json(new HashMap() {{
//      put("a", 1);
//      put("instant", Instant.now());
//      put("undefined", new BsonUndefined());
//    }});
//
//    System.out.println(s);

//		Invocable scriptEngine = getScriptEngine(JSEngineEnum.NASHORN.getEngineName(), script, null, null, null, null, null, logger);
//		Object a = scriptEngine.invokeFunction(FUNCTION_NAME, new HashMap() {{
//			put("a", 1);
//			put("instant", Instant.now());
//			put("undefined", new BsonUndefined());
//		}});
//
//		System.out.println(a);
	}
}
