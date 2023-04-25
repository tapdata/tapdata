package io.tapdata.websocket.handler;

import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.tapdata.constant.DateUtil;
import com.tapdata.constant.HanLPUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MD5Util;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.NetworkUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.util.CustomMongodb;
import com.tapdata.processor.util.CustomRest;
import com.tapdata.processor.util.CustomTcp;
import com.tapdata.processor.util.Util;
import io.tapdata.common.SettingService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2020-12-22 15:39
 **/
@EventHandlerAnnotation(type = "loadJar")
public class LoadJarLibEventHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(LoadJarLibEventHandler.class);

	private ClientMongoOperator clientMongoOperator;
	private SettingService settingService;

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	/**
	 * @param clientMongoOperator 查询管理端数据
	 * @param settingService      系统配置常量
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {
		WebSocketEventResult result;
		LoadJarLibRequest req = JSONUtil.map2POJO(event, LoadJarLibRequest.class);
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

		try {
			String fileId = req.getFileId();
			String packageName = req.getPackageName();
			if (StringUtils.isEmpty(fileId) || StringUtils.isEmpty(packageName)) {
				return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_JAR_LIB_RESULT, "illegal argument");
			}

			//定义类加载器
			final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
			if (Files.notExists(filePath)) {
				GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
				try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(fileId))) {
					if (Files.notExists(filePath.getParent())) {
						Files.createDirectories(filePath.getParent());
					}
					Files.createFile(filePath);
					Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
				}
			}
			URL url = filePath.toUri().toURL();
			URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
			Thread.currentThread().setContextClassLoader(classLoader);

			Set<Class<?>> classSet = ClassUtil.scanPackage(packageName);
			List<LoadJarLibResponse> resList = getMethodList(classSet);

			if (CollectionUtils.isEmpty(resList)) {
				throw new IllegalArgumentException("Can't find a suitable result from the jar");
			}
			result = WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.LOAD_JAR_LIB_RESULT, resList);
		} catch (Exception e) {
			logger.error("load jar class error", e);
			result = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_JAR_LIB_RESULT, e.getMessage());
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
		return result;
	}

	private List<LoadJarLibResponse> getMethodList(Set<Class<?>> classSet) {
		List<LoadJarLibResponse> resList = new ArrayList<>();
		for (Class<?> aClass : classSet) {
			List<Method> methodList = ClassUtil.getPublicMethods(aClass, ClassUtil::isStatic);
			resList.addAll(methodList.stream()
					.map(method -> new LoadJarLibResponse(aClass.getName(), method.getName(), loadParameters(method.getParameters())))
					.collect(Collectors.toList()));
		}
		return resList;
	}

	private List<Parameter> loadParameters(java.lang.reflect.Parameter[] parameters) {
		if (parameters == null || parameters.length == 0) {
			return null;
		}
		return Arrays.stream(parameters)
				.map(parameter -> new Parameter(getType(parameter.getType()),
						parameter.getName(),
						(Integer) ReflectUtil.getFieldValue(parameter, "index")))
				.collect(Collectors.toList());
	}

	private String getType(Class<?> aClass) {
		return aClass.getSimpleName();
	}


	public static class LoadJarLibRequest {
		private String fileId;
		private String packageName;

		public String getFileId() {
			return fileId;
		}

		public void setFileId(String fileId) {
			this.fileId = fileId;
		}

		public String getPackageName() {
			return packageName;
		}

		public void setPackageName(String packageName) {
			this.packageName = packageName;
		}
	}

	public static class LoadJarLibResponse {
		private String functionName;
		private String className;
		private String methodName;
		private List<Parameter> parameters;

		public LoadJarLibResponse(String className, String methodName, List<Parameter> parameters) {
			this.className = className;
			this.methodName = methodName;
			this.parameters = parameters;
			this.functionName = className + "." + methodName;
		}

		public String getFunctionName() {
			return functionName;
		}

		public void setFunctionName(String functionName) {
			this.functionName = functionName;
		}

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}

		public List<Parameter> getParameters() {
			return parameters;
		}

		public void setParameters(List<Parameter> parameters) {
			this.parameters = parameters;
		}

		@Override
		public String toString() {
			return "LoadJarLibResponse{" +
					"functionName='" + functionName + '\'' +
					", className='" + className + '\'' +
					", methodName='" + methodName + '\'' +
					", parameters=" + parameters +
					'}';
		}
	}

	public static class Parameter {
		private String type;
		private String name;
		private int index;

		public Parameter(String type, String name, int index) {
			this.type = type;
			this.name = name;
			this.index = index;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		@Override
		public String toString() {
			return "Parameter{" +
					"type='" + type + '\'' +
					", name='" + name + '\'' +
					", index=" + index +
					'}';
		}
	}

	private List<LoadJarLibResponse> getMethodListByClassName(Map<String, Class<?>> classMap) {
		List<LoadJarLibResponse> resList = new ArrayList<>();
		for (Map.Entry<String, Class<?>> entry : classMap.entrySet()) {
			Class<?> aClass = entry.getValue();
			List<Method> methodList = ClassUtil.getPublicMethods(aClass, ClassUtil::isStatic);
			resList.addAll(methodList.stream()
					.map(method -> new LoadJarLibResponse(entry.getKey(), method.getName(), loadParameters(method.getParameters())))
					.collect(Collectors.toList()));
		}
		return resList;
	}

	public static void main(String[] args) {
		LoadJarLibEventHandler loadJarLibEventHandler = new LoadJarLibEventHandler();
		Map<String, Class<?>> classMap = new HashMap<>();
		classMap.put("DateUtil", DateUtil.class);
		classMap.put("UUIDGenerator", UUIDGenerator.class);
		classMap.put("idGen", HashMap.class);
		classMap.put("ArrayList", ArrayList.class);
		classMap.put("Date", Date.class);
//    classMap.put("uuid", ArrayList.class);
		classMap.put("JSONUtil", JSONUtil.class);
		classMap.put("HanLPUtil", HanLPUtil.class);
//    classMap.put("split_chinese", ArrayList.class);
		classMap.put("rest", CustomRest.class);
		classMap.put("tcp", CustomTcp.class);
		classMap.put("util", Util.class);
		classMap.put("mongo", CustomMongodb.class);
		classMap.put("MD5Util", MD5Util.class);
//    classMap.put("MD5", ArrayList.class);
		classMap.put("Collections", Collections.class);
		classMap.put("networkUtil", NetworkUtil.class);
		classMap.put("MapUtils", MapUtil.class);
//    classMap.put("sleep", ArrayList.class);
		List<LoadJarLibResponse> methodList = loadJarLibEventHandler.getMethodListByClassName(classMap);

		System.out.println(JSON.toJSONString(methodList));

	}
}
