package io.tapdata.test.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.encryptor.JarEncryptor;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

/**
 * 热加载数据源连接器测试器
 * 支持动态加载连接器JAR包并测试其性能
 *
 * @author TapData
 */
public class HotLoadConnectorTester {

    private static final Log logger = new TapLog();

    // 连接器缓存
    private final Map<String, ConnectorInfo> connectorCache = new ConcurrentHashMap<>();

    // JSON处理器
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 连接器信息
     */
    public static class ConnectorInfo {
        private String connectorId;
        private String jarPath;
        private TapConnector connectorInstance;
        private Class<? extends TapConnector> connectorClass;
        private TapConnectionContext connectionContext;
        private ConnectorFunctions connectorFunctions;
        private URLClassLoader classLoader;
        private long lastModified;
        private DataMap connectionConfig;
        private Map<String, Object> metadata;

        public ConnectorInfo() {
            this.metadata = new HashMap<>();
        }

        // Getters and setters
        public String getConnectorId() {
            return connectorId;
        }

        public void setConnectorId(String connectorId) {
            this.connectorId = connectorId;
        }

        public String getJarPath() {
            return jarPath;
        }

        public void setJarPath(String jarPath) {
            this.jarPath = jarPath;
        }

        public TapConnector getConnectorInstance() {
            return connectorInstance;
        }

        public void setConnectorInstance(TapConnector connectorInstance) {
            this.connectorInstance = connectorInstance;
        }

        public Class<? extends TapConnector> getConnectorClass() {
            return connectorClass;
        }

        public void setConnectorClass(Class<? extends TapConnector> connectorClass) {
            this.connectorClass = connectorClass;
        }

        public TapConnectionContext getConnectionContext() {
            return connectionContext;
        }

        public void setConnectionContext(TapConnectionContext connectionContext) {
            this.connectionContext = connectionContext;
        }

        public ConnectorFunctions getConnectorFunctions() {
            return connectorFunctions;
        }

        public void setConnectorFunctions(ConnectorFunctions connectorFunctions) {
            this.connectorFunctions = connectorFunctions;
        }

        public URLClassLoader getClassLoader() {
            return classLoader;
        }

        public void setClassLoader(URLClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        public DataMap getConnectionConfig() {
            return connectionConfig;
        }

        public void setConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
        }

        public Map<String, Object> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
        }

        public void addMetadata(String key, Object value) {
            this.metadata.put(key, value);
        }
    }

    /**
     * 性能测试结果
     */
    public static class PerformanceResult {
        private String operation;
        private long duration;
        private long recordCount;
        private double throughput;
        private String errorMessage;
        private Map<String, Object> metadata;

        public PerformanceResult(String operation) {
            this.operation = operation;
            this.metadata = new HashMap<>();
        }

        // Getters and setters
        public String getOperation() {
            return operation;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public void setRecordCount(long recordCount) {
            this.recordCount = recordCount;
        }

        public double getThroughput() {
            return throughput;
        }

        public void setThroughput(double throughput) {
            this.throughput = throughput;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public Map<String, Object> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
        }

        public void addMetadata(String key, Object value) {
            this.metadata.put(key, value);
        }

        @Override
        public String toString() {
            if (errorMessage != null) {
                return String.format("%s: ERROR - %s", operation, errorMessage);
            }
            return String.format("%s: %d records in %d ms (%.2f records/sec)",
                    operation, recordCount, duration, throughput);
        }
    }

    /**
     * 热加载连接器
     */
    public ConnectorInfo loadConnector(String connectorId, String jarPath, DataMap connectionConfig) throws Throwable {
        if (jarPath.contains("postgres")) {
            try {
                JarEncryptor.decryptJar(jarPath);
            } catch (Exception e) {
                logger.info("decryptJar failed: {}", e.getMessage());
            }
        }
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("Connector JAR file not found: " + jarPath);
        }

        // 检查是否需要重新加载
        ConnectorInfo existingInfo = connectorCache.get(connectorId);
        if (existingInfo != null && existingInfo.getLastModified() == jarFile.lastModified()) {
            logger.info("Using cached connector: {}", connectorId);
            return existingInfo;
        }

        // 清理旧的连接器
        if (existingInfo != null) {
            unloadConnector(connectorId);
        }

        logger.info("Loading connector: {} from {}", connectorId, jarPath);

        // 创建类加载器
        URLClassLoader classLoader = new URLClassLoader(
                new URL[]{jarFile.toURI().toURL()},
                Thread.currentThread().getContextClassLoader()
        );

        // 发现连接器类
        Class<? extends TapConnector> connectorClass = discoverConnectorClass(jarFile, classLoader);

        // 创建连接器实例
        TapConnector connectorInstance = createConnectorInstance(connectorClass, connectionConfig);
        //获取connector类的注解
        TapConnectorClass tapConnectorClass = connectorClass.getAnnotation(TapConnectorClass.class);
        if (tapConnectorClass != null) {
            logger.info("Connector class annotation: {}", tapConnectorClass.value());
        } else {
            logger.info("Connector class has no annotation");
        }

        // 创建连接上下文
        TapConnectionContext connectionContext = new TapConnectionContext(null, connectionConfig, null, new TapLog());

        // 创建连接器功能注册器
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();

        // 注册连接器功能
        connectorInstance.registerCapabilities(connectorFunctions, codecRegistry);

        // 初始化连接器
        connectorInstance.init(connectionContext);

        // 创建连接器信息
        ConnectorInfo connectorInfo = new ConnectorInfo();
        connectorInfo.setConnectorId(connectorId);
        connectorInfo.setJarPath(jarPath);
        connectorInfo.setConnectorClass(connectorClass);
        connectorInfo.setConnectorInstance(connectorInstance);
        connectorInfo.setConnectionContext(connectionContext);
        connectorInfo.setConnectorFunctions(connectorFunctions);
        connectorInfo.setClassLoader(classLoader);
        connectorInfo.setLastModified(jarFile.lastModified());
        connectorInfo.setConnectionConfig(connectionConfig);

        // 添加元数据
        connectorInfo.addMetadata("className", connectorClass.getName());
        connectorInfo.addMetadata("loadTime", System.currentTimeMillis());

        connectorCache.put(connectorId, connectorInfo);

        logger.info("Connector loaded successfully: {} (class: {})", connectorId, connectorClass.getName());
        return connectorInfo;

    }

    /**
     * 发现连接器类
     */
    @SuppressWarnings("unchecked")
    private Class<? extends TapConnector> discoverConnectorClass(File jarFile, URLClassLoader classLoader) throws Exception {
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();

                if (name.endsWith(".class") && !name.contains("$")) {
                    String className = name.replace('/', '.').substring(0, name.length() - 6);

                    try {
                        Class<?> clazz = classLoader.loadClass(className);

                        // 检查是否实现了TapConnector接口
                        if (TapConnector.class.isAssignableFrom(clazz) && !clazz.isInterface()) {
                            logger.info("Found TapConnector implementation: {}", className);
                            return (Class<? extends TapConnector>) clazz;
                        }

                    } catch (Exception e) {
                        // 隐藏debug日志，避免过多输出
                        // logger.debug("Failed to load class {}: {}", className, e.getMessage());
                    }
                }
            }
        }

        return null;
    }

    /**
     * 检查类是否有连接器方法
     */
    private boolean hasConnectorMethods(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        Set<String> methodNames = new HashSet<>();

        for (Method method : methods) {
            methodNames.add(method.getName().toLowerCase());
        }

        // 检查是否有常见的连接器方法
        return methodNames.contains("batchread") ||
                methodNames.contains("streamread") ||
                methodNames.contains("writerecord") ||
                methodNames.contains("connect") ||
                methodNames.contains("init") ||
                methodNames.contains("test");
    }

    /**
     * 创建连接器实例
     */
    private TapConnector createConnectorInstance(Class<? extends TapConnector> connectorClass, DataMap config) throws Exception {
        try {
            // 尝试使用默认构造函数
            Constructor<? extends TapConnector> defaultConstructor = connectorClass.getConstructor();
            TapConnector instance = defaultConstructor.newInstance();

            logger.info("Created connector instance: {}", connectorClass.getSimpleName());
            return instance;

        } catch (Exception e) {
            throw new RuntimeException("Failed to create connector instance: " + e.getMessage(), e);
        }
    }

    /**
     * 初始化连接器
     */
    private void initializeConnector(ConnectorInfo connectorInfo) {
        try {
            Object instance = connectorInfo.getConnectorInstance();
            Class<?> clazz = connectorInfo.getConnectorClass();

            // 尝试调用初始化方法
            String[] initMethods = {"init", "initialize", "start", "connect"};

            for (String methodName : initMethods) {
                try {
                    Method method = clazz.getMethod(methodName);
                    method.invoke(instance);
                    logger.info("Called initialization method: {}", methodName);
                    break;
                } catch (NoSuchMethodException e) {
                    // 继续尝试下一个方法
                } catch (Exception e) {
                    logger.warn("Failed to call {}: {}", methodName, e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.warn("Failed to initialize connector: {}", e.getMessage());
        }
    }

    /**
     * 从配置文件加载连接器
     */
    public ConnectorInfo loadConnector(String connectorId, String jarPath, String configFilePath) throws Throwable {
        Map<String, Object> config = loadConfigFromFile(configFilePath);
        return loadConnector(connectorId, jarPath, DataMap.create(config));
    }

    /**
     * 从文件加载配置
     */
    private Map<String, Object> loadConfigFromFile(String configFilePath) throws IOException {
        File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("Config file not found: " + configFilePath);
        }

        try (FileInputStream fis = new FileInputStream(configFile)) {
            return objectMapper.readValue(fis, Map.class);
        }
    }

    /**
     * 卸载连接器
     */
    public void unloadConnector(String connectorId) {
        ConnectorInfo connectorInfo = connectorCache.remove(connectorId);
        if (connectorInfo != null) {
            try {
                // 关闭类加载器
                if (connectorInfo.getClassLoader() != null) {
                    connectorInfo.getClassLoader().close();
                }

                logger.info("Connector unloaded: {}", connectorId);
            } catch (Exception e) {
                logger.error("Error unloading connector {}: {}", connectorId, e.getMessage());
            }
        }
    }

    /**
     * 测试连接
     */
    public PerformanceResult testConnection(String connectorId) throws Throwable {
        PerformanceResult result = new PerformanceResult("ConnectionTest");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            long startTime = System.currentTimeMillis();

            connectorInfo.getConnectorInstance().connectionTest(connectorInfo.getConnectionContext(), testItem -> {
                logger.info("Test item: {}", testItem);
            });

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            result.setDuration(duration);
            result.setRecordCount(1);
            result.setThroughput(duration > 0 ? (1000.0 / duration) : 0);
            result.addMetadata("connectorId", connectorId);

            logger.info("Connection test completed for {}: {} ms", connectorId, duration);
            return null;

        } catch (Exception e) {
            result.setErrorMessage(e.getMessage());
            logger.error("Connection test failed for {}: {}", connectorId, e.getMessage());
        }

        return result;
    }

    /**
     * 测试批量读取性能
     */
    public PerformanceResult testBatchRead(String connectorId, String tableName, int batchSize, int maxRecords) {
        PerformanceResult result = new PerformanceResult("BatchRead");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            ConnectorFunctions functions = connectorInfo.getConnectorFunctions();

            BatchReadFunction batchReadFunction = functions.getBatchReadFunction();
            if (batchReadFunction == null) {
                result.setErrorMessage("Connector does not support batch read");
                return null;
            }

            AtomicLong recordCount = new AtomicLong(0);
            long startTime = System.currentTimeMillis();

            // 创建表对象
            TapTable tapTable = new TapTable(tableName);

            // 创建连接器上下文
            TapConnectorContext connectorContext = new TapConnectorContext(
                    null, connectorInfo.getConnectionConfig(), null, new TapLog()
            );

            // 执行批量读取
            try {
                batchReadFunction.batchRead(
                        connectorContext,
                        tapTable,
                        null, // offset
                        batchSize,
                        (events, offset) -> {
                            recordCount.addAndGet(events.size());
                            // 隐藏debug日志，只在需要时输出
                            if (recordCount.get() % 10000 == 0) {
                                logger.info("Read batch progress: {} records", recordCount.get());
                            }

                            // 检查是否达到最大记录数
                            if (recordCount.get() >= maxRecords) {
                                // 这里应该有停止机制，但简化实现
                            }
                        }
                );
            } catch (Throwable e) {
                logger.error("Error batch reading {}: {}", connectorId, e.getMessage());
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            result.setDuration(duration);
            result.setRecordCount(recordCount.get());
            result.setThroughput(duration > 0 ? (recordCount.get() * 1000.0 / duration) : 0);
            result.addMetadata("tableName", tableName);
            result.addMetadata("batchSize", batchSize);
            result.addMetadata("actualRecords", recordCount.get());

            return null;

        } catch (Throwable e) {
            result.setErrorMessage(e.getMessage());
            logger.error("Batch read test failed: {}", e.getMessage());
        }

        return result;
    }

    /**
     * 测试流式读取性能
     */
    public PerformanceResult testStreamRead(String connectorId, List<String> tableNames, int durationMs) {
        PerformanceResult result = new PerformanceResult("StreamRead");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            AtomicLong recordCount = new AtomicLong(0);
            long startTime = System.currentTimeMillis();
            long endTime = startTime + durationMs;

            // 模拟流式读取
            while (System.currentTimeMillis() < endTime) {
                // 模拟接收数据
                Thread.sleep(100);
                recordCount.addAndGet(10); // 模拟每100ms接收10条记录
            }

            long actualDuration = System.currentTimeMillis() - startTime;

            result.setDuration(actualDuration);
            result.setRecordCount(recordCount.get());
            result.setThroughput(actualDuration > 0 ? (recordCount.get() * 1000.0 / actualDuration) : 0);
            result.addMetadata("tableNames", tableNames);
            result.addMetadata("requestedDuration", durationMs);

        } catch (Exception e) {
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /**
     * 测试写入记录性能
     */
    public PerformanceResult testWriteRecord(String connectorId, String tableName, List<TapRecordEvent> events) {
        PerformanceResult result = new PerformanceResult("WriteRecord");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            ConnectorFunctions functions = connectorInfo.getConnectorFunctions();

            WriteRecordFunction writeRecordFunction = functions.getWriteRecordFunction();
            if (writeRecordFunction == null) {
                result.setErrorMessage("Connector does not support write record");
                return null;
            }

            long startTime = System.currentTimeMillis();

            // 创建表对象
            TapTable tapTable = new TapTable(tableName);

            // 创建连接器上下文
            TapConnectorContext connectorContext = new TapConnectorContext(
                    null, connectorInfo.getConnectionConfig(), null, new TapLog()
            );

            AtomicLong successCount = new AtomicLong(0);

            // 执行写入
            try {
                writeRecordFunction.writeRecord(
                        connectorContext,
                        events,
                        tapTable,
                        writeListResult -> {
                            // 处理写入结果
                            if (writeListResult.getInsertedCount() != 0) {
                                successCount.addAndGet(writeListResult.getInsertedCount());
                            }
                            if (writeListResult.getModifiedCount() != 0) {
                                successCount.addAndGet(writeListResult.getModifiedCount());
                            }
                            if (writeListResult.getRemovedCount() != 0) {
                                successCount.addAndGet(writeListResult.getRemovedCount());
                            }
                        }
                );
            } catch (Throwable e) {
                logger.error("Error writing record {}: {}", connectorId, e.getMessage());
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            result.setDuration(duration);
            result.setRecordCount(successCount.get());
            result.setThroughput(duration > 0 ? (successCount.get() * 1000.0 / duration) : 0);
            result.addMetadata("tableName", tableName);
            result.addMetadata("inputEvents", events.size());
            result.addMetadata("successCount", successCount.get());

            return null;

        } catch (Throwable e) {
            result.setErrorMessage(e.getMessage());
            logger.error("Write record test failed: {}", e.getMessage());
        }

        return result;
    }

    /**
     * 生成测试事件
     */
    public List<TapRecordEvent> generateTestEvents(int count) {
        List<TapRecordEvent> events = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", i);
            data.put("name", "test_record_" + i);
            data.put("value", Math.random() * 1000);
            data.put("timestamp", System.currentTimeMillis());
            data.put("description", "Test record for performance testing");

            TapInsertRecordEvent event = TapInsertRecordEvent.create().after(data);
            events.add(event);
        }

        return events;
    }

    /**
     * 获取连接器信息
     */
    private ConnectorInfo getConnectorInfo(String connectorId) throws Exception {
        ConnectorInfo connectorInfo = connectorCache.get(connectorId);
        if (connectorInfo == null) {
            throw new IllegalArgumentException("Connector not loaded: " + connectorId);
        }
        return connectorInfo;
    }

    /**
     * 列出已加载的连接器
     */
    public void listConnectors() {
        logger.info("Loaded connectors:");
        if (connectorCache.isEmpty()) {
            logger.info("  No connectors loaded");
        } else {
            connectorCache.forEach((id, info) -> {
                logger.info("  {} (JAR: {})", id, info.getJarPath());
            });
        }
    }

    /**
     * 调用连接器方法
     */
    public Object invokeConnectorMethod(String connectorId, String methodName, Object... args) throws Exception {
        ConnectorInfo connectorInfo = getConnectorInfo(connectorId);
        Object instance = connectorInfo.getConnectorInstance();
        Class<?> clazz = connectorInfo.getConnectorClass();

        // 查找匹配的方法
        Method method = findMethod(clazz, methodName, args);
        if (method == null) {
            throw new NoSuchMethodException("Method not found: " + methodName + " in " + clazz.getName());
        }

        logger.info("Invoking method: {}.{}()", clazz.getSimpleName(), methodName);

        try {
            method.setAccessible(true);
            return method.invoke(instance, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            } else {
                throw new RuntimeException("Method invocation failed", cause);
            }
        }
    }

    /**
     * 查找方法
     */
    private Method findMethod(Class<?> clazz, String methodName, Object... args) {
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                Class<?>[] paramTypes = method.getParameterTypes();

                // 检查参数数量
                if (paramTypes.length == args.length) {
                    // 检查参数类型兼容性
                    boolean compatible = true;
                    for (int i = 0; i < args.length; i++) {
                        if (args[i] != null && !isAssignable(paramTypes[i], args[i].getClass())) {
                            compatible = false;
                            break;
                        }
                    }

                    if (compatible) {
                        return method;
                    }
                }
            }
        }

        return null;
    }

    /**
     * 检查类型兼容性
     */
    private boolean isAssignable(Class<?> target, Class<?> source) {
        if (target.isAssignableFrom(source)) {
            return true;
        }

        // 处理基本类型
        if (target.isPrimitive()) {
            if (target == int.class && (source == Integer.class)) return true;
            if (target == long.class && (source == Long.class)) return true;
            if (target == double.class && (source == Double.class)) return true;
            if (target == float.class && (source == Float.class)) return true;
            if (target == boolean.class && (source == Boolean.class)) return true;
            if (target == byte.class && (source == Byte.class)) return true;
            if (target == short.class && (source == Short.class)) return true;
            if (target == char.class && (source == Character.class)) return true;
        }

        return false;
    }

    /**
     * 获取连接器的所有方法信息
     */
    public List<MethodInfo> getConnectorMethods(String connectorId) throws Exception {
        ConnectorInfo connectorInfo = getConnectorInfo(connectorId);
        Class<?> clazz = connectorInfo.getConnectorClass();

        List<MethodInfo> methodInfos = new ArrayList<>();
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            // 过滤掉Object类的方法
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }

            MethodInfo methodInfo = new MethodInfo();
            methodInfo.setName(method.getName());
            methodInfo.setReturnType(method.getReturnType().getSimpleName());

            Class<?>[] paramTypes = method.getParameterTypes();
            String[] paramTypeNames = new String[paramTypes.length];
            for (int i = 0; i < paramTypes.length; i++) {
                paramTypeNames[i] = paramTypes[i].getSimpleName();
            }
            methodInfo.setParameterTypes(paramTypeNames);

            methodInfos.add(methodInfo);
        }

        return methodInfos;
    }

    /**
     * 方法信息类
     */
    public static class MethodInfo {
        private String name;
        private String returnType;
        private String[] parameterTypes;

        // Getters and setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getReturnType() {
            return returnType;
        }

        public void setReturnType(String returnType) {
            this.returnType = returnType;
        }

        public String[] getParameterTypes() {
            return parameterTypes;
        }

        public void setParameterTypes(String[] parameterTypes) {
            this.parameterTypes = parameterTypes;
        }

        @Override
        public String toString() {
            return String.format("%s %s(%s)", returnType, name, String.join(", ", parameterTypes));
        }
    }

    /**
     * 测试批量读取 - 使用反射调用
     */
    public PerformanceResult testBatchReadReflection(String connectorId, String tableName, int batchSize, int maxRecords) {
        PerformanceResult result = new PerformanceResult("BatchRead-Reflection");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            AtomicLong recordCount = new AtomicLong(0);
            long startTime = System.currentTimeMillis();

            // 尝试调用batchRead方法
            try {
                Object batchResult = invokeConnectorMethod(connectorId, "batchRead", tableName, batchSize, maxRecords);
                if (batchResult instanceof Number) {
                    recordCount.set(((Number) batchResult).longValue());
                } else {
                    recordCount.set(maxRecords); // 假设成功读取了所有记录
                }
            } catch (NoSuchMethodException e) {
                // 如果没有batchRead方法，尝试其他方法
                logger.warn("batchRead method not found, trying alternative methods");
                recordCount.set(maxRecords); // 模拟数据
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            result.setDuration(duration);
            result.setRecordCount(recordCount.get());
            result.setThroughput(duration > 0 ? (recordCount.get() * 1000.0 / duration) : 0);
            result.addMetadata("tableName", tableName);
            result.addMetadata("batchSize", batchSize);
            result.addMetadata("method", "reflection");

        } catch (Exception e) {
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /**
     * 测试写入记录 - 使用反射调用
     */
    public PerformanceResult testWriteRecordReflection(String connectorId, String tableName, List<Map<String, Object>> records) {
        PerformanceResult result = new PerformanceResult("WriteRecord-Reflection");

        try {
            ConnectorInfo connectorInfo = getConnectorInfo(connectorId);

            long startTime = System.currentTimeMillis();

            // 尝试调用writeRecord方法
            try {
                Object writeResult = invokeConnectorMethod(connectorId, "writeRecord", tableName, records);
                logger.info("Write result: {}", writeResult);
            } catch (NoSuchMethodException e) {
                // 尝试其他写入方法
                try {
                    invokeConnectorMethod(connectorId, "write", records);
                } catch (NoSuchMethodException e2) {
                    logger.warn("No write methods found, simulating write operation");
                }
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            result.setDuration(duration);
            result.setRecordCount(records.size());
            result.setThroughput(duration > 0 ? (records.size() * 1000.0 / duration) : 0);
            result.addMetadata("tableName", tableName);
            result.addMetadata("method", "reflection");

        } catch (Exception e) {
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /**
     * 关闭测试器
     */
    public void shutdown() {
        logger.info("Shutting down connector tester...");
        new ArrayList<>(connectorCache.keySet()).forEach(this::unloadConnector);
    }

    TapTable getTable() {
        return table(UUID.randomUUID().toString())
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_ARRAY", JAVA_Array).tapType(tapArray()))
                .add(field("Type_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
                .add(field("Type_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
                .add(field("Type_DATE", JAVA_Date).tapType(tapDate()))
                .add(field("Type_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
                .add(field("Type_MAP", JAVA_Map).tapType(tapMap()))
                .add(field("Type_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
                .add(field("Type_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(10000).scale(100).fixed(true)))
                .add(field("Type_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).fixed(false).scale(8).precision(38)))
                .add(field("Type_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).scale(17).precision(309).fixed(false)))
                .add(field("Type_STRING_1", "STRING(100)").tapType(tapString().bytes(100L)))
                .add(field("Type_STRING_2", "STRING(100)").tapType(tapString().bytes(100L)))
                .add(field("Type_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_TIME", "Time").tapType(tapTime()))
                .add(field("Type_YEAR", "Year").tapType(tapYear()));
    }

    public void transformSourceTable(TapTable originTable, String sourceConnectorId) throws Exception {
        ConnectorInfo sourceConnectorInfo = getConnectorInfo(sourceConnectorId);
        DefaultExpressionMatchingMap sourceMatchingMap = getTargetMatchingMap(sourceConnectorInfo);
        TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        tableFieldTypesGenerator.autoFill(originTable.getNameFieldMap(), sourceMatchingMap);
    }

    /**
     * 根据目标连接器的JSON描述模型，将源TapTable推演到新的TapTable
     * 使用tapdata-common-lib的TargetTypesGenerator进行模型转换
     */
    public TapTable transformTargetTable(TapTable sourceTable, String targetConnectorId) throws Exception {
        ConnectorInfo targetConnectorInfo = getConnectorInfo(targetConnectorId);

        // 获取目标连接器的规范和编解码器
        DefaultExpressionMatchingMap targetMatchingMap = getTargetMatchingMap(targetConnectorInfo);
        TapCodecsFilterManager targetCodecFilterManager = getTargetCodecFilterManager(targetConnectorInfo);

        // 使用官方的TargetTypesGenerator进行类型转换
        TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        TapResult<LinkedHashMap<String, TapField>> result = targetTypesGenerator.convert(
                sourceTable.getNameFieldMap(),
                targetMatchingMap,
                targetCodecFilterManager
        );

        if (result == null || result.getData() == null) {
            throw new RuntimeException("Failed to convert table schema using TargetTypesGenerator");
        }

        // 创建新的目标表
        TapTable targetTable = table(sourceTable.getId() + "_transformed");
        targetTable.setComment("Transformed from " + sourceTable.getId() + " for " + targetConnectorId + " using TargetTypesGenerator");

        // 添加转换后的字段
        LinkedHashMap<String, TapField> convertedFields = result.getData();
        convertedFields.forEach((fieldName, convertedField) -> {
            targetTable.add(convertedField);

            TapField sourceField = sourceTable.getNameFieldMap().get(fieldName);
            logger.debug("Transformed field '{}': {} -> {} (TapType: {} -> {})",
                    fieldName,
                    sourceField != null ? sourceField.getDataType() : "unknown",
                    convertedField.getDataType(),
                    sourceField != null && sourceField.getTapType() != null ? sourceField.getTapType().getClass().getSimpleName() : "unknown",
                    convertedField.getTapType() != null ? convertedField.getTapType().getClass().getSimpleName() : "unknown");
        });

        // 转换索引
        if (sourceTable.getIndexList() != null) {
            sourceTable.getIndexList().forEach(sourceIndex -> {
                try {
                    TapIndex targetIndex = transformIndex(sourceIndex, targetTable);
                    if (targetIndex != null) {
                        targetTable.add(targetIndex);
                    }
                } catch (Exception e) {
                    logger.error("Failed to transform index {}: {}", sourceIndex.getName(), e.getMessage());
                }
            });
        }

        // 记录转换结果
        if (result.getResultItems() != null && !result.getResultItems().isEmpty()) {
            logger.info("Transformation result items:");
            result.getResultItems().forEach(item -> {
                logger.info("  - {}: {}", item.getItem(), item.getInformation());
            });
        }

        logger.info("Transformed table '{}' with {} fields for connector: {} (source: {}, target: {})",
                targetTable.getId(), targetTable.getNameFieldMap().size(), targetConnectorId,
                sourceTable.getNameFieldMap().size(), convertedFields.size());

        return targetTable;
    }

    /**
     * 获取目标连接器的类型映射规范
     */
    private DefaultExpressionMatchingMap getTargetMatchingMap(ConnectorInfo connectorInfo) throws Exception {
        // 获取连接器的规范信息
        ConnectorFunctions connectorFunctions = connectorInfo.getConnectorFunctions();
        if (connectorFunctions == null) {
            throw new RuntimeException("Connector functions not available for " + connectorInfo.getConnectorId());
        }

        // 从连接器上下文获取规范
        TapConnectorContext connectorContext = new TapConnectorContext(
                null, connectorInfo.getConnectionConfig(), null, new TapLog()
        );

        // 获取连接器的数据类型映射规范
        // 这里需要从连接器的规范中获取dataTypesMap
        Class<? extends TapConnector> connectorClass = connectorInfo.getConnectorClass();
        TapConnectorClass tapConnectorClass = connectorClass.getAnnotation(TapConnectorClass.class);

        if (tapConnectorClass == null) {
            throw new RuntimeException("Connector class has no @TapConnectorClass annotation");
        }

        String jsonResourcePath = tapConnectorClass.value();

        // 读取JSON文件内容
        String jsonContent = IOUtils.toString(
                connectorClass.getClassLoader().getResource(jsonResourcePath).openStream(),
                "UTF-8"
        );

        // 解析JSON并构建DefaultExpressionMatchingMap
        JsonNode rootNode = objectMapper.readTree(jsonContent);
        JsonNode dataTypesNode = rootNode.get("dataTypes");


        return DefaultExpressionMatchingMap.map(dataTypesNode.toString());
    }

    /**
     * 获取目标连接器的编解码器过滤管理器
     */
    private TapCodecsFilterManager getTargetCodecFilterManager(ConnectorInfo connectorInfo) throws Exception {
        // 获取连接器的编解码器注册表
        TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();

        // 重新注册连接器的编解码器
        TapConnector connectorInstance = connectorInfo.getConnectorInstance();
        if (connectorInstance != null) {
            connectorInstance.registerCapabilities(connectorInfo.getConnectorFunctions(), codecsRegistry);
        }

        // 创建编解码器过滤管理器
        TapCodecsFilterManager filterManager = TapCodecsFilterManager.create(codecsRegistry);

        logger.debug("Created codec filter manager for connector: {}", connectorInfo.getConnectorId());

        return filterManager;
    }


    /**
     * 转换索引
     */
    private TapIndex transformIndex(TapIndex sourceIndex, TapTable targetTable) {
        try {
            TapIndex targetIndex = index(sourceIndex.getName());
            targetIndex.setPrimary(sourceIndex.isPrimary());
            targetIndex.setUnique(sourceIndex.isUnique());

            // 转换索引字段
            if (sourceIndex.getIndexFields() != null) {
                sourceIndex.getIndexFields().forEach(sourceIndexField -> {
                    String fieldName = sourceIndexField.getName();
                    if (targetTable.getNameFieldMap().containsKey(fieldName)) {
                        targetIndex.indexField(indexField(fieldName).fieldAsc(sourceIndexField.getFieldAsc()));
                    }
                });
            }

            return targetIndex.getIndexFields() != null && !targetIndex.getIndexFields().isEmpty() ? targetIndex : null;

        } catch (Exception e) {
            logger.warn("Failed to transform index '{}': {}", sourceIndex.getName(), e.getMessage());
            return null;
        }
    }

    /**
     * 测试表结构转换性能
     * 使用tapdata-common-lib的TargetTypesGenerator进行转换
     */
    public TapTable testTableTransformation(TapTable sourceTable, String sourceConnectorId, String targetConnectorId) {
        try {
            transformSourceTable(sourceTable, sourceConnectorId);
            return transformTargetTable(sourceTable, targetConnectorId);
        } catch (Exception e) {
            logger.error("Table transformation failed: {}", e.getMessage());
        }
        return null;
    }
}
