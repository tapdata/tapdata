package io.tapdata.flow.engine.V2.script.storage;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.TapFileOperationService;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.FileStorageFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class PdkStorageExecutor implements StorageExecutor {

    private static final int MAX_SESSIONS = 10;
    private static final long SESSION_IDLE_TIMEOUT_MS = 600_000L;

    private final String connectionName;
    private final Connections connections;
    private final ClientMongoOperator clientMongoOperator;
    private final HazelcastInstance hazelcastInstance;
    private final Log scriptLogger;
    private final FileEndpoint endpoint;
    private final PdkFileStorageSessionManager sessions;
    private final DefaultFileOperationService operationService;

    PdkStorageExecutor(String connectionName,
                       Connections connections,
                       ClientMongoOperator clientMongoOperator,
                       HazelcastInstance hazelcastInstance,
                       Log scriptLogger,
                       String taskId,
                       String nodeId) {
        this.connectionName = connectionName;
        this.connections = connections;
        this.clientMongoOperator = clientMongoOperator;
        this.hazelcastInstance = hazelcastInstance;
        this.scriptLogger = scriptLogger;
        this.endpoint = createEndpoint(connections);
        this.sessions = new PdkFileStorageSessionManager(
                ignored -> createPdkStorage(taskId, nodeId), MAX_SESSIONS, SESSION_IDLE_TIMEOUT_MS);
        this.operationService = new DefaultFileOperationService(sessions.delegate());
    }

    @Override
    public String getConnectionName() {
        return connectionName;
    }

    @Override
    public FileEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public TapFileOperationService getOperationService() {
        return operationService;
    }

    @Override
    public void close() {
        sessions.close();
    }

    private TapFileStorage createPdkStorage(String taskId, String nodeId) {
        String protocol = endpoint.getProtocol();
        if (!"ftp".equals(protocol)) {
            throw new io.tapdata.file.operation.FileOperationException(
                    io.tapdata.file.operation.FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                    "JS storage currently supports FTP only, protocol: " + protocol);
        }
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(
                clientMongoOperator, connections.getPdkHash());
        if (databaseType == null) {
            throw new io.tapdata.file.operation.FileOperationException(
                    io.tapdata.file.operation.FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "No PDK database type found for connection: " + connectionName);
        }

        String tag = "StorageExecutor-" + taskId + "-" + nodeId + "-" + connectionName;
        String associateId = "StorageExecutor-" + connections.getName() + "-" + UUIDGenerator.uuid();
        PdkStateMap stateMap = new PdkStateMap(tag, hazelcastInstance);
        PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("StorageExecutor", tag);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        ConnectorNode connectorNode = null;
        try {
            connectorNode = PdkUtil.createNode(tag, databaseType, clientMongoOperator, associateId,
                    connections.getConfig(), pdkTableMap, stateMap, globalStateMap,
                    InstanceFactory.instance(LogFactory.class).getLog());
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT,
                    connectorNode::connectorInit, tag);
            ConnectorFunctions functions = connectorNode.getConnectorFunctions();
            FileStorageFunction storageFunction = functions == null ? null : functions.getFileStorageFunction();
            if (storageFunction == null) {
                throw new io.tapdata.file.operation.FileOperationException(
                        io.tapdata.file.operation.FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                        "Connection does not expose FileStorageFunction: " + connectionName);
            }
            TapFileStorage storage = storageFunction.getStorage(connectorNode.getConnectorContext());
            if (storage == null) {
                throw new io.tapdata.file.operation.FileOperationException(
                        io.tapdata.file.operation.FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                        "File storage is not initialized: " + connectionName);
            }
            return new PdkManagedFileStorage(storage, connectorNode, associateId, stateMap, tapTableMap, tag, scriptLogger);
        } catch (Throwable throwable) {
            if (connectorNode != null) {
                stopAndRelease(connectorNode, associateId, stateMap, tapTableMap, tag, scriptLogger);
            } else {
                stateMap.reset();
                tapTableMap.reset();
                PDKIntegration.releaseAssociateId(associateId);
            }
            if (throwable instanceof RuntimeException) throw (RuntimeException) throwable;
            throw new RuntimeException("Create PDK file storage failed: " + connectionName, throwable);
        }
    }

    private static FileEndpoint createEndpoint(Connections connections) {
        Map<String, Object> config = connections.getConfig();
        String protocol = value(config, "protocol", value(config, "file_source_protocol", null));
        if (protocol == null || protocol.trim().isEmpty()) {
            throw new io.tapdata.file.operation.FileOperationException(
                    io.tapdata.file.operation.FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "File connection protocol is required: " + connections.getName());
        }
        String rootPath = value(config, "rootPath", value(config, "filePathString", ""));
        return FileEndpoint.builder().protocol(protocol).params(config).rootPath(rootPath).build();
    }

    private static String value(Map<String, Object> config, String key, String defaultValue) {
        if (config == null || !config.containsKey(key) || config.get(key) == null) return defaultValue;
        return String.valueOf(config.get(key));
    }

    private static void stopAndRelease(ConnectorNode connectorNode, String associateId,
                                       PdkStateMap stateMap, TapTableMap<String, TapTable> tapTableMap,
                                       String tag, Log logger) {
        CommonUtils.handleAnyError(() -> {
            PDKInvocationMonitor.stop(connectorNode);
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP,
                    connectorNode::connectorStop, tag);
        }, error -> logWarn(logger, "Stop PDK file connector failed: " + error.getMessage()));
        CommonUtils.handleAnyError(() -> PDKIntegration.releaseAssociateId(associateId),
                error -> logWarn(logger, "Release PDK file connector failed: " + error.getMessage()));
        CommonUtils.handleAnyError(stateMap::reset,
                error -> logWarn(logger, "Reset PDK file state failed: " + error.getMessage()));
        CommonUtils.handleAnyError(tapTableMap::reset,
                error -> logWarn(logger, "Reset PDK file table map failed: " + error.getMessage()));
    }

    private static void logWarn(Log logger, String message) {
        if (logger != null) logger.warn(message);
    }

    private static final class PdkManagedFileStorage implements TapFileStorage {
        private final TapFileStorage delegate;
        private final ConnectorNode connectorNode;
        private final String associateId;
        private final PdkStateMap stateMap;
        private final TapTableMap<String, TapTable> tapTableMap;
        private final String tag;
        private final Log logger;

        private PdkManagedFileStorage(TapFileStorage delegate, ConnectorNode connectorNode,
                                      String associateId, PdkStateMap stateMap,
                                      TapTableMap<String, TapTable> tapTableMap, String tag, Log logger) {
            this.delegate = delegate;
            this.connectorNode = connectorNode;
            this.associateId = associateId;
            this.stateMap = stateMap;
            this.tapTableMap = tapTableMap;
            this.tag = tag;
            this.logger = logger;
        }

        @Override public void init(Map<String, Object> params) { }
        @Override public void destroy() {
            Throwable failure = null;
            try {
                delegate.destroy();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                stopAndRelease(connectorNode, associateId, stateMap, tapTableMap, tag, logger);
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure != null) throw new RuntimeException("Destroy PDK file storage failed", failure);
        }
        @Override public TapFile getFile(String path) throws Exception { return delegate.getFile(path); }
        @Override public void readFile(String path, Consumer<InputStream> consumer) throws Exception { delegate.readFile(path, consumer); }
        @Override public InputStream readFile(String path) throws Exception { return delegate.readFile(path); }
        @Override public boolean isFileExist(String path) throws Exception { return delegate.isFileExist(path); }
        @Override public boolean move(String sourcePath, String destPath) throws Exception { return delegate.move(sourcePath, destPath); }
        @Override public boolean delete(String path) throws Exception { return delegate.delete(path); }
        @Override public TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception { return delegate.saveFile(path, is, canReplace); }
        @Override public OutputStream openFileOutputStream(String path, boolean append) throws Exception { return delegate.openFileOutputStream(path, append); }
        @Override public boolean supportAppendData() { return delegate.supportAppendData(); }
        @Override public EnumSet<io.tapdata.file.operation.FileStorageCapability> capabilities() { return delegate.capabilities(); }
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs, Collection<String> excludeRegs,
                                                   boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) throws Exception {
            delegate.getFilesInDirectory(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer);
        }
        @Override public boolean isDirectoryExist(String path) throws Exception { return delegate.isDirectoryExist(path); }
        @Override public String getConnectInfo() { return delegate.getConnectInfo(); }
    }
}
