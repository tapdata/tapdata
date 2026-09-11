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
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class PdkStorageExecutor implements StorageExecutor {

    private static final Map<String, String> STORAGE_CLASSES;

    static {
        Map<String, String> classes = new HashMap<>();
        classes.put("local", "io.tapdata.storage.local.LocalFileStorage");
        classes.put("ftp", "io.tapdata.storage.ftp.FtpFileStorage");
        classes.put("sftp", "io.tapdata.storage.sftp.SftpFileStorage");
        classes.put("smb", "io.tapdata.storage.smb.SmbFileStorage");
        classes.put("s3fs", "io.tapdata.storage.s3fs.S3fsFileStorage");
        classes.put("nfs", "io.tapdata.storage.nfs.NfsFileStorage");
        classes.put("oss", "io.tapdata.storage.oss.OssFileStorage");
        STORAGE_CLASSES = Collections.unmodifiableMap(classes);
    }

    private final String connectionName;
    private final String rootPath;
    private final TapFileStorage storage;

    PdkStorageExecutor(String connectionName,
                       Connections connections,
                       ClientMongoOperator clientMongoOperator,
                       HazelcastInstance hazelcastInstance,
                       Log scriptLogger,
                       String taskId,
                       String nodeId) {
        this.connectionName = connectionName;
        String protocol = protocol(connections);
        this.rootPath = value(connections.getConfig(), "rootPath",
                value(connections.getConfig(), "filePathString", ""));
        this.storage = createStorage(connectionName, connections, protocol, clientMongoOperator,
                hazelcastInstance, scriptLogger, taskId, nodeId);
    }

    @Override
    public String getConnectionName() {
        return connectionName;
    }

    @Override
    public TapFileStorage getStorage() {
        return storage;
    }

    @Override
    public String resolvePath(String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new StorageOperationException("File path is required");
        }
        String value = path.trim().replace('\\', '/');
        while (value.startsWith("/")) {
            value = value.substring(1);
        }
        if (value.contains("://") || containsParentSegment(value) || containsControlCharacter(value)) {
            throw new StorageOperationException("File path is not allowed: " + path);
        }
        String root = rootPath == null ? "" : rootPath.trim();
        if (root.isEmpty() || "/".equals(root)) {
            return "/" + value;
        }
        return (root.endsWith("/") ? root : root + "/") + value;
    }

    @Override
    public void close() {
        try {
            storage.destroy();
        } catch (Exception e) {
            throw new StorageOperationException("Destroy file storage failed: " + connectionName, e);
        }
    }

    private TapFileStorage createStorage(String connectionName,
                                         Connections connections,
                                         String protocol,
                                         ClientMongoOperator clientMongoOperator,
                                         HazelcastInstance hazelcastInstance,
                                         Log scriptLogger,
                                         String taskId,
                                         String nodeId) {
        String storageClass = STORAGE_CLASSES.get(protocol);
        if (storageClass == null) {
            throw new StorageOperationException("Unsupported file storage protocol: " + protocol);
        }
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(
                clientMongoOperator, connections.getPdkHash());
        if (databaseType == null) {
            throw new StorageOperationException("No PDK database type found for connection: " + connectionName);
        }

        String tag = "StorageExecutor-" + taskId + "-" + nodeId + "-" + connectionName;
        String associateId = "StorageExecutor-" + connections.getName() + "-" + UUIDGenerator.uuid();
        PdkStateMap stateMap = new PdkStateMap(tag, hazelcastInstance);
        PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("StorageExecutor", tag);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        try {
            // createNode loads the PDK and provides its class loader. It does not
            // initialize the connector, so the file storage is the only remote
            // connection created for this executor.
            ConnectorNode connectorNode = PdkUtil.createNode(tag, databaseType, clientMongoOperator, associateId,
                    connections.getConfig(), pdkTableMap, stateMap, globalStateMap,
                    InstanceFactory.instance(LogFactory.class).getLog());
            TapFileStorage delegate = new TapFileStorageBuilder()
                    .withClassLoader(connectorNode.getConnectorClassLoader())
                    .withStorageClassName(storageClass)
                    .withParams(connections.getConfig())
                    .build();
            return new PdkManagedFileStorage(delegate, associateId, stateMap, tapTableMap, scriptLogger);
        } catch (Throwable throwable) {
            releaseResources(associateId, stateMap, tapTableMap, scriptLogger);
            if (throwable instanceof StorageOperationException) {
                throw (StorageOperationException) throwable;
            }
            throw new StorageOperationException("Create file storage failed: " + connectionName, throwable);
        }
    }

    private static String protocol(Connections connections) {
        String protocol = value(connections.getConfig(), "protocol",
                value(connections.getConfig(), "file_source_protocol", null));
        if (protocol == null || protocol.trim().isEmpty()) {
            throw new StorageOperationException("File connection protocol is required: " + connections.getName());
        }
        return protocol.trim().toLowerCase();
    }

    private static String value(Map<String, Object> config, String key, String defaultValue) {
        if (config == null || !config.containsKey(key) || config.get(key) == null) {
            return defaultValue;
        }
        return String.valueOf(config.get(key));
    }

    private static boolean containsParentSegment(String path) {
        for (String segment : path.split("/")) {
            if ("..".equals(segment)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsControlCharacter(String path) {
        for (int i = 0; i < path.length(); i++) {
            if (Character.isISOControl(path.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static void releaseResources(String associateId,
                                         PdkStateMap stateMap,
                                         TapTableMap<String, TapTable> tapTableMap,
                                         Log logger) {
        try {
            PDKIntegration.releaseAssociateId(associateId);
        } catch (Throwable throwable) {
            logWarn(logger, "Release PDK file connector failed: " + throwable.getMessage());
        }
        try {
            stateMap.reset();
        } catch (Throwable throwable) {
            logWarn(logger, "Reset PDK file state failed: " + throwable.getMessage());
        }
        try {
            tapTableMap.reset();
        } catch (Throwable throwable) {
            logWarn(logger, "Reset PDK file table map failed: " + throwable.getMessage());
        }
    }

    private static void logWarn(Log logger, String message) {
        if (logger != null) {
            logger.warn(message);
        }
    }

    private static final class PdkManagedFileStorage implements TapFileStorage {
        private final TapFileStorage delegate;
        private final String associateId;
        private final PdkStateMap stateMap;
        private final TapTableMap<String, TapTable> tapTableMap;
        private final Log logger;
        private final AtomicBoolean destroyed = new AtomicBoolean();

        private PdkManagedFileStorage(TapFileStorage delegate,
                                      String associateId,
                                      PdkStateMap stateMap,
                                      TapTableMap<String, TapTable> tapTableMap,
                                      Log logger) {
            this.delegate = delegate;
            this.associateId = associateId;
            this.stateMap = stateMap;
            this.tapTableMap = tapTableMap;
            this.logger = logger;
        }

        @Override public void init(Map<String, Object> params) { }

        @Override
        public void destroy() throws Exception {
            if (!destroyed.compareAndSet(false, true)) {
                return;
            }
            Throwable failure = null;
            try {
                delegate.destroy();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                releaseResources(associateId, stateMap, tapTableMap, logger);
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure != null) {
                throw new RuntimeException("Destroy file storage failed", failure);
            }
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
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                                   Collection<String> excludeRegs, boolean recursive, int batchSize,
                                                   Consumer<List<TapFile>> consumer) throws Exception {
            delegate.getFilesInDirectory(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer);
        }
        @Override public boolean isDirectoryExist(String path) throws Exception { return delegate.isDirectoryExist(path); }
        @Override public String getConnectInfo() { return delegate.getConnectInfo(); }
    }
}
