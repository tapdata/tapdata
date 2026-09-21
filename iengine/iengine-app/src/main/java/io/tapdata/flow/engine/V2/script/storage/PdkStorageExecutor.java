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
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

final class PdkStorageExecutor implements StorageExecutor {

    private static final Map<String, String> STORAGE_CLASSES;

    static {
        // Keep this list synchronized with io.tapdata.common.FileProtocolEnum in
        // file-connector-core. iengine cannot compile against that PDK-loaded
        // enum, so this boundary must be checked whenever a protocol is added.
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
        this.rootPath = resolveRootPath(connections.getConfig());
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

    static String resolveRootPath(Map<String, Object> config) {
        String writeFilePath = value(config, "writeFilePath", "").trim();
        if (!writeFilePath.isEmpty()) {
            return writeFilePath;
        }
        String configuredRootPath = value(config, "rootPath", "").trim();
        if (!configuredRootPath.isEmpty()) {
            return configuredRootPath;
        }
        String readRoots = value(config, "filePathString", "");
        String[] roots = readRoots.split(",");
        String singleRoot = null;
        int rootCount = 0;
        for (String root : roots) {
            String trimmedRoot = root.trim();
            if (!trimmedRoot.isEmpty()) {
                singleRoot = trimmedRoot;
                rootCount++;
            }
        }
        return rootCount == 1 ? singleRoot : "";
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
        private final Semaphore operationLock = new Semaphore(1, true);

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

        @Override
        public void init(Map<String, Object> params) {
            // The builder initializes the delegate before it is wrapped.
        }

        @Override
        public void destroy() throws Exception {
            if (!destroyed.compareAndSet(false, true)) {
                return;
            }
            Throwable failure = null;
            operationLock.acquireUninterruptibly();
            try {
                delegate.destroy();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                operationLock.release();
                releaseResources(associateId, stateMap, tapTableMap, logger);
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure != null) {
                throw new RuntimeException("Destroy file storage failed", failure);
            }
        }

        @Override
        public TapFile getFile(String path) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.getFile(path);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                delegate.readFile(path, consumer);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public InputStream readFile(String path) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return lockInputStream(delegate.readFile(path));
            } catch (Throwable throwable) {
                operationLock.release();
                throw throwable;
            }
        }

        @Override
        public boolean isFileExist(String path) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.isFileExist(path);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public boolean move(String sourcePath, String destPath) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.move(sourcePath, destPath);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public boolean delete(String path) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.delete(path);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.saveFile(path, is, canReplace);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return lockOutputStream(delegate.openFileOutputStream(path, append));
            } catch (Throwable throwable) {
                operationLock.release();
                throw throwable;
            }
        }

        @Override
        public boolean supportAppendData() {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.supportAppendData();
            } finally {
                operationLock.release();
            }
        }

        @Override
        public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                        Collection<String> excludeRegs, boolean recursive, int batchSize,
                                        Consumer<List<TapFile>> consumer) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                delegate.getFilesInDirectory(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public boolean isDirectoryExist(String path) throws Exception {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.isDirectoryExist(path);
            } finally {
                operationLock.release();
            }
        }

        @Override
        public String getConnectInfo() {
            operationLock.acquireUninterruptibly();
            try {
                return delegate.getConnectInfo();
            } finally {
                operationLock.release();
            }
        }

        private InputStream lockInputStream(InputStream input) {
            if (input == null) {
                operationLock.release();
                return null;
            }
            AtomicBoolean released = new AtomicBoolean();
            return new FilterInputStream(input) {
                @Override
                public void close() throws java.io.IOException {
                    try {
                        super.close();
                    } finally {
                        releaseStreamLock(released);
                    }
                }
            };
        }

        private OutputStream lockOutputStream(OutputStream output) {
            if (output == null) {
                operationLock.release();
                return null;
            }
            AtomicBoolean released = new AtomicBoolean();
            return new FilterOutputStream(output) {
                @Override
                public void close() throws java.io.IOException {
                    try {
                        super.close();
                    } finally {
                        releaseStreamLock(released);
                    }
                }
            };
        }

        private void releaseStreamLock(AtomicBoolean released) {
            if (released.compareAndSet(false, true)) {
                operationLock.release();
            }
        }
    }
}
