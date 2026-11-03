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
import io.tapdata.flow.engine.V2.script.PdkNodeResources;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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
        return resolvePath(rootPath, path);
    }

    @Override
    public <T> T withStorageOperation(StorageOperation<T> operation) throws Exception {
        return ((PdkManagedFileStorage) storage).withStorageOperation(operation);
    }

    static String resolvePath(String rootPath, String path) {
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
        if (isDirectoryRoot(value)) {
            throw new StorageOperationException("File path must not be the directory root: " + path);
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
        PdkNodeResources resources = new PdkNodeResources(
                tag, associateId, stateMap, tapTableMap, scriptLogger, false);
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
            resources.setConnectorNode(connectorNode);
            return new PdkManagedFileStorage(delegate, resources, scriptLogger, protocol);
        } catch (Throwable throwable) {
            resources.close();
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
        return protocol.trim().toLowerCase(Locale.ROOT);
    }

    private static String value(Map<String, Object> config, String key, String defaultValue) {
        if (config == null || !config.containsKey(key) || config.get(key) == null) {
            return defaultValue;
        }
        return String.valueOf(config.get(key));
    }

    static String resolveRootPath(Map<String, Object> config) {
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
        if (rootCount > 1) {
            throw new StorageOperationException(
                    "JS storage requires a single file root; configure one filePathString root");
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

    private static boolean isDirectoryRoot(String path) {
        for (String segment : path.split("/", -1)) {
            if (!segment.isEmpty() && !".".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsControlCharacter(String path) {
        for (int i = 0; i < path.length(); i++) {
            if (Character.isISOControl(path.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static void logWarn(Log logger, String message) {
        if (logger != null) {
            logger.warn(message);
        }
    }

    private static final class PdkManagedFileStorage implements TapFileStorage {
        private static final long DESTROY_WAIT_MILLIS = 5_000L;

        private final TapFileStorage delegate;
        private final PdkNodeResources nodeResources;
        private final Log logger;
        private final boolean validateParentDirectory;
        private final boolean forceDestroyOnTimeout;
        private final AtomicBoolean destroyed = new AtomicBoolean();
        private final AtomicBoolean resourcesReleased = new AtomicBoolean();
        private final ReentrantLock operationLock = new ReentrantLock(true);

        private PdkManagedFileStorage(TapFileStorage delegate,
                                      PdkNodeResources nodeResources,
                                      Log logger,
                                      String protocol) {
            this.delegate = delegate;
            this.nodeResources = nodeResources;
            this.logger = logger;
            this.validateParentDirectory = "ftp".equalsIgnoreCase(protocol);
            this.forceDestroyOnTimeout = this.validateParentDirectory;
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
            if (tryAcquireForDestroy()) {
                destroyAfterAcquire();
                return;
            }
            if (forceDestroyOnTimeout) {
                logWarn(logger, "FTP file storage operation did not stop within " + DESTROY_WAIT_MILLIS
                        + "ms; forcing connector destroy");
                scheduleForcedDestroy();
                return;
            }
            // TapFileStorage does not guarantee that destroy() is safe while an operation is in flight.
            // Defer it until the operation releases the lock instead of introducing a concurrent close.
            scheduleDeferredDestroy();
        }

        @Override
        public TapFile getFile(String path) throws Exception {
            acquireOperation();
            try {
                return delegate.getFile(path);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
            acquireOperation();
            try {
                delegate.readFile(path, consumer);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public InputStream readFile(String path) throws Exception {
            throw new UnsupportedOperationException(
                    "Raw readFile streams are not supported; use callback-based readFile");
        }

        @Override
        public boolean isFileExist(String path) throws Exception {
            acquireOperation();
            try {
                return delegate.isFileExist(path);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public boolean move(String sourcePath, String destPath) throws Exception {
            acquireOperation();
            try {
                return delegate.move(sourcePath, destPath);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public boolean delete(String path) throws Exception {
            acquireOperation();
            try {
                return delegate.delete(path);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception {
            acquireOperation();
            try {
                if (validateParentDirectory) {
                    String parentPath = parentPath(path);
                    if (!delegate.isDirectoryExist(parentPath)) {
                        throw new StorageOperationException(
                                "Parent directory does not exist: " + parentPath);
                    }
                }
                return delegate.saveFile(path, is, canReplace);
            } finally {
                operationLock.unlock();
            }
        }

        private static String parentPath(String path) {
            int separator = path.lastIndexOf('/');
            if (separator <= 0) {
                return "/";
            }
            return path.substring(0, separator);
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
            throw new UnsupportedOperationException(
                    "Raw output streams are not supported; use saveFile");
        }

        @Override
        public boolean supportAppendData() {
            acquireOperation();
            try {
                return delegate.supportAppendData();
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                        Collection<String> excludeRegs, boolean recursive, int batchSize,
                                        Consumer<List<TapFile>> consumer) throws Exception {
            acquireOperation();
            try {
                delegate.getFilesInDirectory(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public boolean isDirectoryExist(String path) throws Exception {
            acquireOperation();
            try {
                return delegate.isDirectoryExist(path);
            } finally {
                operationLock.unlock();
            }
        }

        @Override
        public String getConnectInfo() {
            acquireOperation();
            try {
                return delegate.getConnectInfo();
            } finally {
                operationLock.unlock();
            }
        }

        private <T> T withStorageOperation(StorageOperation<T> operation) throws Exception {
            acquireOperation();
            try {
                return operation.apply(this);
            } finally {
                operationLock.unlock();
            }
        }

        private void acquireOperation() {
            if (destroyed.get()) {
                throw new StorageOperationException("File storage is closed");
            }
            operationLock.lock();
            if (destroyed.get()) {
                operationLock.unlock();
                throw new StorageOperationException("File storage is closed");
            }
        }

        private boolean tryAcquireForDestroy() {
            try {
                return operationLock.tryLock(DESTROY_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void destroyAfterAcquire() throws Exception {
            Throwable failure = null;
            try {
                delegate.destroy();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                operationLock.unlock();
                releaseResourcesOnce();
            }
            rethrowDestroyFailure(failure);
        }

        private void destroyWithoutAcquire() throws Exception {
            Throwable failure = null;
            try {
                delegate.destroy();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                releaseResourcesOnce();
            }
            rethrowDestroyFailure(failure);
        }

        private void scheduleForcedDestroy() {
            Thread cleanup = new Thread(() -> {
                try {
                    destroyWithoutAcquire();
                } catch (Throwable throwable) {
                    logWarn(logger, "Forced FTP file storage destroy failed: " + throwable.getMessage());
                }
            }, "pdk-storage-force-destroy-" + nodeResources.getAssociateId());
            cleanup.setDaemon(true);
            cleanup.start();
        }

        private void scheduleDeferredDestroy() {
            Thread cleanup = new Thread(() -> {
                operationLock.lock();
                try {
                    delegate.destroy();
                } catch (Throwable throwable) {
                    logWarn(logger, "Deferred file storage destroy failed: " + throwable.getMessage());
                } finally {
                operationLock.unlock();
                    releaseResourcesOnce();
                }
            }, "pdk-storage-destroy-" + nodeResources.getAssociateId());
            cleanup.setDaemon(true);
            cleanup.start();
            logWarn(logger, "File storage operation did not stop within " + DESTROY_WAIT_MILLIS
                    + "ms; cleanup is deferred until the operation completes");
        }

        private void releaseResourcesOnce() {
            if (resourcesReleased.compareAndSet(false, true)) {
                nodeResources.close();
            }
        }

        private static void rethrowDestroyFailure(Throwable failure) throws Exception {
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure != null) {
                throw new RuntimeException("Destroy file storage failed", failure);
            }
        }
    }
}
