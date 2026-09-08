package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DefaultFileStorageSessionManager implements AutoCloseable {

    @FunctionalInterface
    public interface StorageFactory {
        TapFileStorage create(FileEndpoint endpoint) throws Exception;
    }

    private final StorageFactory storageFactory;
    private final int maxSessions;
    private final long idleTimeoutMs;
    private final Map<SessionKey, SessionEntry> sessions = new HashMap<>();
    private final Map<TapFileStorage, SessionEntry> entriesByStorage = new HashMap<>();
    private boolean closed;

    public DefaultFileStorageSessionManager(StorageFactory storageFactory, int maxSessions, long idleTimeoutMs) {
        if (storageFactory == null) {
            throw new IllegalArgumentException("storageFactory is required");
        }
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("maxSessions must be positive");
        }
        if (idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeoutMs must not be negative");
        }
        this.storageFactory = storageFactory;
        this.maxSessions = maxSessions;
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public synchronized FileStorageSession retain(FileEndpoint endpoint) {
        if (closed) {
            throw new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                    "File storage session manager is closed");
        }
        if (endpoint == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "File endpoint is required");
        }

        long now = System.currentTimeMillis();
        evictIdleSessions(now);
        SessionKey key = SessionKey.of(endpoint);
        SessionEntry entry = sessions.get(key);
        if (entry == null) {
            if (sessions.size() >= maxSessions) {
                throw new FileOperationException(FileOperationErrorCode.FILE_SESSION_LIMIT,
                        "File storage session limit reached");
            }
            TapFileStorage storage;
            try {
                storage = storageFactory.create(endpoint);
                if (storage == null) {
                    throw new IllegalStateException("Storage factory returned null");
                }
            } catch (Exception e) {
                throw new FileOperationException(FileOperationErrorCode.FILE_CONNECT_FAILED,
                        "Create file storage session failed", e);
            }
            entry = new SessionEntry(key, storage, now);
            sessions.put(key, entry);
            entriesByStorage.put(storage, entry);
        }
        entry.references++;
        entry.lastAccessAt = now;
        return new FileStorageSession(endpoint, entry.storage);
    }

    public synchronized void release(FileStorageSession session) {
        if (session == null || !session.markReleased()) {
            return;
        }
        SessionEntry entry = entriesByStorage.get(session.getStorage());
        if (entry == null) {
            return;
        }
        entry.references = Math.max(0, entry.references - 1);
        entry.lastAccessAt = System.currentTimeMillis();
        if (entry.draining && entry.references == 0) {
            sessions.remove(entry.key, entry);
            entriesByStorage.remove(entry.storage, entry);
            destroyQuietly(entry.storage);
        }
    }

    public synchronized void invalidate(FileStorageSession session) {
        if (session == null) {
            return;
        }
        SessionEntry entry = entriesByStorage.get(session.getStorage());
        if (entry == null) {
            return;
        }
        entry.draining = true;
        sessions.remove(entry.key, entry);
        if (entry.references == 0) {
            entriesByStorage.remove(entry.storage, entry);
            destroyQuietly(entry.storage);
        }
    }

    public synchronized int size() {
        return sessions.size();
    }

    private void evictIdleSessions(long now) {
        if (idleTimeoutMs == 0) {
            return;
        }
        List<SessionEntry> expired = new ArrayList<>();
        for (SessionEntry entry : sessions.values()) {
            if (!entry.draining && entry.references == 0 && now - entry.lastAccessAt >= idleTimeoutMs) {
                expired.add(entry);
            }
        }
        for (SessionEntry entry : expired) {
            sessions.remove(entry.key, entry);
            entriesByStorage.remove(entry.storage, entry);
            destroyQuietly(entry.storage);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        List<SessionEntry> entries = new ArrayList<>(sessions.values());
        sessions.clear();
        entriesByStorage.clear();
        for (SessionEntry entry : entries) {
            entry.draining = true;
            destroyQuietly(entry.storage);
        }
    }

    private void destroyQuietly(TapFileStorage storage) {
        try {
            storage.destroy();
        } catch (Throwable ignored) {
            // Session cleanup must continue for other connections.
        }
    }

    private static final class SessionEntry {
        private final SessionKey key;
        private final TapFileStorage storage;
        private long lastAccessAt;
        private int references;
        private boolean draining;

        private SessionEntry(SessionKey key, TapFileStorage storage, long lastAccessAt) {
            this.key = key;
            this.storage = storage;
            this.lastAccessAt = lastAccessAt;
        }
    }

    private static final class SessionKey {
        private final String protocol;
        private final String rootPath;
        private final String paramsFingerprint;

        private SessionKey(String protocol, String rootPath, String paramsFingerprint) {
            this.protocol = protocol;
            this.rootPath = rootPath;
            this.paramsFingerprint = paramsFingerprint;
        }

        private static SessionKey of(FileEndpoint endpoint) {
            return new SessionKey(endpoint.getProtocol(), endpoint.getRootPath(), fingerprint(endpoint.getParams()));
        }

        private static String fingerprint(Map<String, Object> params) {
            StringBuilder canonical = new StringBuilder();
            if (params != null) {
                params.entrySet().stream()
                        .sorted(Comparator.comparing(entry -> String.valueOf(entry.getKey())))
                        .forEach(entry -> canonical.append(String.valueOf(entry.getKey()).toLowerCase())
                                .append('=').append(String.valueOf(entry.getValue())).append(';'));
            }
            try {
                byte[] digest = MessageDigest.getInstance("SHA-256")
                        .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
                StringBuilder result = new StringBuilder(digest.length * 2);
                for (byte value : digest) {
                    result.append(String.format("%02x", value));
                }
                return result.toString();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is not available", e);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof SessionKey)) return false;
            SessionKey that = (SessionKey) other;
            return protocol.equals(that.protocol)
                    && rootPath.equals(that.rootPath)
                    && paramsFingerprint.equals(that.paramsFingerprint);
        }

        @Override
        public int hashCode() {
            int result = protocol.hashCode();
            result = 31 * result + rootPath.hashCode();
            result = 31 * result + paramsFingerprint.hashCode();
            return result;
        }
    }
}
