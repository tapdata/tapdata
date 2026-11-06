package com.tapdata.tm.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import jakarta.annotation.PreDestroy;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.registration.RegistrationArchive;
import io.tapdata.pdk.registration.RegistrationMetadata;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@Setter(onMethod_ = {@Autowired})
public class PdkReleaseRegistrationService {
    private PdkReleaseCatalogService releaseCatalogService;
    private PkdSourceService pkdSourceService;
    private SettingsService settingsService;
    private MongoTemplate mongoTemplate;
    private static final String REGISTRATION_JOB_COLLECTION = "pdkRegistrationJobs";
    private final ExecutorService registrationExecutor;
    private final ScheduledExecutorService registrationHeartbeatExecutor =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pdk-release-registration-heartbeat");
                thread.setDaemon(true);
                return thread;
            });

    public PdkReleaseRegistrationService() {
        this(Executors.newFixedThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "pdk-release-registration");
            thread.setDaemon(true);
            return thread;
        }));
    }

    PdkReleaseRegistrationService(ExecutorService registrationExecutor) {
        this.registrationExecutor = registrationExecutor;
    }

    public Map<String, Object> startRegistration(String artifactId, String expectedPdkId, boolean stopTasks,
                                                boolean ignoreInFlightResources, UserDetail user) {
        requireEnterprise(user);
        Map<String, Object> release = releaseCatalogService.findRelease(artifactId);
        if (!expectedPdkId.equals(release.get("pdkId"))) {
            throw new BizException("Connector release does not match the requested PDK id");
        }
        if (!stopTasks && !ignoreInFlightResources && hasInFlightResources(expectedPdkId, user)) {
            throw new BizException("Connector resources changed. Review affected tasks before registering.");
        }
        purgeExpiredJobs();
        String jobId = UUID.randomUUID().toString();
        RegistrationJob job = new RegistrationJob(jobId, expectedPdkId, user.getUserId());
        persistJob(job);
        ScheduledFuture<?> heartbeat = registrationHeartbeatExecutor.scheduleAtFixedRate(
                () -> heartbeatJob(job), 30, 30, TimeUnit.SECONDS);
        try {
            registrationExecutor.execute(() -> {
                try {
                    downloadAndRegister(artifactId, expectedPdkId, stopTasks, ignoreInFlightResources, user,
                            job);
                    updateJob(job, "completed", 100, null);
                } catch (Exception e) {
                    String message = e.getMessage() == null ? "Connector registration failed" : e.getMessage();
                    String errorCode = e instanceof RegistrationImpactChangedException
                            ? RegistrationImpactChangedException.ERROR_CODE : null;
                    updateJob(job, "failed", job.progress, message, errorCode);
                } finally {
                    heartbeat.cancel(false);
                }
            });
        } catch (RuntimeException e) {
            heartbeat.cancel(false);
            updateJob(job, "failed", job.progress, "Connector registration could not be queued");
            throw e;
        }
        return job.toMap();
    }

    public Map<String, Object> getRegistrationJob(String jobId, UserDetail user) {
        org.bson.Document stored = mongoTemplate.findById(jobId, org.bson.Document.class,
                REGISTRATION_JOB_COLLECTION);
        if (stored == null || !user.getUserId().equals(stored.getString("ownerId"))) {
            throw new BizException("Connector registration job was not found");
        }
        RegistrationJob job = new RegistrationJob(stored);
        if (!job.isTerminal() && job.updatedAt < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(3)) {
            updateJob(job, "failed", job.progress, "Connector registration was interrupted");
        }
        return job.toMap();
    }

    @PreDestroy
    public void shutdownRegistrationExecutor() {
        registrationExecutor.shutdown();
        registrationHeartbeatExecutor.shutdownNow();
    }

    public Map<String, Object> registrationImpact(String pdkId, UserDetail user) {
        requireEnterprise(user);
        return pkdSourceService.registrationImpact(pdkId, "io.tapdata", user);
    }

    public void downloadAndRegister(String artifactId, String expectedPdkId, boolean stopTasks,
                                    boolean ignoreInFlightResources, UserDetail user) {
        downloadAndRegister(artifactId, expectedPdkId, stopTasks, ignoreInFlightResources, user, null);
    }

    private void downloadAndRegister(String artifactId, String expectedPdkId, boolean stopTasks,
                                     boolean ignoreInFlightResources, UserDetail user, RegistrationJob job) {
        requireEnterprise(user);
        updateJobIfTracked(job, "preparing", 5, null);
        Map<String, Object> release = releaseCatalogService.findRelease(artifactId);
        if (!expectedPdkId.equals(release.get("pdkId"))) {
            throw new BizException("Connector release does not match the requested PDK id");
        }
        updateJobIfTracked(job, "downloading", 10, null);
        byte[] jarBytes = releaseCatalogService.downloadJar(release,
                percent -> updateProgressIfTracked(job, 10 + percent * 6 / 10));

        String version = String.valueOf(release.get("version"));
        String fileName = artifactId + "-" + version + ".jar";
        updateJobIfTracked(job, "preparing", 72, null);
        List<MultipartFile> files = new ArrayList<>();
        List<PdkSourceDto> sources = readSources(jarBytes, fileName, expectedPdkId, files);
        if (sources.isEmpty()) {
            throw new BizException("Released connector artifact does not contain its PDK specification");
        }

        // Inspect current resources after download, immediately before registration.
        if (!stopTasks && !ignoreInFlightResources && hasInFlightResources(expectedPdkId, user)) {
            if (job == null) {
                throw new BizException("Connector resources changed. Review affected tasks before registering.");
            }
            throw new RegistrationImpactChangedException(
                    "Connector resources changed. Review affected tasks before registering.");
        }

        updateJobIfTracked(job, "registering", 80, null);
        pkdSourceService.uploadReleasedPdk(files.toArray(new MultipartFile[0]), sources, user, !stopTasks);
    }

    private void updateJobIfTracked(RegistrationJob job, String status, int progress, String error) {
        if (job != null) updateJob(job, status, progress, error);
    }

    private void updateProgressIfTracked(RegistrationJob job, int progress) {
        if (job != null) updateProgress(job, progress);
    }

    private void purgeExpiredJobs() {
        long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        mongoTemplate.remove(Query.query(new Criteria().andOperator(
                Criteria.where("updatedAt").lt(cutoff),
                Criteria.where("status").in("completed", "failed"))), REGISTRATION_JOB_COLLECTION);
    }

    private void updateProgress(RegistrationJob job, int progress) {
        if (progress - job.progress >= 1) updateJob(job, "downloading", progress, null);
    }

    private void updateJob(RegistrationJob job, String status, int progress, String error) {
        updateJob(job, status, progress, error, null);
    }

    private void updateJob(RegistrationJob job, String status, int progress, String error, String errorCode) {
        synchronized (job.lock) {
            job.update(status, progress, error, errorCode);
            persistJob(job);
        }
    }

    private void heartbeatJob(RegistrationJob job) {
        synchronized (job.lock) {
            if (!job.isTerminal()) {
                job.touch();
                persistJob(job);
            }
        }
    }

    private void persistJob(RegistrationJob job) {
        mongoTemplate.save(job.toDocument(), REGISTRATION_JOB_COLLECTION);
    }

    private static final class RegistrationJob {
        private final String id;
        private final String pdkId;
        private final String ownerId;
        private final Object lock = new Object();
        private volatile String status = "queued";
        private volatile int progress = 1;
        private volatile String error;
        private volatile String errorCode;
        private volatile long updatedAt = System.currentTimeMillis();

        private RegistrationJob(org.bson.Document document) {
            this.id = document.getString("_id");
            this.pdkId = document.getString("pdkId");
            this.ownerId = document.getString("ownerId");
            this.status = document.getString("status");
            this.progress = document.getInteger("progress", 1);
            this.error = document.getString("error");
            this.errorCode = document.getString("errorCode");
            this.updatedAt = document.getLong("updatedAt");
        }

        private RegistrationJob(String id, String pdkId, String ownerId) {
            this.id = id;
            this.pdkId = pdkId;
            this.ownerId = ownerId;
        }

        private boolean isTerminal() {
            return "completed".equals(status) || "failed".equals(status);
        }

        private void update(String status, int progress, String error, String errorCode) {
            this.status = status;
            this.progress = progress;
            this.error = error;
            this.errorCode = errorCode;
            this.updatedAt = System.currentTimeMillis();
        }

        private void touch() {
            this.updatedAt = System.currentTimeMillis();
        }

        private org.bson.Document toDocument() {
            org.bson.Document document = new org.bson.Document("_id", id)
                    .append("pdkId", pdkId)
                    .append("ownerId", ownerId)
                    .append("status", status)
                    .append("progress", progress)
                    .append("updatedAt", updatedAt);
            if (error != null) document.append("error", error);
            if (errorCode != null) document.append("errorCode", errorCode);
            return document;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("jobId", id);
            result.put("pdkId", pdkId);
            result.put("status", status);
            result.put("progress", progress);
            result.put("error", error);
            result.put("errorCode", errorCode);
            return result;
        }
    }

    private static final class RegistrationImpactChangedException extends RuntimeException {
        private static final String ERROR_CODE = "RESOURCE_IMPACT_CHANGED";

        private RegistrationImpactChangedException(String message) {
            super(message);
        }
    }

    private void requireEnterprise(UserDetail user) {
        if (settingsService.isCloud() || user == null || user.isFreeAuth()) {
            throw new BizException("Connector download and registration is available only in the enterprise edition");
        }
    }

    private boolean hasInFlightResources(String pdkId, UserDetail user) {
        Map<String, Object> impact = registrationImpact(pdkId, user);
        return Boolean.TRUE.equals(impact.get("hasInFlightResources"));
    }

    private List<PdkSourceDto> readSources(byte[] jarBytes, String fileName, String expectedPdkId, List<MultipartFile> files) {
        Path localJar = null;
        try (RegistrationArchive archive = new RegistrationArchive()) {
            localJar = createPrivateTempJar();
            Files.write(localJar, jarBytes);
            List<PdkSourceDto> sources = new ArrayList<>();
            Map<String, MultipartFile> resources = new LinkedHashMap<>();
            for (TapNodeInfo nodeInfo : archive.load(localJar.toFile())) {
                if (!expectedPdkId.equals(nodeInfo.getTapNodeSpecification().getId())) {
                    continue;
                }
                String icon = nodeInfo.getTapNodeSpecification().getIcon();
                if (icon != null && !icon.trim().isEmpty()) {
                    addResource(nodeInfo, icon, "image/*", resources);
                }
                sources.add(JSON.toJavaObject(RegistrationMetadata.build(nodeInfo,
                        path -> addResource(nodeInfo, path, "text/markdown", resources)), PdkSourceDto.class));
            }
            // The PDK loader may decrypt its local file, as it does before a CLI upload.
            files.add(new ByteArrayMultipartFile("file", fileName, "application/java-archive", Files.readAllBytes(localJar)));
            files.addAll(resources.values());
            return sources;
        } catch (Exception e) {
            throw new BizException("Unable to read PDK specification from released connector", e);
        } finally {
            if (localJar != null) {
                try {
                    Files.deleteIfExists(localJar);
                } catch (IOException e) {
                    localJar.toFile().deleteOnExit();
                }
            }
        }
    }

    private static Path createPrivateTempJar() throws IOException {
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            throw new IOException("Secure temporary connector files require POSIX file permissions");
        }
        return Files.createTempFile("pdk-registration-", ".jar",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    }

    private void addResource(TapNodeInfo nodeInfo, String path, String contentType,
                             Map<String, MultipartFile> resources) {
        if (resources.containsKey(path)) return;
        try (InputStream input = nodeInfo.readResource(path)) {
            if (input != null) {
                resources.put(path, new ByteArrayMultipartFile("file", path, contentType, input.readAllBytes()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class ByteArrayMultipartFile implements MultipartFile {
        private final String name;
        private final String filename;
        private final String contentType;
        private final byte[] bytes;

        private ByteArrayMultipartFile(String name, String filename, String contentType, byte[] bytes) {
            this.name = name;
            this.filename = filename;
            this.contentType = contentType;
            this.bytes = bytes;
        }

        @Override public String getName() { return name; }
        @Override public String getOriginalFilename() { return filename; }
        @Override public String getContentType() { return contentType; }
        @Override public boolean isEmpty() { return bytes.length == 0; }
        @Override public long getSize() { return bytes.length; }
        @Override public byte[] getBytes() { return bytes; }
        @Override public InputStream getInputStream() { return new ByteArrayInputStream(bytes); }
        @Override public void transferTo(File dest) throws IllegalStateException {
            try {
                java.nio.file.Files.write(dest.toPath(), bytes);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
