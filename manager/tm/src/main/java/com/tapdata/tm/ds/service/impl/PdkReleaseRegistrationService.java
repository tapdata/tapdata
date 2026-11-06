package com.tapdata.tm.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.registration.RegistrationArchive;
import io.tapdata.pdk.registration.RegistrationMetadata;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
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

@Service
@Setter(onMethod_ = {@Autowired})
public class PdkReleaseRegistrationService {
    private PdkReleaseCatalogService releaseCatalogService;
    private PkdSourceService pkdSourceService;
    private SettingsService settingsService;

    public Map<String, Object> registrationImpact(String pdkId, UserDetail user) {
        requireEnterprise(user);
        return pkdSourceService.registrationImpact(pdkId, "io.tapdata", user);
    }

    public void downloadAndRegister(String artifactId, String expectedPdkId, boolean stopTasks,
                                    boolean ignoreInFlightResources, UserDetail user) {
        requireEnterprise(user);
        Map<String, Object> release = releaseCatalogService.findRelease(artifactId);
        if (!expectedPdkId.equals(release.get("pdkId"))) {
            throw new BizException("Connector release does not match the requested PDK id");
        }
        byte[] jarBytes = releaseCatalogService.downloadJar(release);

        String version = String.valueOf(release.get("version"));
        String fileName = artifactId + "-" + version + ".jar";
        List<MultipartFile> files = new ArrayList<>();
        List<PdkSourceDto> sources = readSources(jarBytes, fileName, expectedPdkId, files);
        if (sources.isEmpty()) {
            throw new BizException("Released connector artifact does not contain its PDK specification");
        }

        // Inspect current resources after download, immediately before registration.
        if (!stopTasks && !ignoreInFlightResources && hasInFlightResources(expectedPdkId, user)) {
            throw new BizException("Connector resources changed. Review affected tasks before registering.");
        }

        pkdSourceService.uploadReleasedPdk(files.toArray(new MultipartFile[0]), sources, user, !stopTasks);
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
