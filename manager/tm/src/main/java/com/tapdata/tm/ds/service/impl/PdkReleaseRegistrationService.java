package com.tapdata.tm.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
@Setter(onMethod_ = {@Autowired})
public class PdkReleaseRegistrationService {
    static final int MAX_ENTRIES = 10_000;
    static final long MAX_ENTRY_BYTES = 64L * 1024 * 1024;
    static final long MAX_TOTAL_BYTES = 512L * 1024 * 1024;

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
        // Nexus Maven releases are published as ordinary JAR files. The upload
        // CLI encrypts its local copy only after the server accepted the upload;
        // decrypting a Nexus release here would corrupt a valid ZIP/JAR.
        byte[] jarBytes = releaseCatalogService.downloadJar(release);

        String version = String.valueOf(release.get("version"));
        String pdkApiVersion = String.valueOf(release.get("pdkAPIVersion"));
        String fileName = artifactId + "-" + version + ".jar";
        List<MultipartFile> files = new ArrayList<>();
        MultipartFile jar = new ByteArrayMultipartFile("file", fileName, "application/java-archive", jarBytes);
        files.add(jar);
        List<PdkSourceDto> sources = readSources(jarBytes, expectedPdkId, version, pdkApiVersion, files);
        if (sources.isEmpty()) {
            throw new BizException("Released connector artifact does not contain its PDK specification");
        }

        // Inspect current resources after download, immediately before registration.
        if (!stopTasks && !ignoreInFlightResources && hasInFlightResources(expectedPdkId, user)) {
            throw new BizException("Connector resources changed. Review affected tasks before registering.");
        }

        pkdSourceService.uploadPdk(files.toArray(new MultipartFile[0]), sources, true, user, !stopTasks);
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

    private List<PdkSourceDto> readSources(byte[] jarBytes, String expectedPdkId,
                                           String version, String pdkApiVersion, List<MultipartFile> files) {
        List<PdkSourceDto> sources = new ArrayList<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(jarBytes))) {
            ZipEntry entry;
            int entryCount = 0;
            long totalBytes = 0;
            while ((entry = zip.getNextEntry()) != null) {
                if (++entryCount > MAX_ENTRIES) {
                    throw new BizException("Released connector artifact has too many entries");
                }
                if (entry.isDirectory()) {
                    continue;
                }
                byte[] content = readEntry(zip, MAX_TOTAL_BYTES - totalBytes);
                totalBytes += content.length;
                String entryName = entry.getName();

                if (entryName.endsWith(".md")) {
                    files.add(new ByteArrayMultipartFile("file", entryName, "text/markdown", content));
                } else if (entryName.startsWith("icons/")) {
                    files.add(new ByteArrayMultipartFile("file", entryName, "image/*", content));
                }

                if (entryName.endsWith(".json")) {
                    PdkSourceDto source = processJsonEntry(content, expectedPdkId, version, pdkApiVersion);
                    if (source != null) {
                        sources.add(source);
                    }
                }
            }
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException("Unable to read PDK specification from released connector", e);
        }
        return sources;
    }

    private PdkSourceDto processJsonEntry(byte[] content, String expectedPdkId, String version, String pdkApiVersion) {
        JSONObject specification;
        try {
            specification = JSON.parseObject(new String(content, StandardCharsets.UTF_8));
        } catch (Exception ignored) {
            return null;
        }

        JSONObject properties = specification == null ? null : specification.getJSONObject("properties");
        if (properties == null || !expectedPdkId.equals(properties.getString("id"))) {
            return null;
        }

        PdkSourceDto source = new PdkSourceDto();
        source.setId(expectedPdkId);
        source.setName(properties.getString("name"));
        source.setRealName(properties.getString("realName"));
        source.setIcon(properties.getString("icon"));
        source.setGroup("io.tapdata");
        source.setVersion(version);
        source.setPdkAPIVersion(pdkApiVersion);
        source.setPdkAPIBuildNumber(CommonUtils.getPdkBuildNumer(pdkApiVersion));
        source.setType("source_and_target");

        JSONObject manifest = specification.getJSONObject("manifest");
        String authentication = manifest == null ? null : manifest.getString("Authentication");
        DataSourceQCType qcType = DataSourceQCType.parse(authentication);
        source.setQcType(qcType == null ? DataSourceQCType.Alpha : qcType);
        source.setAuthentication(authentication);
        source.setTags(JSON.parseArray(JSON.toJSONString(properties.get("tags")), String.class));
        source.setConfigOptions(asLinkedMap(specification.getJSONObject("configOptions")));
        source.setMessages(asLinkedMap(specification.getJSONObject("messages")));
        source.setExpression(JSON.toJSONString(specification.get("dataTypes")));

        List<Capability> capabilities = new ArrayList<>();
        JSONObject configOptions = specification.getJSONObject("configOptions");
        if (configOptions != null && configOptions.getJSONArray("capabilities") != null) {
            for (Object capability : configOptions.getJSONArray("capabilities")) {
                JSONObject capabilityObject = (JSONObject) capability;
                String id = capabilityObject.getString("id");
                if (id != null) {
                    capabilities.add(Capability.create(id).type(Capability.TYPE_OTHER));
                }
            }
        }
        source.setCapabilities(capabilities);
        return source;
    }

    private byte[] readEntry(InputStream inputStream, long remainingBytes) throws IOException {
        // Read one extra byte to detect overflow without draining an unbounded ZIP entry.
        long limit = Math.min(MAX_ENTRY_BYTES, remainingBytes);
        byte[] content = inputStream.readNBytes(Math.toIntExact(limit + 1));
        if (content.length > limit) {
            throw new BizException(remainingBytes < MAX_ENTRY_BYTES
                    ? "Released connector artifact is too large"
                    : "Released connector artifact entry is too large");
        }
        return content;
    }

    private LinkedHashMap<String, Object> asLinkedMap(Map<String, Object> value) {
        return value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
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
