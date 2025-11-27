package com.tapdata.tm.openapi.generator.util;

import com.tapdata.tm.file.service.FileService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * GridFS upload utility for uploading files to MongoDB GridFS
 *
 * @author sam
 * @date 2024/12/19
 */
@Slf4j
public class GridFSUploadUtil {

    /**
     * Upload file to GridFS
     *
     * @param fileService FileService instance for GridFS operations
     * @param filePath    Path to the file to upload
     * @param filename    Filename to use in GridFS
     * @param contentType Content type of the file
     * @param metadata    Additional metadata to store with the file
     * @return UploadResult containing success status, GridFS ID, and error message
     */
    public static UploadResult uploadFile(FileService fileService, Path filePath, String filename, 
                                        String contentType, Map<String, Object> metadata) {
        log.info("Starting GridFS upload for file: {} as filename: {}", filePath, filename);

        try {
            // Validate file exists
            if (!Files.exists(filePath)) {
                return UploadResult.failure("File does not exist: " + filePath);
            }

            if (!Files.isRegularFile(filePath)) {
                return UploadResult.failure("Path is not a regular file: " + filePath);
            }

            // Get file size
            long fileSize = Files.size(filePath);
            log.info("File size: {} bytes", fileSize);

            // Prepare metadata
            Map<String, Object> fileMetadata = new HashMap<>();
            if (metadata != null) {
                fileMetadata.putAll(metadata);
            }
            fileMetadata.put("originalPath", filePath.toString());
            fileMetadata.put("uploadTime", System.currentTimeMillis());
            fileMetadata.put("fileSize", fileSize);

            // Upload file to GridFS
            try (FileInputStream inputStream = new FileInputStream(filePath.toFile())) {
                ObjectId gridfsId = fileService.storeFile(inputStream, filename, contentType, fileMetadata);
                
                log.info("Successfully uploaded file to GridFS with ID: {}", gridfsId);
                return UploadResult.success(gridfsId, fileSize);
            }

        } catch (IOException e) {
            log.error("IOException during GridFS upload for file: {}", filePath, e);
            return UploadResult.failure("IOException during upload: " + e.getMessage());
        } catch (Exception e) {
            log.error("Exception during GridFS upload for file: {}", filePath, e);
            return UploadResult.failure("Exception during upload: " + e.getMessage());
        }
    }

    /**
     * Upload ZIP file to GridFS with appropriate metadata
     */
    public static UploadResult uploadZipFile(FileService fileService, Path zipPath, String artifactId, String version) {
        String filename = String.format("%s-%s-sources.zip", artifactId, version);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "sdk-sources");
        metadata.put("artifactId", artifactId);
        metadata.put("version", version);
        metadata.put("format", "zip");

        return uploadFile(fileService, zipPath, filename, "application/zip", metadata);
    }

    /**
     * Upload JAR file to GridFS with appropriate metadata
     */
    public static UploadResult uploadJarFile(FileService fileService, Path jarPath, String artifactId, String version) {
        String filename = String.format("%s-%s.jar", artifactId, version);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "sdk-jar");
        metadata.put("artifactId", artifactId);
        metadata.put("version", version);
        metadata.put("format", "jar");

        return uploadFile(fileService, jarPath, filename, "application/java-archive", metadata);
    }

    /**
     * Result of GridFS upload operation
     */
    public static class UploadResult {
        private final boolean success;
        private final ObjectId gridfsId;
        private final long fileSize;
        private final String errorMessage;

        private UploadResult(boolean success, ObjectId gridfsId, long fileSize, String errorMessage) {
            this.success = success;
            this.gridfsId = gridfsId;
            this.fileSize = fileSize;
            this.errorMessage = errorMessage;
        }

        public static UploadResult success(ObjectId gridfsId, long fileSize) {
            return new UploadResult(true, gridfsId, fileSize, null);
        }

        public static UploadResult failure(String errorMessage) {
            return new UploadResult(false, null, 0, errorMessage);
        }

        public boolean isSuccess() {
            return success;
        }

        public ObjectId getGridfsId() {
            return gridfsId;
        }

        public long getFileSize() {
            return fileSize;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}
