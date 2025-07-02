package com.tapdata.tm.sdkVersion.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.entity.SdkVersionEntity;
import com.tapdata.tm.sdkVersion.repository.SdkVersionRepository;
import com.tapdata.tm.sdkModule.service.SdkModuleService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2025/07/02
 * @Description:
 */
@Service
@Slf4j
public class SdkVersionService extends BaseService<SdkVersionDto, SdkVersionEntity, ObjectId, SdkVersionRepository> {

    @Autowired
    private SdkModuleService sdkModuleService;

    @Autowired
    private FileService fileService;

    public SdkVersionService(@NonNull SdkVersionRepository repository) {
        super(repository, SdkVersionDto.class, SdkVersionEntity.class);
    }

    protected void beforeSave(SdkVersionDto sdkVersion, UserDetail user) {

    }

    /**
     * Delete SDK version by ID with cascade deletion of associated modules and GridFS files
     *
     * @param id SDK version ID
     * @param userDetail user detail
     * @return true if deletion was successful
     */
    @Override
    public boolean deleteById(ObjectId id, UserDetail userDetail) {
        log.info("Starting cascade deletion for SDK version with ID: {}", id);

        try {
            // 1. Get SDK version information before deletion to retrieve GridFS file IDs
            SdkVersionDto sdkVersion = findById(id, userDetail);
            if (sdkVersion == null) {
                log.warn("SDK version with ID {} not found", id);
                return false;
            }

            // 2. Delete associated SDK modules
            deleteAssociatedSdkModules(id.toString(), userDetail);

            // 3. Delete GridFS files (ZIP and JAR)
            deleteGridFSFiles(sdkVersion);

            // 4. Delete the SDK version record itself
            boolean deleted = super.deleteById(id, userDetail);

            if (deleted) {
                log.info("Successfully completed cascade deletion for SDK version with ID: {}", id);
            } else {
                log.error("Failed to delete SDK version record with ID: {}", id);
            }

            return deleted;

        } catch (Exception e) {
            log.error("Error during cascade deletion of SDK version with ID: {}", id, e);
            throw new RuntimeException("Failed to delete SDK version with cascade deletion", e);
        }
    }

    /**
     * Delete all SDK modules associated with the given SDK version ID
     *
     * @param sdkVersionId SDK version ID
     * @param userDetail user detail
     */
    private void deleteAssociatedSdkModules(String sdkVersionId, UserDetail userDetail) {
        log.info("Deleting SDK modules associated with SDK version ID: {}", sdkVersionId);

        try {
            // Find all SDK modules with the given sdkVersionId
            Query query = new Query(Criteria.where("sdkVersionId").is(sdkVersionId));
            sdkModuleService.deleteAll(query, userDetail);

            log.info("Deleted SDK modules associated with SDK version ID: {}", sdkVersionId);

        } catch (Exception e) {
            log.error("Error deleting SDK modules for SDK version ID: {}", sdkVersionId, e);
            // Continue with deletion process even if module deletion fails
        }
    }

    /**
     * Delete GridFS files (ZIP and JAR) associated with the SDK version
     *
     * @param sdkVersion SDK version DTO containing file IDs
     */
    private void deleteGridFSFiles(SdkVersionDto sdkVersion) {
        log.info("Deleting GridFS files for SDK version: {}", sdkVersion.getId());

        // Delete ZIP file if exists
        if (StringUtils.isNotBlank(sdkVersion.getZipGridfsId())) {
            try {
                ObjectId zipFileId = new ObjectId(sdkVersion.getZipGridfsId());
                fileService.deleteFileById(zipFileId);
                log.info("Deleted ZIP file with GridFS ID: {}", sdkVersion.getZipGridfsId());
            } catch (Exception e) {
                log.error("Error deleting ZIP file with GridFS ID: {}", sdkVersion.getZipGridfsId(), e);
                // Continue with deletion process even if file deletion fails
            }
        }

        // Delete JAR file if exists
        if (StringUtils.isNotBlank(sdkVersion.getJarGridfsId())) {
            try {
                ObjectId jarFileId = new ObjectId(sdkVersion.getJarGridfsId());
                fileService.deleteFileById(jarFileId);
                log.info("Deleted JAR file with GridFS ID: {}", sdkVersion.getJarGridfsId());
            } catch (Exception e) {
                log.error("Error deleting JAR file with GridFS ID: {}", sdkVersion.getJarGridfsId(), e);
                // Continue with deletion process even if file deletion fails
            }
        }
    }
}