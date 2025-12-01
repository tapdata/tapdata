package com.tapdata.tm.sdkVersion.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.entity.SdkVersionEntity;
import com.tapdata.tm.sdkVersion.repository.SdkVersionRepository;
import com.tapdata.tm.sdkModule.service.SdkModuleService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.sdk.dto.SDKDto;
import com.tapdata.tm.sdk.service.SDKService;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

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

    @Autowired
    @Lazy
    private SDKService sdkService;

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

                // 5. Update SDK's last fields after successful deletion
                updateSdkLastFields(sdkVersion.getSdkId(), userDetail);
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

    /**
     * Update SDK's last fields based on the latest remaining version after deletion
     *
     * @param sdkId SDK ID
     * @param userDetail user detail
     */
    private void updateSdkLastFields(String sdkId, UserDetail userDetail) {
        log.info("Updating SDK last fields for SDK ID: {}", sdkId);

        try {
            // Find the latest remaining version for this SDK (sorted by creation time descending)
            Query query = new Query(Criteria.where("sdkId").is(sdkId))
                    .with(Sort.by(Sort.Direction.DESC, "createAt"))
                    .limit(1);

            List<SdkVersionDto> remainingVersions = findAllDto(query, userDetail);

            // Get the SDK record
            SDKDto sdk = sdkService.findById(new ObjectId(sdkId), userDetail);
            if (sdk == null) {
                log.warn("SDK with ID {} not found when updating last fields", sdkId);
                return;
            }

            if (remainingVersions.isEmpty()) {
                // No versions remain, clear all last fields
                clearSdkLastFields(sdk, userDetail);
                log.info("Cleared all last fields for SDK ID: {} (no versions remaining)", sdkId);
            } else {
                // Update last fields with the latest remaining version
                SdkVersionDto latestVersion = remainingVersions.get(0);
                updateSdkLastFieldsFromVersion(sdk, latestVersion, userDetail);
                log.info("Updated last fields for SDK ID: {} with latest version: {}", sdkId, latestVersion.getVersion());
            }

        } catch (Exception e) {
            log.error("Error updating SDK last fields for SDK ID: {}", sdkId, e);
            // Don't throw exception to avoid affecting the deletion process
        }
    }

    /**
     * Clear all last fields in SDK when no versions remain
     *
     * @param sdk SDK DTO
     * @param userDetail user detail
     */
    private void clearSdkLastFields(SDKDto sdk, UserDetail userDetail) {
        sdk.setLastGeneratedVersion(null);
        sdk.setLastGenerationTime(null);
        sdk.setLastGenerateStatus(null);
        sdk.setLastZipGridfsId(null);
        sdk.setLastZipSizeOfByte(null);
        sdk.setLastJarGridfsId(null);
        sdk.setLastJarSizeOfByte(null);
        sdk.setLastJarGenerationErrorMessage(null);
        sdk.setGenerationErrorMessage(null);
        sdk.setLastModuleIds(null);

        sdkService.save(sdk, userDetail);
    }

    /**
     * Update SDK's last fields from the latest version
     *
     * @param sdk SDK DTO
     * @param latestVersion latest version DTO
     * @param userDetail user detail
     */
    private void updateSdkLastFieldsFromVersion(SDKDto sdk, SdkVersionDto latestVersion, UserDetail userDetail) {
        sdk.setLastGeneratedVersion(latestVersion.getVersion());
        sdk.setLastGenerationTime(latestVersion.getCreateAt());
        sdk.setLastGenerateStatus(latestVersion.getGenerateStatus());
        sdk.setLastZipGridfsId(latestVersion.getZipGridfsId());
        sdk.setLastZipSizeOfByte(latestVersion.getZipSizeOfByte());
        sdk.setLastJarGridfsId(latestVersion.getJarGridfsId());
        sdk.setLastJarSizeOfByte(latestVersion.getJarSizeOfByte());
        sdk.setLastJarGenerationErrorMessage(latestVersion.getJarGenerationErrorMessage());
        sdk.setGenerationErrorMessage(latestVersion.getGenerationErrorMessage());
        sdk.setLastModuleIds(latestVersion.getModuleIds());

        sdkService.save(sdk, userDetail);
    }
}