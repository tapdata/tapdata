package com.tapdata.tm.sdk.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.sdk.dto.SDKDto;
import com.tapdata.tm.sdk.entity.SDKEntity;
import com.tapdata.tm.sdk.repository.SDKRepository;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.service.SdkVersionService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:
 * @Date: 2025/07/01
 * @Description:
 */
@Service
@Slf4j
public class SDKService extends BaseService<SDKDto, SDKEntity, ObjectId, SDKRepository> {

	private final SdkVersionService sdkVersionService;

	public SDKService(@NonNull SDKRepository repository, SdkVersionService sdkVersionService) {
		super(repository, SDKDto.class, SDKEntity.class);
		this.sdkVersionService = sdkVersionService;
	}

	protected void beforeSave(SDKDto sDK, UserDetail user) {

	}

	/**
	 * Delete SDK by ID with cascade deletion of associated versions and modules
	 *
	 * @param id         SDK ID
	 * @param userDetail user detail
	 * @return true if deletion was successful
	 */
	@Override
	public boolean deleteById(ObjectId id, UserDetail userDetail) {
		log.info("Starting cascade deletion for SDK with ID: {}", id);

		try {
			// 1. Check if SDK exists
			SDKDto sdk = findById(id, userDetail);
			if (sdk == null) {
				log.warn("SDK with ID {} not found", id);
				return false;
			}

			String sdkId = id.toString();

			// 2. Delete all associated SDK versions (which will cascade delete modules and files)
			deleteAssociatedSdkVersions(sdkId, userDetail);

			// 3. Delete the SDK record itself
			boolean deleted = super.deleteById(id, userDetail);

			if (deleted) {
				log.info("Successfully completed cascade deletion for SDK with ID: {}", id);
			} else {
				log.error("Failed to delete SDK record with ID: {}", id);
			}

			return deleted;

		} catch (Exception e) {
			log.error("Error during cascade deletion of SDK with ID: {}", id, e);
			throw new RuntimeException("Failed to delete SDK with cascade deletion", e);
		}
	}

	/**
	 * Delete all SDK versions associated with the given SDK ID
	 * This will trigger cascade deletion of modules and GridFS files for each version
	 *
	 * @param sdkId      SDK ID
	 * @param userDetail user detail
	 */
	private void deleteAssociatedSdkVersions(String sdkId, UserDetail userDetail) {
		log.info("Deleting SDK versions associated with SDK ID: {}", sdkId);

		try {
			// Find all SDK versions with the given sdkId
			Query query = new Query(Criteria.where("sdkId").is(sdkId));
			List<SdkVersionDto> sdkVersions = sdkVersionService.findAllDto(query, userDetail);

			if (CollectionUtils.isEmpty(sdkVersions)) {
				log.info("No SDK versions found for SDK ID: {}", sdkId);
				return;
			}

			log.info("Found {} SDK versions to delete for SDK ID: {}", sdkVersions.size(), sdkId);

			// Delete each version (this will trigger cascade deletion in SdkVersionService)
			for (SdkVersionDto version : sdkVersions) {
				try {
					boolean deleted = sdkVersionService.deleteById(version.getId(), userDetail);
					if (deleted) {
						log.debug("Successfully deleted SDK version with ID: {} for SDK ID: {}",
								version.getId(), sdkId);
					} else {
						log.warn("Failed to delete SDK version with ID: {} for SDK ID: {}",
								version.getId(), sdkId);
					}
				} catch (Exception e) {
					log.error("Error deleting SDK version with ID: {} for SDK ID: {}",
							version.getId(), sdkId, e);
					// Continue with other versions even if one fails
				}
			}

			log.info("Completed deletion of SDK versions for SDK ID: {}", sdkId);

		} catch (Exception e) {
			log.error("Error deleting SDK versions for SDK ID: {}", sdkId, e);
			// Continue with deletion process even if version deletion fails
		}
	}


	private void afterFind(Page<SDKDto> page) {
		List<SDKDto> items = page.getItems();
		if (CollectionUtils.isEmpty(items)) {
			return;
		}
	}
}