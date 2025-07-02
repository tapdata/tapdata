package com.tapdata.tm.openapi.generator.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.sdk.dto.SDKDto;
import com.tapdata.tm.sdk.service.GenerateStatus;
import com.tapdata.tm.sdk.service.SDKService;
import com.tapdata.tm.sdkModule.dto.SdkModuleDto;
import com.tapdata.tm.sdkModule.service.SdkModuleService;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.service.SdkVersionService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Async code generation service for handling SDK generation tasks
 *
 * @author sam
 * @date 2024/12/19
 */
@Service
@Slf4j
public class AsyncCodeGenerationService {

	private final SDKService sdkService;
	private final SdkVersionService sdkVersionService;
	private final SdkModuleService sdkModuleService;
	private final ModulesService modulesService;
	private final OpenApiGeneratorService openApiGeneratorService;
	private final ApplicationContext applicationContext;

	public AsyncCodeGenerationService(SDKService sdkService,
									  SdkVersionService sdkVersionService,
									  SdkModuleService sdkModuleService,
									  ModulesService modulesService, OpenApiGeneratorService openApiGeneratorService, ApplicationContext applicationContext) {
		this.sdkService = sdkService;
		this.sdkVersionService = sdkVersionService;
		this.sdkModuleService = sdkModuleService;
		this.modulesService = modulesService;
		this.openApiGeneratorService = openApiGeneratorService;
		this.applicationContext = applicationContext;
	}

	public void generateCode(CodeGenerationRequest request, UserDetail userDetail) {
		log.info("Starting async code generation for artifactId: {}, version: {}",
				request.getArtifactId(), request.getVersion());
		beforeGenerate(request, userDetail);
		SDKDto sdkDto = createOrUpdateSdkRecords(request, userDetail);
		// Get the version record for status updates
		SdkVersionDto versionDto = getVersionRecord(sdkDto.getId().toString(), request.getVersion(), userDetail);

		// Use Spring proxy to call async method to ensure @Async annotation works
		AsyncCodeGenerationService self = applicationContext.getBean(AsyncCodeGenerationService.class);
		self.generateCodeAsync(request, userDetail, sdkDto, versionDto);

		log.info("Async code generation task submitted successfully for artifactId: {}", request.getArtifactId());
	}

	@Async
	public void generateCodeAsync(CodeGenerationRequest request, UserDetail userDetail, SDKDto sdkDto, SdkVersionDto versionDto) {
		try {
			// Create module records
			createModuleRecords(sdkDto.getId().toString(), versionDto.getId().toString(), request.getModuleIds(), userDetail);

			// Use enhanced code generation with ZIP and JAR creation
			OpenApiGeneratorService.EnhancedGenerationResult result = openApiGeneratorService.generateCodeEnhanced(request);

			if (result.isSuccess()) {
				// Success: Update both SDK and version status to GENERATED with GridFS IDs
				updateSdkGenerationStatusWithFiles(sdkDto.getId().toString(), GenerateStatus.GENERATED, null,
						result.getZipGridfsId(), result.getZipSize(),
						result.getJarGridfsId(), result.getJarSize(), result.getJarError(), userDetail);
				updateSdkVersionGenerationStatusWithFiles(versionDto.getId().toString(), GenerateStatus.GENERATED, null,
						result.getZipGridfsId(), result.getZipSize(),
						result.getJarGridfsId(), result.getJarSize(), result.getJarError(), userDetail);
				log.info("Async enhanced code generation completed successfully for artifactId: {}, version: {}, ZIP ID: {}, JAR ID: {}",
						request.getArtifactId(), request.getVersion(), result.getZipGridfsId(), result.getJarGridfsId());
			} else {
				// Failure: Update both SDK and version status to FAILED
				String errorMessage = "Enhanced code generation failed: " + result.getErrorMessage();
				updateSdkGenerationStatus(sdkDto.getId().toString(), GenerateStatus.FAILED, errorMessage, userDetail);
				updateSdkVersionGenerationStatus(versionDto.getId().toString(), GenerateStatus.FAILED, errorMessage, userDetail);
				log.error("Async enhanced code generation failed for artifactId: {}, version: {}, error: {}",
						request.getArtifactId(), request.getVersion(), result.getErrorMessage());
			}

		} catch (Exception e) {
			log.error("Async code generation failed for artifactId: {}, version: {}", request.getArtifactId(), request.getVersion(), e);

			// Update both SDK and version status to FAILED with error message
			try {
				// Update status using the passed parameters (more reliable)
				updateSdkGenerationStatus(sdkDto.getId().toString(), GenerateStatus.FAILED, e.getMessage(), userDetail);
				updateSdkVersionGenerationStatus(versionDto.getId().toString(), GenerateStatus.FAILED, e.getMessage(), userDetail);
			} catch (Exception updateException) {
				log.error("Failed to update SDK and version status to FAILED", updateException);
			}
		}
	}

	private void beforeGenerate(CodeGenerationRequest request, UserDetail userDetail) {
		// Check for re-entry prevention - ensure no other version is currently generating for the same artifactId
		checkGenerationReentry(request.getArtifactId(), userDetail);
		// Check if version already exists for this SDK
		SdkVersionDto existingVersion = checkVersionExists(request, userDetail);
		if (existingVersion != null) {
			throw new BizException("openapi.generator.version.exists");
		}
		// Validate package name format
		validatePackageName(request.getPackageName());
		request.setGroupId(request.getPackageName());
	}

	/**
	 * Check for re-entry prevention - ensure no other version is currently generating for the same artifactId
	 *
	 * @param artifactId Artifact ID to check
	 * @param userDetail User details
	 * @throws BizException if another version is currently generating for the same artifactId
	 */
	private void checkGenerationReentry(String artifactId, UserDetail userDetail) {
		log.info("Checking generation re-entry for artifactId: {}", artifactId);

		// Query for SDK with same artifactId and GENERATING status
		Query query = new Query(Criteria.where("artifactId").is(artifactId)
				.and("lastGenerateStatus").is(GenerateStatus.GENERATING));
		SDKDto generatingSdk = sdkService.findOne(query, userDetail);

		if (generatingSdk != null) {
			throw new BizException("openapi.generator.reentry.blocked", artifactId);
		}

		log.info("No generating version found for artifactId: {}, proceeding with generation", artifactId);
	}

	/**
	 * Validate Java package name format
	 *
	 * @param packageName Package name to validate
	 * @throws BizException if package name format is invalid
	 */
	private static void validatePackageName(String packageName) {
		if (packageName == null || packageName.trim().isEmpty()) {
			throw new BizException("openapi.generator.package.name.empty");
		}

		String trimmedPackageName = packageName.trim();

		// Java package name regex pattern:
		// - Must start with lowercase letter
		// - Can contain lowercase letters, digits, underscores
		// - Segments separated by dots
		// - Each segment must start with a letter
		String packageNamePattern = "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)*$";

		if (!trimmedPackageName.matches(packageNamePattern)) {
			throw new BizException("openapi.generator.package.name.invalid.format");
		}

		// Check for Java reserved keywords in package segments
		String[] segments = trimmedPackageName.split("\\.");
		for (String segment : segments) {
			if (isJavaReservedKeyword(segment)) {
				throw new BizException("openapi.generator.package.name.contains.reserved.keyword", segment);
			}
		}
	}

	/**
	 * Check if a string is a Java reserved keyword
	 *
	 * @param word Word to check
	 * @return true if the word is a Java reserved keyword
	 */
	private static boolean isJavaReservedKeyword(String word) {
		// Java reserved keywords that cannot be used in package names
		String[] javaKeywords = {
				"abstract", "assert", "boolean", "break", "byte", "case", "catch", "char",
				"class", "const", "continue", "default", "do", "double", "else", "enum",
				"extends", "final", "finally", "float", "for", "goto", "if", "implements",
				"import", "instanceof", "int", "interface", "long", "native", "new", "null",
				"package", "private", "protected", "public", "return", "short", "static",
				"strictfp", "super", "switch", "synchronized", "this", "throw", "throws",
				"transient", "try", "void", "volatile", "while", "true", "false"
		};

		for (String keyword : javaKeywords) {
			if (keyword.equals(word)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Create or update SDK records based on artifactId uniqueness
	 *
	 * @param request    Code generation request
	 * @param userDetail User details
	 * @return Created or updated SDK DTO
	 */
	private SDKDto createOrUpdateSdkRecords(CodeGenerationRequest request, UserDetail userDetail) {
		log.info("Creating or updating SDK records for artifactId: {}", request.getArtifactId());

		// Check if SDK with same artifactId already exists
		Query sdkQuery = new Query(Criteria.where("artifactId").is(request.getArtifactId()));
		SDKDto existingSdk = sdkService.findOne(sdkQuery, userDetail);

		SDKDto sdkDto;
		if (existingSdk == null) {
			// Create new SDK record
			sdkDto = createNewSdkRecord(request, userDetail);
			log.info("Created new SDK record with ID: {}", sdkDto.getId());
		} else {
			// Update existing SDK record
			sdkDto = updateExistingSdkRecord(existingSdk, request, userDetail);
			log.info("Updated existing SDK record with ID: {}", sdkDto.getId());
		}

		// Create version record
		SdkVersionDto versionDto = createVersionRecord(sdkDto.getId().toString(), request.getVersion(), userDetail);
		log.info("Created version record with ID: {}", versionDto.getId());

		return sdkDto;
	}

	/**
	 * Create a new SDK record
	 */
	private SDKDto createNewSdkRecord(CodeGenerationRequest request, UserDetail userDetail) {
		SDKDto sdkDto = new SDKDto();
		sdkDto.setOas(request.getOas());
		sdkDto.setLan(request.getLan());
		sdkDto.setPackageName(request.getPackageName());
		sdkDto.setArtifactId(request.getArtifactId());
		sdkDto.setGroupId(request.getGroupId());
		sdkDto.setVersion(request.getVersion());
		sdkDto.setClientId(request.getClientId());
		sdkDto.setRequestAddress(request.getRequestAddress());
		sdkDto.setTemplateLibrary(request.getTemplateLibrary());
		sdkDto.setLastGenerateStatus(GenerateStatus.GENERATING);
		sdkDto.setLastGenerationTime(new Date());

		return sdkService.save(sdkDto, userDetail);
	}

	/**
	 * Update existing SDK record
	 */
	private SDKDto updateExistingSdkRecord(SDKDto existingSdk, CodeGenerationRequest request, UserDetail userDetail) {
		// Update fields from request
		existingSdk.setOas(request.getOas());
		existingSdk.setLan(request.getLan());
		existingSdk.setPackageName(request.getPackageName());
		existingSdk.setGroupId(request.getGroupId());
		existingSdk.setVersion(request.getVersion());
		existingSdk.setClientId(request.getClientId());
		existingSdk.setRequestAddress(request.getRequestAddress());
		existingSdk.setTemplateLibrary(request.getTemplateLibrary());
		existingSdk.setLastGenerateStatus(GenerateStatus.GENERATING);
		existingSdk.setLastGenerationTime(new Date());

		return sdkService.save(existingSdk, userDetail);
	}

	/**
	 * Create version record with existence check
	 */
	private SdkVersionDto createVersionRecord(String sdkId, String version, UserDetail userDetail) {
		log.info("Creating version record for SDK ID: {}, version: {}", sdkId, version);

		// Create new version record
		SdkVersionDto versionDto = new SdkVersionDto();
		versionDto.setVersion(version);
		versionDto.setSdkId(sdkId);
		versionDto.setGenerateStatus(GenerateStatus.GENERATING);

		SdkVersionDto savedVersion = sdkVersionService.save(versionDto, userDetail);
		log.info("Created new version record with ID: {} for version: {}", savedVersion.getId(), version);
		return savedVersion;
	}

	private SdkVersionDto checkVersionExists(CodeGenerationRequest request, UserDetail userDetail) {
		String version = request.getVersion();
		Query sdkQuery = new Query(Criteria.where("artifactId").is(request.getArtifactId()));
		SDKDto existingSdk = sdkService.findOne(sdkQuery, userDetail);
		if (null == existingSdk) {
			return null;
		}
		String sdkId = existingSdk.getId().toString();
		Query query = new Query(Criteria.where("sdkId").is(sdkId).and("version").is(version));
		SdkVersionDto existingVersion = sdkVersionService.findOne(query, userDetail);

		if (existingVersion != null) {
			log.debug("Found existing version record with ID: {} for SDK ID: {}, version: {}",
					existingVersion.getId(), sdkId, version);
		} else {
			log.debug("No existing version found for SDK ID: {}, version: {}", sdkId, version);
		}

		return existingVersion;
	}

	/**
	 * Create module records and save module snapshots
	 */
	private void createModuleRecords(String sdkId, String versionId, List<String> moduleIds, UserDetail userDetail) {
		List<ModulesDto> modulesToProcess;

		if (CollectionUtils.isEmpty(moduleIds)) {
			log.info("No module IDs provided, retrieving all modules");
			// Get all modules that are not deleted
			Query allModulesQuery = new Query(Criteria.where("is_deleted").ne(true).and("status").is("active"));
			modulesToProcess = modulesService.findAllDto(allModulesQuery, userDetail);
			log.info("Found {} modules to process", modulesToProcess.size());
		} else {
			log.info("Creating module records and saving snapshots for {} specific modules", moduleIds.size());
			modulesToProcess = new ArrayList<>();
			List<ObjectId> batchModuleIds = new ArrayList<>();
			for (String moduleId : moduleIds) {
				batchModuleIds.add(new ObjectId(moduleId));
				if (batchModuleIds.size() >= 10) {
					List<ModulesDto> modulesDtos = modulesService.findAllDto(Query.query(Criteria.where("_id").in(batchModuleIds)), userDetail);
					if (null != modulesDtos) {
						modulesToProcess.addAll(modulesDtos);
					}
					batchModuleIds.clear();
				}
			}
			if (!batchModuleIds.isEmpty()) {
				List<ModulesDto> modulesDtos = modulesService.findAllDto(Query.query(Criteria.where("_id").in(batchModuleIds)), userDetail);
				if (null != modulesDtos) {
					modulesToProcess.addAll(modulesDtos);
				}
			}
		}

		// Process all modules (either all modules or specific ones)
		for (ModulesDto moduleDto : modulesToProcess) {
			try {
				SdkModuleDto sdkModuleDto = new SdkModuleDto();
				// Copy module properties (this creates the snapshot)
				BeanUtils.copyProperties(moduleDto, sdkModuleDto);
				// Set SDK references
				sdkModuleDto.setSdkId(sdkId);
				sdkModuleDto.setSdkVersionId(versionId);
				// Clear ID to create new record
				sdkModuleDto.setId(null);

				// Save the module snapshot as SdkModule record
				sdkModuleService.save(sdkModuleDto, userDetail);
				log.info("Created SDK module record and saved snapshot for module ID: {}", moduleDto.getId());
			} catch (Exception e) {
				log.error("Failed to create SDK module record for module ID: {}", moduleDto.getId(), e);
			}
		}
	}

	/**
	 * Update SDK generation status
	 */
	private void updateSdkGenerationStatus(String sdkId, GenerateStatus generateStatus, String errorMessage, UserDetail userDetail) {
		try {
			SDKDto sdkDto = sdkService.findById(new ObjectId(sdkId), userDetail);
			if (sdkDto != null) {
				sdkDto.setLastGenerateStatus(generateStatus);
				sdkDto.setLastGenerationTime(new Date());
				if (GenerateStatus.GENERATED.equals(generateStatus)) {
					sdkDto.setLastGeneratedVersion(sdkDto.getVersion());
					sdkDto.setGenerationErrorMessage(null);
				} else if (GenerateStatus.FAILED.equals(generateStatus)) {
					sdkDto.setGenerationErrorMessage(errorMessage);
				}
				sdkService.save(sdkDto, userDetail);
				log.info("Updated SDK generation status to: {} for SDK ID: {}", generateStatus, sdkId);
			}
		} catch (Exception e) {
			log.error("Failed to update SDK generation status for SDK ID: {}", sdkId, e);
		}
	}

	/**
	 * Update SDK generation status with file information (GridFS IDs and sizes)
	 */
	private void updateSdkGenerationStatusWithFiles(String sdkId, GenerateStatus generateStatus, String errorMessage,
													String zipGridfsId, Long zipSize, String jarGridfsId, Long jarSize,
													String jarError, UserDetail userDetail) {
		try {
			SDKDto sdkDto = sdkService.findById(new ObjectId(sdkId), userDetail);
			if (sdkDto != null) {
				sdkDto.setLastGenerateStatus(generateStatus);
				sdkDto.setLastGenerationTime(new Date());

				if (GenerateStatus.FAILED.equals(generateStatus)) {
					sdkDto.setGenerationErrorMessage(errorMessage);
				} else {
					// Clear any previous error message on success
					sdkDto.setGenerationErrorMessage(null);
					sdkDto.setLastGeneratedVersion(sdkDto.getVersion());
				}

				// Set ZIP file information
				if (zipGridfsId != null) {
					sdkDto.setLastZipGridfsId(zipGridfsId);
					sdkDto.setLastZipSizeOfByte(zipSize);
				}

				// Set JAR file information (if available)
				if (jarGridfsId != null) {
					sdkDto.setLastJarGridfsId(jarGridfsId);
					sdkDto.setLastJarSizeOfByte(jarSize);
				}

				// Set JAR generation error (if any)
				if (jarError != null) {
					sdkDto.setLastJarGenerationErrorMessage(jarError);
				}

				sdkService.save(sdkDto, userDetail);
				log.info("Updated SDK generation status to: {} for SDK ID: {} with ZIP ID: {}, JAR ID: {}",
						generateStatus, sdkId, zipGridfsId, jarGridfsId);
			}
		} catch (Exception e) {
			log.error("Failed to update SDK generation status with files for SDK ID: {}", sdkId, e);
		}
	}

	/**
	 * Update SDK version generation status
	 */
	private void updateSdkVersionGenerationStatus(String versionId, GenerateStatus generateStatus, String errorMessage, UserDetail userDetail) {
		try {
			SdkVersionDto versionDto = sdkVersionService.findById(new ObjectId(versionId), userDetail);
			if (versionDto != null) {
				versionDto.setGenerateStatus(generateStatus);
				if (GenerateStatus.FAILED.equals(generateStatus)) {
					versionDto.setGenerationErrorMessage(errorMessage);
				}
				sdkVersionService.save(versionDto, userDetail);
				log.info("Updated SDK version generation status to: {} for version ID: {}", generateStatus, versionId);
			}
		} catch (Exception e) {
			log.error("Failed to update SDK version generation status for version ID: {}", versionId, e);
		}
	}

	/**
	 * Update SDK version generation status with file information (GridFS IDs and sizes)
	 */
	private void updateSdkVersionGenerationStatusWithFiles(String versionId, GenerateStatus generateStatus, String errorMessage,
														   String zipGridfsId, Long zipSize, String jarGridfsId, Long jarSize,
														   String jarError, UserDetail userDetail) {
		try {
			SdkVersionDto versionDto = sdkVersionService.findById(new ObjectId(versionId), userDetail);
			if (versionDto != null) {
				versionDto.setGenerateStatus(generateStatus);

				if (GenerateStatus.FAILED.equals(generateStatus)) {
					versionDto.setGenerationErrorMessage(errorMessage);
				} else {
					// Clear any previous error message on success
					versionDto.setGenerationErrorMessage(null);
				}

				// Set ZIP file information
				if (zipGridfsId != null) {
					versionDto.setZipGridfsId(zipGridfsId);
					versionDto.setZipSizeOfByte(zipSize);
				}

				// Set JAR file information (if available)
				if (jarGridfsId != null) {
					versionDto.setJarGridfsId(jarGridfsId);
					versionDto.setJarSizeOfByte(jarSize);
				}

				// Set JAR generation error (if any)
				if (jarError != null) {
					versionDto.setJarGenerationErrorMessage(jarError);
				}

				sdkVersionService.save(versionDto, userDetail);
				log.info("Updated SDK version generation status to: {} for version ID: {} with ZIP ID: {}, JAR ID: {}",
						generateStatus, versionId, zipGridfsId, jarGridfsId);
			}
		} catch (Exception e) {
			log.error("Failed to update SDK version generation status with files for version ID: {}", versionId, e);
		}
	}

	/**
	 * Get version record by SDK ID and version
	 */
	private SdkVersionDto getVersionRecord(String sdkId, String version, UserDetail userDetail) {
		Query query = new Query(Criteria.where("sdkId").is(sdkId).and("version").is(version));
		return sdkVersionService.findOne(query, userDetail);
	}


}
