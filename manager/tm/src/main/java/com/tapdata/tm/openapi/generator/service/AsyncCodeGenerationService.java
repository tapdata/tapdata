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

import java.util.*;
import java.util.stream.Collectors;

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
		SdkVersionDto versionDto = createSdkVersion(request, userDetail, sdkDto);

		// Use Spring proxy to call async method to ensure @Async annotation works
		AsyncCodeGenerationService self = applicationContext.getBean(AsyncCodeGenerationService.class);
		self.generateCodeAsync(request, userDetail, sdkDto, versionDto);

		log.info("Async code generation task submitted successfully for artifactId: {}", request.getArtifactId());
	}

	@Async
	public void generateCodeAsync(CodeGenerationRequest request, UserDetail userDetail, SDKDto sdkDto, SdkVersionDto versionDto) {
		GenerationContext context = new GenerationContext(request, userDetail, sdkDto, versionDto);

		try {
			log.info("Starting async code generation for artifactId: {}, version: {}",
					context.getArtifactId(), context.getVersion());

			// Create module records
			executeWithErrorHandling(() -> createModuleRecords(
							context.getSdkId(), context.getVersionId(),
							request.getModuleIds(), userDetail),
					"Failed to create module records", context);

			// Use enhanced code generation with ZIP and JAR creation
			OpenApiGeneratorService.EnhancedGenerationResult result = executeWithErrorHandling(
					() -> openApiGeneratorService.generateCodeEnhanced(request),
					"Failed to generate enhanced code", context);

			// Handle generation result
			handleGenerationResult(result, context);

		} catch (GenerationException e) {
			handleGenerationFailure(e, context);
		} catch (Exception e) {
			handleUnexpectedFailure(e, context);
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
		if(CollectionUtils.isEmpty(request.getModuleIds())){
			throw new BizException("openapi.generator.module.empty");
		}
		// Check version conflicts among selected modules: same basePath+prefix must share the same version
		checkModuleVersionConflicts(request, userDetail);
		// Validate package name format
		validatePackageName(request.getPackageName());
		request.setGroupId(request.getPackageName());
	}

	/**
	 * Check version conflicts for selected modules.
	 * If there exist modules with the same basePath and prefix but different versions, throws BizException.
	 * Notes:
	 * - Treat missing basePath or prefix as empty string "".
	 * - Error message will explicitly list basePath values that have version conflicts.
	 */
	private void checkModuleVersionConflicts(CodeGenerationRequest request, UserDetail userDetail) {
		List<String> moduleIds = request.getModuleIds();
		if (CollectionUtils.isEmpty(moduleIds)) {
			return;
		}
		List<ObjectId> objectIds = moduleIds.stream().map(ObjectId::new).collect(Collectors.toList());
		Query query = Query.query(Criteria.where("_id").in(objectIds));
		// Only fetch necessary fields to reduce resource usage
		query.fields().include("basePath", "prefix", "apiVersion", "name");
		List<ModulesDto> modules = modulesService.findAllDto(query, userDetail);
		if (modules == null || modules.isEmpty()) {
			return;
		}
		// Group versions by basePath+prefix key, and collect module names for conflict reporting
		Map<String, Set<String>> versionsByKey = new HashMap<>();
		Map<String, List<String>> namesByKey = new HashMap<>();
		for (ModulesDto m : modules) {
			String basePath = m.getBasePath() == null ? "" : m.getBasePath();
			String prefix = m.getPrefix() == null ? "" : m.getPrefix();
			String version = m.getApiVersion() == null ? "" : m.getApiVersion();
			String key = basePath + "\u0001" + prefix; // use non-printable delimiter to avoid collisions
			versionsByKey.computeIfAbsent(key, k -> new HashSet<>()).add(version);
			namesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(m.getName());
		}
		java.util.List<String> conflictModuleNames = versionsByKey.entrySet().stream()
				.filter(e -> e.getValue().size() > 1)
				.flatMap(e -> namesByKey.getOrDefault(e.getKey(), Collections.emptyList()).stream())
				.distinct()
				.collect(Collectors.toList());
		if (!conflictModuleNames.isEmpty()) {
			String detail = String.join(", ", conflictModuleNames);
			throw new BizException("openapi.generator.module.version.conflict", detail);
		}
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

		return sdkDto;
	}

	private SdkVersionDto createSdkVersion(CodeGenerationRequest request, UserDetail userDetail, SDKDto sdkDto) {
		// Create version record
		SdkVersionDto versionDto = createVersionRecord(request, sdkDto, userDetail);
		log.info("Created version record with ID: {}", versionDto.getId());
		return versionDto;
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
		sdkDto.setLastGeneratedVersion(request.getVersion());
		sdkDto.setLastClientId(request.getClientId());
		sdkDto.setRequestAddress(request.getRequestAddress());
		sdkDto.setTemplateLibrary(request.getTemplateLibrary());
		sdkDto.setLastGenerateStatus(GenerateStatus.GENERATING);
		sdkDto.setLastGenerationTime(new Date());
		sdkDto.setLastModuleIds(request.getModuleIds());

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
		existingSdk.setLastGeneratedVersion(request.getVersion());
		existingSdk.setLastClientId(request.getClientId());
		existingSdk.setRequestAddress(request.getRequestAddress());
		existingSdk.setTemplateLibrary(request.getTemplateLibrary());
		existingSdk.setLastGenerateStatus(GenerateStatus.GENERATING);
		existingSdk.setLastGenerationTime(new Date());
		existingSdk.setLastModuleIds(request.getModuleIds());
		existingSdk.setLastZipGridfsId("");
		existingSdk.setLastJarGridfsId("");
		existingSdk.setLastZipSizeOfByte(0L);
		existingSdk.setLastJarSizeOfByte(0L);

		return sdkService.save(existingSdk, userDetail);
	}

	/**
	 * Create version record with existence check
	 */
	private SdkVersionDto createVersionRecord(CodeGenerationRequest request, SDKDto sdkDto, UserDetail userDetail) {
		log.info("Creating version record for SDK ID: {}, version: {}", sdkDto.getId().toString(), request.getVersion());

		// Create new version record
		SdkVersionDto versionDto = new SdkVersionDto();
		versionDto.setVersion(request.getVersion());
		versionDto.setSdkId(sdkDto.getId().toString());
		versionDto.setModuleIds(request.getModuleIds());
		versionDto.setClientId(request.getClientId());
		versionDto.setGenerateStatus(GenerateStatus.GENERATING);

		SdkVersionDto savedVersion = sdkVersionService.save(versionDto, userDetail);
		log.info("Created new version record with ID: {} for version: {}", savedVersion.getId(), request.getVersion());
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
	 * Create module records and save module snapshots using batch processing to prevent memory overflow
	 */
	private void createModuleRecords(String sdkId, String versionId, List<String> moduleIds, UserDetail userDetail) {
		// Configurable batch size for processing modules
		final int BATCH_SIZE = 20; // Process 50 modules at a time
		final int QUERY_BATCH_SIZE = 40; // Query 100 modules at a time for specific IDs

		if (CollectionUtils.isEmpty(moduleIds)) {
			log.info("No module IDs provided, processing all active modules in batches");
			processAllModulesInBatches(sdkId, versionId, userDetail, BATCH_SIZE);
		} else {
			log.info("Processing {} specific modules in batches", moduleIds.size());
			processSpecificModulesInBatches(sdkId, versionId, moduleIds, userDetail, QUERY_BATCH_SIZE, BATCH_SIZE);
		}
	}

	/**
	 * Process all active modules using pagination to prevent memory overflow
	 */
	private void processAllModulesInBatches(String sdkId, String versionId, UserDetail userDetail, int batchSize) {
		Criteria baseCriteria = Criteria.where("is_deleted").ne(true).and("status").is("active");
		Query baseQuery = new Query(baseCriteria);

		// Get total count for logging
		long totalCount = modulesService.count(baseQuery, userDetail);
		log.info("Found {} total active modules to process in batches of {}", totalCount, batchSize);

		int skip = 0;
		int processedCount = 0;
		int batchNumber = 1;

		while (true) {
			// Create paginated query with same criteria
			Query paginatedQuery = new Query(baseCriteria)
					.skip(skip)
					.limit(batchSize);

			// Retrieve current batch
			List<ModulesDto> currentBatch = modulesService.findAllDto(paginatedQuery, userDetail);

			if (currentBatch.isEmpty()) {
				log.info("No more modules to process. Total processed: {}", processedCount);
				break;
			}

			log.info("Processing batch {} with {} modules (skip: {}, processed so far: {})",
					batchNumber, currentBatch.size(), skip, processedCount);

			// Check if this is the last batch before processing
			boolean isLastBatch = currentBatch.size() < batchSize;

			// Process current batch
			List<SdkModuleDto> sdkModuleBatch = processBatch(currentBatch, sdkId, versionId);

			// Batch save all modules in current batch
			saveBatch(sdkModuleBatch, userDetail, batchNumber);

			processedCount += currentBatch.size();
			skip += batchSize;
			batchNumber++;

			// Clear batch from memory
			currentBatch.clear();
			sdkModuleBatch.clear();

			// If this was the last batch, we've reached the end
			if (isLastBatch) {
				log.info("Reached end of modules. Total processed: {}", processedCount);
				break;
			}
		}

		log.info("Completed processing all modules. Total processed: {} modules in {} batches",
				processedCount, batchNumber - 1);
	}

	/**
	 * Process specific modules by IDs using batch queries and processing
	 */
	private void processSpecificModulesInBatches(String sdkId, String versionId, List<String> moduleIds,
												 UserDetail userDetail, int queryBatchSize, int processBatchSize) {
		log.info("Processing {} specific modules with query batch size {} and process batch size {}",
				moduleIds.size(), queryBatchSize, processBatchSize);

		List<ModulesDto> allModulesToProcess = new ArrayList<>();

		// First, retrieve all specified modules in query batches
		for (int i = 0; i < moduleIds.size(); i += queryBatchSize) {
			int endIndex = Math.min(i + queryBatchSize, moduleIds.size());
			List<String> batchModuleIds = moduleIds.subList(i, endIndex);

			List<ObjectId> objectIds = batchModuleIds.stream()
					.map(ObjectId::new)
					.collect(Collectors.toList());

			Query batchQuery = Query.query(Criteria.where("_id").in(objectIds));
			List<ModulesDto> batchModules = modulesService.findAllDto(batchQuery, userDetail);

			if (batchModules != null && !batchModules.isEmpty()) {
				allModulesToProcess.addAll(batchModules);
				log.info("Retrieved {} modules in query batch {}/{}",
						batchModules.size(), (i / queryBatchSize) + 1,
						(moduleIds.size() + queryBatchSize - 1) / queryBatchSize);
			}
		}

		log.info("Retrieved {} total modules, now processing in batches of {}",
				allModulesToProcess.size(), processBatchSize);

		// Process retrieved modules in processing batches
		for (int i = 0; i < allModulesToProcess.size(); i += processBatchSize) {
			int endIndex = Math.min(i + processBatchSize, allModulesToProcess.size());
			List<ModulesDto> processBatch = allModulesToProcess.subList(i, endIndex);

			int batchNumber = (i / processBatchSize) + 1;
			log.info("Processing batch {} with {} modules", batchNumber, processBatch.size());

			// Process current batch
			List<SdkModuleDto> sdkModuleBatch = processBatch(processBatch, sdkId, versionId);

			// Batch save all modules in current batch
			saveBatch(sdkModuleBatch, userDetail, batchNumber);

			// Clear processed batch from memory
			sdkModuleBatch.clear();
		}

		log.info("Completed processing {} specific modules", allModulesToProcess.size());
	}

	/**
	 * Process a batch of modules and convert them to SdkModuleDto
	 */
	private List<SdkModuleDto> processBatch(List<ModulesDto> modulesBatch, String sdkId, String versionId) {
		List<SdkModuleDto> sdkModuleBatch = new ArrayList<>(modulesBatch.size());

		for (ModulesDto moduleDto : modulesBatch) {
			try {
				SdkModuleDto sdkModuleDto = new SdkModuleDto();
				// Copy module properties (this creates the snapshot)
				BeanUtils.copyProperties(moduleDto, sdkModuleDto);
				// Set SDK references
				sdkModuleDto.setSdkId(sdkId);
				sdkModuleDto.setSdkVersionId(versionId);
				// Clear ID to create new record
				sdkModuleDto.setId(null);

				sdkModuleBatch.add(sdkModuleDto);
			} catch (Exception e) {
				log.error("Failed to process module ID: {} in batch, skipping", moduleDto.getId(), e);
			}
		}

		return sdkModuleBatch;
	}

	/**
	 * Save a batch of SdkModuleDto using batch save operation
	 */
	private void saveBatch(List<SdkModuleDto> sdkModuleBatch, UserDetail userDetail, int batchNumber) {
		if (sdkModuleBatch.isEmpty()) {
			log.warn("Batch {} is empty, skipping save operation", batchNumber);
			return;
		}

		try {
			// Use batch save operation
			List<SdkModuleDto> savedModules = sdkModuleService.save(sdkModuleBatch, userDetail);
			log.info("Successfully saved batch {} with {} SDK module records",
					batchNumber, savedModules.size());
		} catch (Exception e) {
			log.error("Failed to save batch {} with {} modules, attempting individual saves",
					batchNumber, sdkModuleBatch.size(), e);

			// Fallback to individual saves if batch save fails
			int successCount = 0;
			for (SdkModuleDto sdkModuleDto : sdkModuleBatch) {
				try {
					sdkModuleService.save(sdkModuleDto, userDetail);
					successCount++;
				} catch (Exception individualError) {
					log.error("Failed to save individual SDK module record for module ID: {}",
							sdkModuleDto.getId(), individualError);
				}
			}
			log.info("Batch {} fallback completed: {}/{} modules saved successfully",
					batchNumber, successCount, sdkModuleBatch.size());
		}
	}

	/**
	 * Update SDK generation status
	 */
	private void updateSdkGenerationStatus(String sdkId, GenerateStatus generateStatus, String errorMessage, SdkVersionDto versionDto, UserDetail userDetail) {
		try {
			SDKDto sdkDto = sdkService.findById(new ObjectId(sdkId), userDetail);
			if (sdkDto != null) {
				sdkDto.setLastGenerateStatus(generateStatus);
				sdkDto.setLastGenerationTime(new Date());
				if (GenerateStatus.GENERATED.equals(generateStatus)) {
					sdkDto.setLastGeneratedVersion(versionDto.getVersion());
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
													String jarError, SdkVersionDto versionDto, UserDetail userDetail) {
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
					sdkDto.setLastGeneratedVersion(versionDto.getVersion());
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
	 * Execute operation with error handling wrapper
	 */
	private <T> T executeWithErrorHandling(ThrowingSupplier<T> operation, String errorPhase, GenerationContext context) {
		try {
			return operation.get();
		} catch (Exception e) {
			String errorMessage = formatErrorMessage(errorPhase, e);
			log.error("{} for artifactId: {}, version: {}", errorMessage, context.getArtifactId(), context.getVersion(), e);
			throw new GenerationException(errorPhase, errorMessage, e);
		}
	}

	/**
	 * Execute operation with error handling wrapper (no return value)
	 */
	private void executeWithErrorHandling(ThrowingRunnable operation, String errorPhase, GenerationContext context) {
		try {
			operation.run();
		} catch (Exception e) {
			String errorMessage = formatErrorMessage(errorPhase, e);
			log.error("{} for artifactId: {}, version: {}", errorMessage, context.getArtifactId(), context.getVersion(), e);
			throw new GenerationException(errorPhase, errorMessage, e);
		}
	}

	/**
	 * Handle generation result (success or failure)
	 */
	private void handleGenerationResult(OpenApiGeneratorService.EnhancedGenerationResult result, GenerationContext context) {
		if (result.isSuccess()) {
			handleGenerationSuccess(result, context);
		} else {
			handleGenerationFailure(result, context);
		}
	}

	/**
	 * Handle successful generation
	 */
	private void handleGenerationSuccess(OpenApiGeneratorService.EnhancedGenerationResult result, GenerationContext context) {
		try {
			// Update both SDK and version status to GENERATED with GridFS IDs
			updateStatusWithRetry(() -> updateSdkGenerationStatusWithFiles(
							context.getSdkId(), GenerateStatus.GENERATED, null,
							result.getZipGridfsId(), result.getZipSize(),
							result.getJarGridfsId(), result.getJarSize(), result.getJarError(),
							context.getVersionDto(), context.getUserDetail()),
					"SDK status update", context);

			updateStatusWithRetry(() -> updateSdkVersionGenerationStatusWithFiles(
							context.getVersionId(), GenerateStatus.GENERATED, null,
							result.getZipGridfsId(), result.getZipSize(),
							result.getJarGridfsId(), result.getJarSize(), result.getJarError(),
							context.getUserDetail()),
					"SDK version status update", context);

			log.info("Async enhanced code generation completed successfully for artifactId: {}, version: {}, ZIP ID: {}, JAR ID: {}",
					context.getArtifactId(), context.getVersion(), result.getZipGridfsId(), result.getJarGridfsId());
		} catch (Exception e) {
			log.error("Failed to update status after successful generation for artifactId: {}, version: {}",
					context.getArtifactId(), context.getVersion(), e);
			// Even if status update fails, the generation was successful, so we don't re-throw
		}
	}

	/**
	 * Handle generation failure from result
	 */
	private void handleGenerationFailure(OpenApiGeneratorService.EnhancedGenerationResult result, GenerationContext context) {
		String errorMessage = formatErrorMessage("Enhanced code generation failed", result.getErrorMessage());
		log.error("Async enhanced code generation failed for artifactId: {}, version: {}, error: {}",
				context.getArtifactId(), context.getVersion(), result.getErrorMessage());

		updateFailureStatus(errorMessage, context);
	}

	/**
	 * Handle generation exception
	 */
	private void handleGenerationFailure(GenerationException e, GenerationContext context) {
		log.error("Generation failed in phase '{}' for artifactId: {}, version: {}",
				e.getPhase(), context.getArtifactId(), context.getVersion(), e);

		updateFailureStatus(e.getMessage(), context);
	}

	/**
	 * Handle unexpected exceptions
	 */
	private void handleUnexpectedFailure(Exception e, GenerationContext context) {
		String errorMessage = formatErrorMessage("Unexpected error during code generation", e);
		log.error("Unexpected failure during async code generation for artifactId: {}, version: {}",
				context.getArtifactId(), context.getVersion(), e);

		updateFailureStatus(errorMessage, context);
	}

	/**
	 * Update status to FAILED with error message
	 */
	private void updateFailureStatus(String errorMessage, GenerationContext context) {
		try {
			updateStatusWithRetry(() -> updateSdkGenerationStatus(
							context.getSdkId(), GenerateStatus.FAILED, errorMessage,
							context.getVersionDto(), context.getUserDetail()),
					"SDK failure status update", context);

			updateStatusWithRetry(() -> updateSdkVersionGenerationStatus(
							context.getVersionId(), GenerateStatus.FAILED, errorMessage,
							context.getUserDetail()),
					"SDK version failure status update", context);
		} catch (Exception updateException) {
			log.error("Critical: Failed to update failure status for artifactId: {}, version: {}",
					context.getArtifactId(), context.getVersion(), updateException);
		}
	}

	/**
	 * Execute status update with retry mechanism
	 */
	private void updateStatusWithRetry(ThrowingRunnable operation, String operationName, GenerationContext context) {
		int maxRetries = 3;
		int retryDelay = 1000; // 1 second

		for (int attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				operation.run();
				if (attempt > 1) {
					log.info("Status update succeeded on attempt {} for {}", attempt, operationName);
				}
				return;
			} catch (Exception e) {
				if (attempt == maxRetries) {
					log.error("Failed to execute {} after {} attempts for artifactId: {}, version: {}",
							operationName, maxRetries, context.getArtifactId(), context.getVersion(), e);
					throw new RuntimeException("Status update failed after retries", e);
				} else {
					log.warn("Attempt {} failed for {}, retrying in {}ms: {}",
							attempt, operationName, retryDelay, e.getMessage());
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw new RuntimeException("Interrupted during retry delay", ie);
					}
				}
			}
		}
	}

	/**
	 * Format error message consistently
	 */
	private String formatErrorMessage(String phase, String errorMessage) {
		return String.format("%s: %s", phase, errorMessage != null ? errorMessage : "Unknown error");
	}

	/**
	 * Format error message from exception
	 */
	private String formatErrorMessage(String phase, Exception e) {
		String message = e.getMessage();
		if (message == null || message.trim().isEmpty()) {
			message = e.getClass().getSimpleName();
		}
		return formatErrorMessage(phase, message);
	}

	/**
	 * Context class to hold generation-related information
	 */
	private static class GenerationContext {
		private final CodeGenerationRequest request;
		private final UserDetail userDetail;
		private final SDKDto sdkDto;
		private final SdkVersionDto versionDto;

		public GenerationContext(CodeGenerationRequest request, UserDetail userDetail,
								 SDKDto sdkDto, SdkVersionDto versionDto) {
			this.request = request;
			this.userDetail = userDetail;
			this.sdkDto = sdkDto;
			this.versionDto = versionDto;
		}

		public String getArtifactId() {
			return request.getArtifactId();
		}

		public String getVersion() {
			return request.getVersion();
		}

		public String getSdkId() {
			return sdkDto.getId().toString();
		}

		public String getVersionId() {
			return versionDto.getId().toString();
		}

		public CodeGenerationRequest getRequest() {
			return request;
		}

		public UserDetail getUserDetail() {
			return userDetail;
		}

		public SDKDto getSdkDto() {
			return sdkDto;
		}

		public SdkVersionDto getVersionDto() {
			return versionDto;
		}
	}

	/**
	 * Custom exception for generation failures
	 */
	private static class GenerationException extends RuntimeException {
		private final String phase;

		public GenerationException(String phase, String message, Throwable cause) {
			super(message, cause);
			this.phase = phase;
		}

		public String getPhase() {
			return phase;
		}
	}

	/**
	 * Functional interface for operations that can throw exceptions
	 */
	@FunctionalInterface
	private interface ThrowingSupplier<T> {
		T get() throws Exception;
	}

	/**
	 * Functional interface for operations that don't return values
	 */
	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

}
