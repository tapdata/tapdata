package com.tapdata.tm.openapi.generator.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.sdk.dto.SDKDto;
import com.tapdata.tm.sdk.service.GenerateStatus;
import com.tapdata.tm.sdk.service.SDKService;
import com.tapdata.tm.sdkModule.dto.SdkModuleDto;
import com.tapdata.tm.sdkModule.service.SdkModuleService;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.service.SdkVersionService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AsyncCodeGenerationService
 *
 * @author samuel
 * @Description
 * @create 2025-11-26 16:14
 **/
@DisplayName("Class AsyncCodeGenerationService Test")
class AsyncCodeGenerationServiceTest {

    private SDKService sdkService;
    private SdkVersionService sdkVersionService;
    private SdkModuleService sdkModuleService;
    private ModulesService modulesService;
    private OpenApiGeneratorService openApiGeneratorService;
    private ApplicationContext applicationContext;
    private UserDetail userDetail;

    private AsyncCodeGenerationService asyncCodeGenerationService;

    @BeforeEach
    void setUp() {
        sdkService = mock(SDKService.class);
        sdkVersionService = mock(SdkVersionService.class);
        sdkModuleService = mock(SdkModuleService.class);
        modulesService = mock(ModulesService.class);
        openApiGeneratorService = mock(OpenApiGeneratorService.class);
        applicationContext = mock(ApplicationContext.class);
        userDetail = mock(UserDetail.class);

        asyncCodeGenerationService = new AsyncCodeGenerationService(
                sdkService, sdkVersionService, sdkModuleService,
                modulesService, openApiGeneratorService, applicationContext);
    }

    private CodeGenerationRequest createValidRequest() {
        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setArtifactId("test-sdk");
        request.setVersion("1.0.0");
        request.setPackageName("io.tapdata.test");
        request.setGroupId("io.tapdata");
        request.setLan("spring");
        request.setOas("http://example.com/openapi.json");
        request.setClientId("client-123");
        request.setRequestAddress("http://127.0.0.1:3030");
        request.setModuleIds(Arrays.asList(new ObjectId().toHexString(), new ObjectId().toHexString()));
        return request;
    }

    @Nested
    @DisplayName("generateCode method tests")
    class GenerateCodeTests {

        @Test
        @DisplayName("should throw BizException when version already exists")
        void testGenerateCode_VersionExists() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto existingSdk = new SDKDto();
            existingSdk.setId(new ObjectId());
            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(existingSdk);

            SdkVersionDto existingVersion = new SdkVersionDto();
            existingVersion.setId(new ObjectId());
            when(sdkVersionService.findOne(any(Query.class), eq(userDetail))).thenReturn(existingVersion);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should throw BizException when moduleIds is empty")
        void testGenerateCode_EmptyModuleIds() {
            CodeGenerationRequest request = createValidRequest();
            request.setModuleIds(Collections.emptyList());

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should throw BizException when another version is generating")
        void testGenerateCode_ReentryBlocked() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto generatingSdk = new SDKDto();
            generatingSdk.setId(new ObjectId());
            generatingSdk.setLastGenerateStatus(GenerateStatus.GENERATING);

            // First query for re-entry check returns generating SDK
            when(sdkService.findOne(any(Query.class), eq(userDetail)))
                    .thenReturn(generatingSdk);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should throw BizException when package name is empty")
        void testGenerateCode_EmptyPackageName() {
            CodeGenerationRequest request = createValidRequest();
            request.setPackageName("");

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should throw BizException when package name contains reserved keyword")
        void testGenerateCode_PackageNameWithReservedKeyword() {
            CodeGenerationRequest request = createValidRequest();
            request.setPackageName("io.tapdata.class");

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should throw BizException when package name format is invalid")
        void testGenerateCode_InvalidPackageNameFormat() {
            CodeGenerationRequest request = createValidRequest();
            request.setPackageName("Io.Tapdata.Test"); // uppercase not allowed

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }
    }

    @Nested
    @DisplayName("checkModuleVersionConflicts tests")
    class CheckModuleVersionConflictsTests {

        @Test
        @DisplayName("should throw BizException when modules have version conflicts")
        void testCheckModuleVersionConflicts_HasConflicts() {
            CodeGenerationRequest request = createValidRequest();
            List<String> moduleIds = Arrays.asList(
                    new ObjectId().toHexString(),
                    new ObjectId().toHexString()
            );
            request.setModuleIds(moduleIds);

            // Create modules with same basePath+prefix but different versions
            ModulesDto module1 = new ModulesDto();
            module1.setId(new ObjectId());
            module1.setName("module1");
            module1.setBasePath("/api");
            module1.setPrefix("v1");
            module1.setApiVersion("1.0.0");

            ModulesDto module2 = new ModulesDto();
            module2.setId(new ObjectId());
            module2.setName("module2");
            module2.setBasePath("/api");
            module2.setPrefix("v1");
            module2.setApiVersion("2.0.0"); // Different version

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);
            when(modulesService.findAllDto(any(Query.class), eq(userDetail)))
                    .thenReturn(Arrays.asList(module1, module2));

            assertThrows(BizException.class, () -> asyncCodeGenerationService.generateCode(request, userDetail));
        }

        @Test
        @DisplayName("should not throw when modules have same version for same basePath+prefix")
        void testCheckModuleVersionConflicts_NoConflicts() {
            CodeGenerationRequest request = createValidRequest();
            List<String> moduleIds = Arrays.asList(
                    new ObjectId().toHexString(),
                    new ObjectId().toHexString()
            );
            request.setModuleIds(moduleIds);

            // Create modules with same basePath+prefix and same version
            ModulesDto module1 = new ModulesDto();
            module1.setId(new ObjectId());
            module1.setName("module1");
            module1.setBasePath("/api");
            module1.setPrefix("v1");
            module1.setApiVersion("1.0.0");

            ModulesDto module2 = new ModulesDto();
            module2.setId(new ObjectId());
            module2.setName("module2");
            module2.setBasePath("/api");
            module2.setPrefix("v1");
            module2.setApiVersion("1.0.0"); // Same version

            SDKDto savedSdk = new SDKDto();
            savedSdk.setId(new ObjectId());

            SdkVersionDto savedVersion = new SdkVersionDto();
            savedVersion.setId(new ObjectId());

            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);
            when(modulesService.findAllDto(any(Query.class), eq(userDetail)))
                    .thenReturn(Arrays.asList(module1, module2));
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(savedSdk);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(savedVersion);
            when(applicationContext.getBean(AsyncCodeGenerationService.class)).thenReturn(asyncCodeGenerationService);

            // Should not throw
            assertDoesNotThrow(() -> asyncCodeGenerationService.generateCode(request, userDetail));
        }
    }

    @Nested
    @DisplayName("generateCodeAsync method tests")
    class GenerateCodeAsyncTests {

        @Test
        @DisplayName("should update status to GENERATED on success")
        void testGenerateCodeAsync_Success() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto sdkDto = new SDKDto();
            sdkDto.setId(new ObjectId());

            SdkVersionDto versionDto = new SdkVersionDto();
            versionDto.setId(new ObjectId());

            OpenApiGeneratorService.EnhancedGenerationResult successResult =
                    OpenApiGeneratorService.EnhancedGenerationResult.success(
                            "zip-gridfs-id", 1024L, "jar-gridfs-id", 2048L, null);

            when(openApiGeneratorService.generateCodeEnhanced(request)).thenReturn(successResult);
            when(modulesService.count(any(Query.class), eq(userDetail))).thenReturn(0L);
            when(sdkService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(versionDto);
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(versionDto);

            asyncCodeGenerationService.generateCodeAsync(request, userDetail, sdkDto, versionDto);

            verify(sdkService, atLeastOnce()).save(any(SDKDto.class), eq(userDetail));
            verify(sdkVersionService, atLeastOnce()).save(any(SdkVersionDto.class), eq(userDetail));
        }

        @Test
        @DisplayName("should update status to FAILED on generation failure")
        void testGenerateCodeAsync_Failure() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto sdkDto = new SDKDto();
            sdkDto.setId(new ObjectId());

            SdkVersionDto versionDto = new SdkVersionDto();
            versionDto.setId(new ObjectId());

            OpenApiGeneratorService.EnhancedGenerationResult failureResult =
                    OpenApiGeneratorService.EnhancedGenerationResult.failure("Generation failed");

            when(openApiGeneratorService.generateCodeEnhanced(request)).thenReturn(failureResult);
            when(modulesService.count(any(Query.class), eq(userDetail))).thenReturn(0L);
            when(sdkService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(versionDto);
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(versionDto);

            asyncCodeGenerationService.generateCodeAsync(request, userDetail, sdkDto, versionDto);

            verify(sdkService, atLeastOnce()).save(argThat((SDKDto sdk) ->
                    GenerateStatus.FAILED.equals(sdk.getLastGenerateStatus())), eq(userDetail));
        }

        @Test
        @DisplayName("should handle exception during generation")
        void testGenerateCodeAsync_Exception() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto sdkDto = new SDKDto();
            sdkDto.setId(new ObjectId());

            SdkVersionDto versionDto = new SdkVersionDto();
            versionDto.setId(new ObjectId());

            when(openApiGeneratorService.generateCodeEnhanced(request))
                    .thenThrow(new RuntimeException("Unexpected error"));
            when(modulesService.count(any(Query.class), eq(userDetail))).thenReturn(0L);
            when(sdkService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(versionDto);
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(versionDto);

            // Should not throw, but should update status to FAILED
            assertDoesNotThrow(() ->
                    asyncCodeGenerationService.generateCodeAsync(request, userDetail, sdkDto, versionDto));

            verify(sdkService, atLeastOnce()).save(argThat((SDKDto sdk) ->
                    GenerateStatus.FAILED.equals(sdk.getLastGenerateStatus())), eq(userDetail));
        }
    }

    @Nested
    @DisplayName("createModuleRecords tests")
    class CreateModuleRecordsTests {

        @Test
        @DisplayName("should process all modules when moduleIds is empty")
        void testCreateModuleRecords_AllModules() {
            CodeGenerationRequest request = createValidRequest();
            request.setModuleIds(Collections.emptyList());

            SDKDto sdkDto = new SDKDto();
            sdkDto.setId(new ObjectId());

            SdkVersionDto versionDto = new SdkVersionDto();
            versionDto.setId(new ObjectId());

            // Mock for all modules processing
            when(modulesService.count(any(Query.class), eq(userDetail))).thenReturn(2L);

            List<ModulesDto> modules = new ArrayList<>();
            ModulesDto module1 = new ModulesDto();
            module1.setId(new ObjectId());
            module1.setName("module1");
            modules.add(module1);

            when(modulesService.findAllDto(any(Query.class), eq(userDetail)))
                    .thenReturn(modules)
                    .thenReturn(Collections.emptyList());

            List<SdkModuleDto> savedModules = new ArrayList<>();
            savedModules.add(new SdkModuleDto());
            when(sdkModuleService.save(anyList(), eq(userDetail))).thenReturn(savedModules);

            OpenApiGeneratorService.EnhancedGenerationResult successResult =
                    OpenApiGeneratorService.EnhancedGenerationResult.success(
                            "zip-id", 1024L, "jar-id", 2048L, null);
            when(openApiGeneratorService.generateCodeEnhanced(request)).thenReturn(successResult);
            when(sdkService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(versionDto);
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(versionDto);

            asyncCodeGenerationService.generateCodeAsync(request, userDetail, sdkDto, versionDto);

            verify(sdkModuleService, atLeastOnce()).save(anyList(), eq(userDetail));
        }

        @Test
        @DisplayName("should process specific modules when moduleIds is provided")
        void testCreateModuleRecords_SpecificModules() {
            CodeGenerationRequest request = createValidRequest();
            ObjectId moduleId1 = new ObjectId();
            ObjectId moduleId2 = new ObjectId();
            request.setModuleIds(Arrays.asList(moduleId1.toHexString(), moduleId2.toHexString()));

            SDKDto sdkDto = new SDKDto();
            sdkDto.setId(new ObjectId());

            SdkVersionDto versionDto = new SdkVersionDto();
            versionDto.setId(new ObjectId());

            List<ModulesDto> modules = new ArrayList<>();
            ModulesDto module1 = new ModulesDto();
            module1.setId(moduleId1);
            module1.setName("module1");
            ModulesDto module2 = new ModulesDto();
            module2.setId(moduleId2);
            module2.setName("module2");
            modules.add(module1);
            modules.add(module2);

            when(modulesService.findAllDto(any(Query.class), eq(userDetail))).thenReturn(modules);

            List<SdkModuleDto> savedModules = new ArrayList<>();
            savedModules.add(new SdkModuleDto());
            savedModules.add(new SdkModuleDto());
            when(sdkModuleService.save(anyList(), eq(userDetail))).thenReturn(savedModules);

            OpenApiGeneratorService.EnhancedGenerationResult successResult =
                    OpenApiGeneratorService.EnhancedGenerationResult.success(
                            "zip-id", 1024L, "jar-id", 2048L, null);
            when(openApiGeneratorService.generateCodeEnhanced(request)).thenReturn(successResult);
            when(sdkService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(versionDto);
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(sdkDto);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(versionDto);

            asyncCodeGenerationService.generateCodeAsync(request, userDetail, sdkDto, versionDto);

            verify(sdkModuleService, atLeastOnce()).save(anyList(), eq(userDetail));
        }
    }

    @Nested
    @DisplayName("SDK record creation tests")
    class SdkRecordCreationTests {

        @Test
        @DisplayName("should create new SDK record when not exists")
        void testCreateNewSdkRecord() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto savedSdk = new SDKDto();
            savedSdk.setId(new ObjectId());

            SdkVersionDto savedVersion = new SdkVersionDto();
            savedVersion.setId(new ObjectId());

            // No existing SDK
            when(sdkService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);
            when(modulesService.findAllDto(any(Query.class), eq(userDetail))).thenReturn(Collections.emptyList());
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(savedSdk);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(savedVersion);
            when(applicationContext.getBean(AsyncCodeGenerationService.class)).thenReturn(asyncCodeGenerationService);

            asyncCodeGenerationService.generateCode(request, userDetail);

            verify(sdkService).save(argThat((SDKDto sdk) ->
                    sdk.getArtifactId().equals("test-sdk") &&
                            sdk.getLastGenerateStatus() == GenerateStatus.GENERATING), eq(userDetail));
        }

        @Test
        @DisplayName("should update existing SDK record when exists")
        void testUpdateExistingSdkRecord() {
            CodeGenerationRequest request = createValidRequest();

            SDKDto existingSdk = new SDKDto();
            existingSdk.setId(new ObjectId());
            existingSdk.setArtifactId("test-sdk");
            existingSdk.setLastGenerateStatus(GenerateStatus.GENERATED);

            SdkVersionDto savedVersion = new SdkVersionDto();
            savedVersion.setId(new ObjectId());

            // First call for re-entry check returns null, second call for existence check returns existing SDK
            when(sdkService.findOne(any(Query.class), eq(userDetail)))
                    .thenReturn(null)  // re-entry check
                    .thenReturn(existingSdk);  // existence check
            when(sdkVersionService.findOne(any(Query.class), eq(userDetail))).thenReturn(null);
            when(modulesService.findAllDto(any(Query.class), eq(userDetail))).thenReturn(Collections.emptyList());
            when(sdkService.save(any(SDKDto.class), eq(userDetail))).thenReturn(existingSdk);
            when(sdkVersionService.save(any(SdkVersionDto.class), eq(userDetail))).thenReturn(savedVersion);
            when(applicationContext.getBean(AsyncCodeGenerationService.class)).thenReturn(asyncCodeGenerationService);

            asyncCodeGenerationService.generateCode(request, userDetail);

            verify(sdkService).save(argThat((SDKDto sdk) ->
                    sdk.getLastGenerateStatus() == GenerateStatus.GENERATING), eq(userDetail));
        }
    }
}