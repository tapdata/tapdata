package com.tapdata.tm.ds.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.ds.service.impl.PdkReleaseCatalogService;
import com.tapdata.tm.ds.service.impl.PdkReleaseRegistrationService;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PdkControllerTest {
    private PdkController pdkController;
    private PkdSourceService pkdSourceService;
    private ProductComponent productComponent;
    @BeforeEach
    void buildMember(){
        pdkController = spy(new PdkController());
        pkdSourceService = mock(PkdSourceService.class);
        productComponent = mock(ProductComponent.class);
        ReflectionTestUtils.setField(pdkController,"pkdSourceService",pkdSourceService);
        ReflectionTestUtils.setField(pdkController,"productComponent",productComponent);
    }

    static Stream<Arguments> authenticationCases() {
        return Stream.of(
                Arguments.of("username_login", true, null, null, false),
                Arguments.of("saml_login", true, null, null, false),
                Arguments.of("access_code", true, null, null, true),
                Arguments.of("unknown", true, null, null, true),
                Arguments.of((String) null, true, null, null, true),
                Arguments.of("username_login", false, null, null, true),
                Arguments.of("saml_login", false, null, null, true),
                Arguments.of("username_login", true, "external-user", null, true),
                Arguments.of("username_login", true, null, "Basic encoded", true)
        );
    }

    @ParameterizedTest
    @MethodSource("authenticationCases")
    void uploadJarUsesProductAndAuthenticationSource(String authType, boolean enterprise,
                                                       String userIdHeader, String authorizationHeader,
                                                       boolean uploadOnlyRegistration) {
        UserDetail user = mock(UserDetail.class);
        when(user.getAuthType()).thenReturn(authType);
        when(productComponent.isDAAS()).thenReturn(enterprise);
        doReturn(user).when(pdkController).getLoginUser();
        MultipartFile[] files = {mock(MultipartFile.class)};
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (userIdHeader != null) {
            request.addHeader("user_id", userIdHeader);
        }
        if (authorizationHeader != null) {
            request.addHeader("authorization", authorizationHeader);
        }

        pdkController.uploadJar(files, List.of("{}"), false, request);

        verify(pkdSourceService).uploadPdk(eq(files), anyList(), eq(false), eq(user), eq(uploadOnlyRegistration));
    }

    @Test
    void uploadJarWithoutResolvedUserKeepsUploadOnlyPath() {
        doReturn(null).when(pdkController).getLoginUser();
        MultipartFile[] files = {mock(MultipartFile.class)};

        pdkController.uploadJar(files, List.of("{}"), false, new MockHttpServletRequest());

        verify(pkdSourceService).uploadPdk(eq(files), anyList(), eq(false), eq(null), eq(true));
    }
    @Nested
    class checkFileMd5Test{
        @Test
        void testCheckFileMd5(){
            when(pkdSourceService.checkJarMD5("111",14)).thenReturn("123456");
            ResponseMessage<String> actual = pdkController.checkFileMd5("111", 14);
            assertEquals("123456",actual.getData());
        }
        @Test
        void testCheckFileMd5CompatibleOldEngine(){
            when(pkdSourceService.checkJarMD5("111","a.jar")).thenReturn("123456");
            ResponseMessage<String> actual = pdkController.checkFileMd5("111", "a.jar");
            assertEquals("123456",actual.getData());
        }
    }

    @Nested
    class ConnectorReleaseTest {
        private PdkReleaseCatalogService catalogService;
        private PdkReleaseRegistrationService registrationService;
        private SettingsService settingsService;
        private UserDetail user;

        @BeforeEach
        void setUp() {
            catalogService = mock(PdkReleaseCatalogService.class);
            registrationService = mock(PdkReleaseRegistrationService.class);
            settingsService = mock(SettingsService.class);
            user = mock(UserDetail.class);
            ReflectionTestUtils.setField(pdkController, "pdkReleaseCatalogService", catalogService);
            ReflectionTestUtils.setField(pdkController, "pdkReleaseRegistrationService", registrationService);
            ReflectionTestUtils.setField(pdkController, "settingsService", settingsService);
            doReturn(user).when(pdkController).getLoginUser();
        }

        @Test
        void connectorReleasesReturnsCatalog() {
            List<Map<String, Object>> releases = List.of(Map.of("pdkId", "mysql"));
            when(catalogService.findLatestReleases()).thenReturn(releases);

            assertEquals(releases, pdkController.connectorReleases().getData());
        }

        @Test
        void registrationImpactValidatesReleaseFirst() {
            Map<String, Object> impact = Map.of("hasInFlightResources", false);
            when(registrationService.registrationImpact("mysql", user)).thenReturn(impact);

            assertEquals(impact, pdkController.registrationImpact("mysql", "mysql-connector").getData());
            verify(catalogService).findRelease("mysql-connector");
        }

        @Test
        void registerReleasePassesOptions() {
            pdkController.registerRelease(Map.of("artifactId", "mysql-connector", "pdkId", "mysql",
                    "stopTasks", true, "ignoreInFlightResources", false));

            verify(registrationService).downloadAndRegister("mysql-connector", "mysql", true, false, user);
        }

        @Test
        void registerReleaseRejectsCloud() {
            when(settingsService.isCloud()).thenReturn(true);
            Map<String, Object> request = Map.of("artifactId", "mysql-connector", "pdkId", "mysql");

            assertThrows(BizException.class, () -> pdkController.registerRelease(request));
            verify(registrationService, never()).downloadAndRegister(anyString(), anyString(), anyBoolean(), anyBoolean(), any());
        }

        @Test
        void registrationImpactRejectsFreeAuthAndMissingUser() {
            when(user.isFreeAuth()).thenReturn(true);
            assertThrows(BizException.class, () -> pdkController.registrationImpact("mysql", "mysql-connector"));

            doReturn(null).when(pdkController).getLoginUser();
            assertThrows(BizException.class, () -> pdkController.registrationImpact("mysql", "mysql-connector"));
            verify(catalogService, never()).findRelease(anyString());
        }
    }
}
