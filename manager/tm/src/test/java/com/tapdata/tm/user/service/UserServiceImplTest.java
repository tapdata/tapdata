package com.tapdata.tm.user.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.user.dto.LdapLoginDto;
import com.tapdata.tm.user.dto.TestLdapDto;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UserServiceImplTest {
    private UserServiceImpl userService;
    private RoleMappingService roleMappingService;
    private SettingsService settingsService;

    @BeforeEach
    void beforeEach(){
        userService = mock(UserServiceImpl.class);
        roleMappingService = mock(RoleMappingService.class);
        settingsService = mock(SettingsService.class);
        ReflectionTestUtils.setField(userService, "settingsService", settingsService);
        ReflectionTestUtils.setField(userService, "roleMappingService", roleMappingService);
    }

    @Nested
    class updateRoleMapping{
        private String userId;
        private List<Object> roleusers;
        private UserDetail userDetail;

        @BeforeEach
        void beforeEach(){
            userId = "66c84372a5921a16459c2cef";
            roleusers = new ArrayList<>();
            userDetail = mock(UserDetail.class);
        }

        @Test
        void testForAdmin(){
            doCallRealMethod().when(userService).updateRoleMapping(userId,roleusers,userDetail);
            List<RoleMappingDto> actual = userService.updateRoleMapping(userId, roleusers, userDetail);
            verify(roleMappingService,new Times(0)).deleteAll(any(Query.class));
            assertNull(actual);
        }

        @Test
        void testForUser(){
            when(userDetail.getEmail()).thenReturn("test@tapdata.com");
            roleusers.add("5d31ae1ab953565ded04badd");
            doCallRealMethod().when(userService).updateRoleMapping(userId,roleusers,userDetail);
            List<RoleMappingDto> actual = userService.updateRoleMapping(userId, roleusers, userDetail);
            verify(roleMappingService,new Times(1)).deleteAll(any(Query.class));
            verify(roleMappingService,new Times(1)).updateUserRoleMapping(anyList(), any(UserDetail.class));
        }
    }

    @Nested
    class checkADLoginEnable {
        @BeforeEach
        void beforeEach() {
            doCallRealMethod().when(userService).checkLdapLoginEnable();
        }

        @Test
        @DisplayName("test checkADLoginEnable method when open is true")
        void test1() {
            Settings settings = new Settings();
            settings.setOpen(true);
            when(settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_LOGIN_ENABLE)).thenReturn(settings);
            boolean result = userService.checkLdapLoginEnable();
            assertTrue(result);
        }

        @Test
        @DisplayName("test checkADLoginEnable method when settings is null")
        void test2() {
            when(settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_LOGIN_ENABLE)).thenReturn(null);
            boolean result = userService.checkLdapLoginEnable();
            assertFalse(result);
        }
    }

    @Nested
    class testLoginByADTest {
        private DirContext dirContext;
        private TestLdapDto testAdDto;

        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            dirContext = mock(DirContext.class);
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
            doCallRealMethod().when(userService).testLoginByLdap(any(TestLdapDto.class));
            testAdDto = new TestLdapDto();
            testAdDto.setLdap_Server_Host("ldap://ad.example.com");
            testAdDto.setLdap_Server_Port("389");
            testAdDto.setLdap_Bind_DN("CN=Admin,CN=Users,DC=example,DC=com");
            testAdDto.setLdap_SSL_Enable(false);
        }

        @Test
        @SneakyThrows
        void testLoginByAD_SuccessfulConnection() {
            testAdDto.setLdap_Bind_Password("password");
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
            TestResponseDto response = userService.testLoginByLdap(testAdDto);
            assertTrue(response.isResult());
            assertNull(response.getStack());
        }

        @Test
        @SneakyThrows
        void testLoginByAD_FailedConnection() {
            testAdDto.setLdap_Bind_Password("password");
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(null);
            TestResponseDto response = userService.testLoginByLdap(testAdDto);
            assertFalse(response.isResult());
            assertEquals("connect to active directory server failed", response.getStack());
        }

        @Test
        @Disabled
        @SneakyThrows
        void testLoginByAD_ThrowsNamingException() {
            testAdDto.setLdap_Bind_Password("password");
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenThrow(new NamingException("LDAP connection failed"));
            TestResponseDto response = userService.testLoginByLdap(testAdDto);
            assertFalse(response.isResult());
            assertTrue(response.getStack().contains("LDAP connection failed"));
        }

        @Test
        @SneakyThrows
        void testLoginByAD_EncryptedPassword() {
            try (MockedStatic<SettingUtil> mb = Mockito
                    .mockStatic(SettingUtil.class)) {
                mb.when(() -> SettingUtil.getValue("LDAP", "ldap.bind.password")).thenReturn("123456");
                testAdDto.setLdap_Bind_Password("*****");
                when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
                TestResponseDto response = userService.testLoginByLdap(testAdDto);
                assertTrue(response.isResult());
                assertNull(response.getStack());
            }
        }

        @Test
        @SneakyThrows
        void testLoginByAD_NPE() {
            try (MockedStatic<SettingUtil> mb = Mockito
                    .mockStatic(SettingUtil.class)) {
                mb.when(() -> SettingUtil.getValue("LDAP", "ldap.bind.password")).thenReturn("123456");
                testAdDto.setLdap_Bind_Password("*****");
                when(userService.buildDirContext(any(LdapLoginDto.class))).thenThrow(NullPointerException.class);
                TestResponseDto response = userService.testLoginByLdap(testAdDto);
                assertFalse(response.isResult());
                assertEquals("please check ldap configuration, such as bind dn or password", response.getStack());
            }
        }
    }

    @Nested
    class loginByADTest {
        private List<Settings> settingsList;
        private String username;
        private String password;

        @BeforeEach
        void beforeEach() {
            username = "user1";
            password = "password1";
            Settings settings1 = new Settings();
            settings1.setKey("ad.server.host");
            settings1.setValue("ldap://ad.example.com");
            Settings settings2 = new Settings();
            settings2.setKey("ad.server.port");
            settings2.setValue("389");
            Settings settings3 = new Settings();
            settings3.setKey("ad.bind.dn");
            settings3.setValue("CN=Admin,CN=Users,DC=example,DC=com");
            Settings settings4 = new Settings();
            settings4.setKey("ad.bind.password");
            settings4.setValue("adminPassword");
            Settings settings5 = new Settings();
            settings5.setKey("ad.base.dn");
            settings5.setValue("DC=example,DC=com");
            settingsList = Arrays.asList(
                    settings1, settings2, settings3, settings4, settings5
            );
            doCallRealMethod().when(userService).loginByLdap(username, password);
        }

        @Test
        void testLoginByAD_Success() throws NamingException {
            when(settingsService.findAll()).thenReturn(settingsList);
            Settings settings = new Settings();
            settings.setKey("ad.ssl.enable");
            settings.setOpen(true);
            when(settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_SSL_ENABLE))
                    .thenReturn(settings);
            when(userService.searchUser(any(LdapLoginDto.class), eq(username))).thenReturn(true);
            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
            boolean result = userService.loginByLdap(username, password);
            assertTrue(result);
        }

        @Test
        void testLoginByAD_UserNotExists() throws NamingException {
            when(settingsService.findAll()).thenReturn(settingsList);
            when(userService.searchUser(any(LdapLoginDto.class), eq(username))).thenReturn(false);
            BizException thrown = assertThrows(BizException.class, () -> userService.loginByLdap(username, password));
            assertEquals("AD.Account.Not.Exists", thrown.getErrorCode());
        }

        @Test
        void testLoginByAD_NamingException() throws NamingException {
            when(settingsService.findAll()).thenReturn(settingsList);
            when(userService.searchUser(any(LdapLoginDto.class), eq(username))).thenReturn(true);
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenThrow(new NamingException("LDAP connection failed"));
            BizException thrown = assertThrows(BizException.class, () -> userService.loginByLdap(username, password));
            assertTrue(thrown.getErrorCode().contains("AD.Login.Fail"));
        }

        @Test
        void testLoginByAD_SSLDisabled() throws NamingException {
            when(settingsService.findAll()).thenReturn(settingsList);
            Settings settings = new Settings();
            settings.setKey("ad.ssl.enable");
            settings.setOpen(true);
            when(settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_SSL_ENABLE))
                    .thenReturn(settings);
            when(userService.searchUser(any(LdapLoginDto.class), eq(username))).thenReturn(true);
            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
            boolean result = userService.loginByLdap(username, password);
            assertTrue(result);
        }
    }

    @Nested
    class searchUserTest {
        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            doCallRealMethod().when(userService).searchUser(any(LdapLoginDto.class), anyString());
        }

        @Test
        void testSearchUser_SuccessWithSAMAccountName() throws NamingException {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .baseDN("DC=example,DC=com")
                    .ldapUrl("ldap://ad.example.com:389")
                    .build();
            String username = "user1";
            String sAMAccountNameFilter = "(sAMAccountName=user1)";
            String userPrincipalNameFilter = "(userPrincipalName=user1)";
            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(adLoginDto)).thenReturn(dirContext);
            when(userService.searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(sAMAccountNameFilter), any(SearchControls.class)))
                    .thenReturn(true);
            boolean result = userService.searchUser(adLoginDto, username);
            assertTrue(result);
            verify(userService, times(1)).searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(sAMAccountNameFilter), any(SearchControls.class));
        }

        @Test
        void testSearchUser_SuccessWithUserPrincipalName() throws NamingException {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .baseDN("DC=example,DC=com")
                    .ldapUrl("ldap://ad.example.com:389")
                    .build();
            String username = "user1";
            String sAMAccountNameFilter = "(sAMAccountName=user1)";
            String userPrincipalNameFilter = "(userPrincipalName=user1)";
            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(adLoginDto)).thenReturn(dirContext);
            when(userService.searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(sAMAccountNameFilter), any(SearchControls.class)))
                    .thenReturn(false);
            when(userService.searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(userPrincipalNameFilter), any(SearchControls.class)))
                    .thenReturn(true);

            boolean result = userService.searchUser(adLoginDto, username);

            assertTrue(result);
            verify(userService, times(1)).searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(userPrincipalNameFilter), any(SearchControls.class));
        }

        @Test
        void testSearchUser_UserNotFound() throws NamingException {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .baseDN("DC=example,DC=com")
                    .ldapUrl("ldap://ad.example.com:389")
                    .build();
            String username = "user1";
            String sAMAccountNameFilter = "(sAMAccountName=user1)";
            String userPrincipalNameFilter = "(userPrincipalName=user1)";

            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(adLoginDto)).thenReturn(dirContext);
            when(userService.searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(sAMAccountNameFilter), any(SearchControls.class)))
                    .thenReturn(false);
            when(userService.searchWithFilter(eq(dirContext), eq(adLoginDto.getBaseDN()), eq(userPrincipalNameFilter), any(SearchControls.class)))
                    .thenReturn(false);
            boolean result = userService.searchUser(adLoginDto, username);
            assertFalse(result);
        }

        @Test
        void testSearchUser_NamingExceptionThrown() throws NamingException {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .baseDN("DC=example,DC=com")
                    .ldapUrl("ldap://ad.example.com:389")
                    .build();
            String username = "user1";
            DirContext dirContext = mock(DirContext.class);
            when(userService.buildDirContext(adLoginDto)).thenReturn(dirContext);
            when(userService.searchWithFilter(any(), any(), any(), any())).thenThrow(new NamingException("Search failed"));
            BizException thrown = assertThrows(BizException.class, () -> userService.searchUser(adLoginDto, username));
            assertTrue(thrown.getErrorCode().contains("AD.Search.Fail"));
        }
    }

    @Nested
    class searchWithFilterTest {
        private DirContext dirContext;
        private NamingEnumeration<SearchResult> namingEnum;
        private SearchResult searchResult;
        private Attributes attributes;
        private String searchBase;
        private String filter;

        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            searchBase = "DC=example,DC=com";
            filter = "(sAMAccountName=user1)";
            searchResult = mock(SearchResult.class);
            dirContext = mock(DirContext.class);
            namingEnum = mock(NamingEnumeration.class);
            attributes = mock(Attributes.class);
            doCallRealMethod().when(userService).searchWithFilter(any(DirContext.class), anyString(), anyString(), any(SearchControls.class));
        }

        @Test
        public void testSearchWithFilter_FoundUser() throws NamingException {
            SearchControls searchControls = new SearchControls();
            when(dirContext.search(searchBase, filter, searchControls)).thenReturn(namingEnum);
            when(namingEnum.hasMore()).thenReturn(true).thenReturn(false); // First call returns true, then false
            when(namingEnum.next()).thenReturn(searchResult);
            when(searchResult.getAttributes()).thenReturn(attributes);
            Attribute attribute = mock(Attribute.class);
            when(attribute.get()).thenReturn("user1@example.com");
            when(attributes.get("userPrincipalName")).thenReturn(attribute);
            Attribute attribute1 = mock(Attribute.class);
            when(attribute1.get()).thenReturn("User One");
            when(attributes.get("displayName")).thenReturn(attribute1);
            boolean result = userService.searchWithFilter(dirContext, searchBase, filter, searchControls);
            assertTrue(result);
        }

        @Test
        public void testSearchWithFilter_UserNotFound() throws NamingException {
            SearchControls searchControls = new SearchControls();
            when(dirContext.search(searchBase, filter, searchControls)).thenReturn(namingEnum);
            when(namingEnum.hasMore()).thenReturn(false);
            boolean result = userService.searchWithFilter(dirContext, searchBase, filter, searchControls);
            assertFalse(result);
        }

        @Test
        public void testSearchWithFilter_UserPrincipalNameMissing() throws NamingException {
            SearchControls searchControls = new SearchControls();
            when(dirContext.search(searchBase, filter, searchControls)).thenReturn(namingEnum);
            when(namingEnum.hasMore()).thenReturn(true).thenReturn(false);
            when(namingEnum.next()).thenReturn(searchResult);
            when(searchResult.getAttributes()).thenReturn(attributes);
            when(attributes.get("userPrincipalName")).thenReturn(null);
            Attribute attribute1 = mock(Attribute.class);
            when(attribute1.get()).thenReturn("User One");
            when(attributes.get("displayName")).thenReturn(attribute1);
            boolean result = userService.searchWithFilter(dirContext, searchBase, filter, searchControls);
            assertTrue(result);
        }

        @Test
        public void testSearchWithFilter_DisplayNameMissing() throws NamingException {
            String searchBase = "DC=example,DC=com";
            String filter = "(sAMAccountName=user1)";
            SearchControls searchControls = new SearchControls();
            when(dirContext.search(searchBase, filter, searchControls)).thenReturn(namingEnum);
            when(namingEnum.hasMore()).thenReturn(true).thenReturn(false);
            when(namingEnum.next()).thenReturn(searchResult);
            when(searchResult.getAttributes()).thenReturn(attributes);
            Attribute attribute = mock(Attribute.class);
            when(attribute.get()).thenReturn("user1@example.com");
            when(attributes.get("userPrincipalName")).thenReturn(attribute);
            when(attributes.get("displayName")).thenReturn(null);
            boolean result = userService.searchWithFilter(dirContext, searchBase, filter, searchControls);
            assertTrue(result);
        }

        @Test
        void testSearchWithFilter_NamingExceptionThrown() throws NamingException {
            String searchBase = "DC=example,DC=com";
            String filter = "(sAMAccountName=user1)";
            SearchControls searchControls = new SearchControls();
            when(dirContext.search(searchBase, filter, searchControls)).thenThrow(new NamingException("Search failed"));
            NamingException thrown = assertThrows(NamingException.class, () -> {
                userService.searchWithFilter(dirContext, searchBase, filter, searchControls);
            });
            assertEquals("Search failed", thrown.getMessage());
        }
    }

    @Nested
    class buildDirContextTest {
        private InitialDirContext dirContext;

        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            dirContext = mock(InitialDirContext.class);
            doCallRealMethod().when(userService).buildDirContext(any(LdapLoginDto.class));
        }

        @Test
        public void testBuildDirContext_NoSSL() throws NamingException {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .bindDN("cn=admin,dc=example,dc=com")
                    .password("password")
                    .sslEnable(false)
                    .build();
            assertThrows(NamingException.class, () -> userService.buildDirContext(adLoginDto));
        }

        @Test
        public void testBuildDirContext_NamingExceptionThrown() {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .bindDN("cn=admin,dc=example,dc=com")
                    .password("password")
                    .sslEnable(true)
                    .build();
            assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
        }

        @Test
        @SneakyThrows
        public void testBuildDirContext_SSL() {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .bindDN("cn=admin,dc=example,dc=com")
                    .password("password")
                    .sslEnable(true)
                    .cert(certString)
                    .build();
            SSLContext sslContext = mock(SSLContext.class);
            when(userService.createSSLContext(any(InputStream.class))).thenReturn(sslContext);
            when(sslContext.getSocketFactory()).thenReturn(mock(SSLSocketFactory.class));
            assertThrows(NamingException.class, () -> userService.buildDirContext(adLoginDto));
        }

        @Test
        @SneakyThrows
        public void testBuildDirContext_BindDn_Contains() {
            String baseDN = "dc=example,dc=com";
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .baseDN(baseDN)
                    .bindDN("admin")
                    .password("password")
                    .sslEnable(true)
                    .cert(certString)
                    .build();
            SSLContext sslContext = mock(SSLContext.class);
            doCallRealMethod().when(userService).convertBaseDnToDomain(baseDN);
            when(userService.createSSLContext(any(InputStream.class))).thenReturn(sslContext);
            when(sslContext.getSocketFactory()).thenReturn(mock(SSLSocketFactory.class));
            assertThrows(NamingException.class, () -> userService.buildDirContext(adLoginDto));
        }
    }

    @Nested
    class createSSLContextTest {
        @Test
        public void testCreateSSLContext() throws Exception {
            InputStream certFile = new ByteArrayInputStream(certString.getBytes());

            doCallRealMethod().when(userService).createSSLContext(certFile);
            SSLContext sslContext = userService.createSSLContext(certFile);

            assertNotNull(sslContext);
            assertNotNull(sslContext.getSocketFactory());

        }
    }
    private String certString = "-----BEGIN CERTIFICATE-----\n" +
            "MIIGpzCCBY+gAwIBAgITQwAAAAJuc4zIBYqYIwAAAAAAAjANBgkqhkiG9w0BAQUF\n" +
            "ADBwMRIwEAYKCZImiZPyLGQBGRYCaW8xFzAVBgoJkiaJk/IsZAEZFgd0YXBkYXRh\n" +
            "MRgwFgYKCZImiZPyLGQBGRYIaW50ZXJuYWwxEjAQBgoJkiaJk/IsZAEZFgJhZDET\n" +
            "MBEGA1UEAxMKdGFwZGF0YS1DQTAeFw0yNDA5MTMxNTQ1NTJaFw0yNTA5MTMxNTQ1\n" +
            "NTJaMDExLzAtBgNVBAMTJmlaOTk4eHJoNnkyYjNiWi5hZC5pbnRlcm5hbC50YXBk\n" +
            "YXRhLmlvMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo8mqTkTvFfu+\n" +
            "LXH69clyyxGw9Na8FKdmySUnI7zF2csyfryfU2+Lxi/Xq1q8WkjCpMQ+lRJ5K5WJ\n" +
            "7TOZ8FaWELFXGJDSdixRuK5aORjWz1KzMWZPW6w62FNLQIfpOtgVQebLe8it7eQR\n" +
            "WA6PtILbwEtkQtgagwtWmuf9linElM48WSEmpz78eFkW83vgJGfr2nYXyP7VqY83\n" +
            "XvcagE75nvg1CzgADaCm1plxoSyyx887C3AagSnamenZPH4Dtk3vi2RSJUsVGPcA\n" +
            "RQg6IViA1cradLu+Q2Htxwh2hL2+lJ/979qILEewFFTIwt9cOT5GoElrW21TSSpI\n" +
            "p5tQywmPGwIDAQABo4IDdzCCA3MwLwYJKwYBBAGCNxQCBCIeIABEAG8AbQBhAGkA\n" +
            "bgBDAG8AbgB0AHIAbwBsAGwAZQByMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEF\n" +
            "BQcDATAOBgNVHQ8BAf8EBAMCBaAweAYJKoZIhvcNAQkPBGswaTAOBggqhkiG9w0D\n" +
            "AgICAIAwDgYIKoZIhvcNAwQCAgCAMAsGCWCGSAFlAwQBKjALBglghkgBZQMEAS0w\n" +
            "CwYJYIZIAWUDBAECMAsGCWCGSAFlAwQBBTAHBgUrDgMCBzAKBggqhkiG9w0DBzAd\n" +
            "BgNVHQ4EFgQU9hwjXlc86SCkrWFyFlkg+KWh/IMwHwYDVR0jBBgwFoAU0PZvyVVN\n" +
            "5kp+RcZAZ76MbVHAIw8wgeEGA1UdHwSB2TCB1jCB06CB0KCBzYaBymxkYXA6Ly8v\n" +
            "Q049dGFwZGF0YS1DQSxDTj1pWjk5OHhyaDZ5MmIzYlosQ049Q0RQLENOPVB1Ymxp\n" +
            "YyUyMEtleSUyMFNlcnZpY2VzLENOPVNlcnZpY2VzLENOPUNvbmZpZ3VyYXRpb24s\n" +
            "REM9YWQsREM9aW50ZXJuYWwsREM9dGFwZGF0YSxEQz1pbz9jZXJ0aWZpY2F0ZVJl\n" +
            "dm9jYXRpb25MaXN0P2Jhc2U/b2JqZWN0Q2xhc3M9Y1JMRGlzdHJpYnV0aW9uUG9p\n" +
            "bnQwgc0GCCsGAQUFBwEBBIHAMIG9MIG6BggrBgEFBQcwAoaBrWxkYXA6Ly8vQ049\n" +
            "dGFwZGF0YS1DQSxDTj1BSUEsQ049UHVibGljJTIwS2V5JTIwU2VydmljZXMsQ049\n" +
            "U2VydmljZXMsQ049Q29uZmlndXJhdGlvbixEQz1hZCxEQz1pbnRlcm5hbCxEQz10\n" +
            "YXBkYXRhLERDPWlvP2NBQ2VydGlmaWNhdGU/YmFzZT9vYmplY3RDbGFzcz1jZXJ0\n" +
            "aWZpY2F0aW9uQXV0aG9yaXR5MFIGA1UdEQRLMEmgHwYJKwYBBAGCNxkBoBIEEHRb\n" +
            "dGzUREpNjZzesBnzoguCJmlaOTk4eHJoNnkyYjNiWi5hZC5pbnRlcm5hbC50YXBk\n" +
            "YXRhLmlvME8GCSsGAQQBgjcZAgRCMECgPgYKKwYBBAGCNxkCAaAwBC5TLTEtNS0y\n" +
            "MS0yNTA2MzQzMDk4LTM0MzExNDI3NzUtMzEwMjg3OTkwMi0xMDAwMA0GCSqGSIb3\n" +
            "DQEBBQUAA4IBAQCFJ40kMxvgjpFF/cTglqKBO4MJSMH3wJpN+VG33/WccM62osm4\n" +
            "TB/QydzHuSWtn52fdtp8Q82dmDvI2QldYyJLvifR+1TGbOt8dc5866jRNqUeRr7Q\n" +
            "KmTTPxGInM64b/aYYF0cQxC9HX1ct8aTlTArfSztoWgLWG3aXtBa0+TBMuvIDn5o\n" +
            "dUxc9/NvLsksOecz/AHfIPWmEKu9w+OtnbBnoo5ymB7tmSPUUR54pd5Ul06LvlWs\n" +
            "hinNPHvFAMDUY7HfrdDfCoEIZSK4u/xFQscsNsE3hywVYYN7uz8wy502fEHzwIgp\n" +
            "/3vC2igEWcqhxJn9iiIHAzA0mZoS1DOKhgFG\n" +
            "-----END CERTIFICATE-----";
}
