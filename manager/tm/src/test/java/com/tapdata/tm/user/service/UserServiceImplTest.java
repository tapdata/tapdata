package com.tapdata.tm.user.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.repository.UserRepository;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.user.repository.UserRepository;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UserServiceImplTest {
    private UserServiceImpl userService;
    private RoleMappingService roleMappingService;
    private SettingsService settingsService;
    private RoleService roleService;
    private PermissionService permissionService;
    private UserLogService userLogService;
    private LdpService ldpService;
    private UserRepository repository;

    @BeforeEach
    void beforeEach(){
        userService = mock(UserServiceImpl.class);
        roleMappingService = mock(RoleMappingService.class);
        settingsService = mock(SettingsService.class);
        roleService = mock(RoleService.class);
        permissionService = mock(PermissionService.class);
        userLogService = mock(UserLogService.class);
        ldpService = mock(LdpService.class);
        repository = mock(UserRepository.class);
        ReflectionTestUtils.setField(userService, "repository", repository);
        ReflectionTestUtils.setField(userService, "settingsService", settingsService);
        ReflectionTestUtils.setField(userService, "roleMappingService", roleMappingService);
        ReflectionTestUtils.setField(userService, "roleService", roleService);
        ReflectionTestUtils.setField(userService, "permissionService", permissionService);
        ReflectionTestUtils.setField(userService, "userLogService", userLogService);
        ReflectionTestUtils.setField(userService, "ldpService", ldpService);
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
        @Disabled
        @SneakyThrows
        void testLoginByAD_NPE() {
            try (MockedStatic<SettingUtil> mb = Mockito
                    .mockStatic(SettingUtil.class)) {
                mb.when(() -> SettingUtil.getValue("LDAP", "ldap.bind.password")).thenReturn("123456");
                testAdDto.setLdap_Bind_Password("*****");
                when(userService.buildDirContext(any(LdapLoginDto.class))).thenThrow(new RuntimeException("please check ldap configuration, such as bind dn or password"));
                TestResponseDto response = userService.testLoginByLdap(testAdDto);
                assertFalse(response.isResult());
                assertTrue(response.getStack().contains("please check ldap configuration, such as bind dn or password"));
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
        void testLoginByAD_NamingException() {
            when(settingsService.findAll()).thenReturn(settingsList);
            when(userService.searchUser(any(LdapLoginDto.class), eq(username))).thenReturn(true);
            when(userService.buildDirContext(any(LdapLoginDto.class))).thenThrow(new BizException("AD.Login.Fail"));
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
        public void testBuildDirContext_NoSSL() {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .bindDN("cn=admin,dc=example,dc=com")
                    .password("password")
                    .sslEnable(false)
                    .build();
            assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
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
            assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
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
            assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
        }

        @Test
        public void testBuildDirContext_NPE() {
            LdapLoginDto adLoginDto = LdapLoginDto.builder()
                    .ldapUrl("ldap://example.com:389")
                    .bindDN("cn=admin,dc=example,dc=com")
                    .password(null)
                    .sslEnable(false)
                    .build();
            RuntimeException exception = assertThrows(RuntimeException.class, () -> userService.buildDirContext(adLoginDto));
            assertEquals("please check ldap configuration, such as bind dn or password", exception.getMessage());
        }

        @Test
        @SneakyThrows
        public void testBuildDirContext_WrongPassword() {
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
            when(userService.createSSLContext(any(InputStream.class))).thenThrow(new NamingException("Error code 49"));
            when(sslContext.getSocketFactory()).thenReturn(mock(SSLSocketFactory.class));
            BizException exception = assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
            assertEquals("AD.Login.WrongPassword", exception.getErrorCode());
        }

        @Test
        @SneakyThrows
        public void testBuildDirContext_InvalidCert() {
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
            when(userService.createSSLContext(any(InputStream.class))).thenThrow(new NamingException("TLS handshake"));
            when(sslContext.getSocketFactory()).thenReturn(mock(SSLSocketFactory.class));
            BizException exception = assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
            assertEquals("AD.Login.InvalidCert", exception.getErrorCode());
        }

        @Test
        @SneakyThrows
        public void testBuildDirContext_Retryable() {
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
            when(userService.createSSLContext(any(InputStream.class))).thenThrow(new NamingException("No subject alternative dns"));
            when(sslContext.getSocketFactory()).thenReturn(mock(SSLSocketFactory.class));
            BizException exception = assertThrows(BizException.class, () -> userService.buildDirContext(adLoginDto));
            assertEquals("AD.Login.Retryable", exception.getErrorCode());
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


    @Nested
    class GetUserDetailTest{

        @Test
        void test_main(){
            doCallRealMethod().when(userService).getUserDetail(anyString());
            UserDto userDto = new UserDto();
            userDto.setCreateAt(new Date());
            when(userService.findById(any())).thenReturn(userDto);
            List<RoleMappingDto> roleMappingDtoList =  new ArrayList<>();
            RoleMappingDto roleMappingDto = new RoleMappingDto();
            ObjectId id = new ObjectId();
            roleMappingDto.setRoleId(id);
            roleMappingDtoList.add(roleMappingDto);
            List<RoleDto> roleDtos = new ArrayList<>();
            RoleDto roleDto = new RoleDto();
            roleDto.setId(id);
            roleDtos.add(roleDto);
            when(roleService.findAll(any(Query.class))).thenReturn(roleDtos);
            when(roleMappingService.getUser(any(),any())).thenReturn(roleMappingDtoList);
            List<PermissionDto> permissionDtos = new ArrayList<>();
            PermissionDto permissionDto = new PermissionDto();
            permissionDto.setId("test");
            permissionDtos.add(permissionDto);
            when(permissionService.getCurrentPermission(anyString())).thenReturn(permissionDtos);
            UserDto result = userService.getUserDetail("test");
            Assertions.assertEquals(1,result.getPermissions().size());
            Assertions.assertEquals(1,result.getRoleMappings().size());
        }

        @Test
        void test_roleDtosIsNull(){
            doCallRealMethod().when(userService).getUserDetail(anyString());
            UserDto userDto = new UserDto();
            userDto.setCreateAt(new Date());
            when(userService.findById(any())).thenReturn(userDto);
            List<RoleMappingDto> roleMappingDtoList =  new ArrayList<>();
            RoleMappingDto roleMappingDto = new RoleMappingDto();
            ObjectId id = new ObjectId();
            roleMappingDto.setRoleId(id);
            roleMappingDtoList.add(roleMappingDto);
            when(roleService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(roleMappingService.getUser(any(),any())).thenReturn(roleMappingDtoList);
            List<PermissionDto> permissionDtos = new ArrayList<>();
            PermissionDto permissionDto = new PermissionDto();
            permissionDto.setId("test");
            permissionDtos.add(permissionDto);
            when(permissionService.getCurrentPermission(anyString())).thenReturn(permissionDtos);
            UserDto result = userService.getUserDetail("test");
            Assertions.assertEquals(1,result.getPermissions().size());
            Assertions.assertEquals(1,result.getRoleMappings().size());
        }

        @Test
        void test_roleMappingAndPermissionIsEmpty(){
            doCallRealMethod().when(userService).getUserDetail(anyString());
            UserDto userDto = new UserDto();
            userDto.setCreateAt(new Date());
            when(userService.findById(any())).thenReturn(userDto);
            when(roleMappingService.getUser(any(),any())).thenReturn(new ArrayList<>());
            when(permissionService.getCurrentPermission(anyString())).thenReturn(new ArrayList<>());
            UserDto result = userService.getUserDetail("test");
            Assertions.assertNull(result.getRoleMappings());
        }


    }

    @Nested
    class getterTest {
        @BeforeEach
        void beforeEach() {
            userService = new UserServiceImpl(mock(UserRepository.class));
        }

        @Test
        void isSslTest() {
            assertFalse(Boolean.getBoolean(userService.isSsl()));
        }

        @Test
        void getCaPathTest() {
            String caPath = userService.getCaPath();
            assertNull(caPath);
        }

        @Test
        void getKeyPathTest() {
            String keyPath = userService.getKeyPath();
            assertNull(keyPath);
        }
    }

    @Test
    void testUpdateUserSetting() {
        String id = "675fa0e310853b4b042db50c";
        String settingJson = "{\"id\":\"671b091f4193690843a27c9a\",\"username\":\"test\",\"email\":\"test@tapdata.io\",\"password\":\"\",\"roleusers\":[\"671b07fd4193690843a27bd4\"],\"status\":\"activated\",\"emailVerified\":true,\"account_status\":1,\"emailVerified_from_frontend\":true}";
        UserDetail userDetail = mock(UserDetail.class);
        Locale locale = new Locale("zh_CN");
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        when(repository.getMongoOperations()).thenReturn(mongoTemplate);
        when(userDetail.getUserId()).thenReturn("671b091f4193690843a27c9a");
        UserDto userDto = mock(UserDto.class);
        when(userService.findById(any(ObjectId.class))).thenReturn(userDto);
        when(userDto.getUserId()).thenReturn("671b091f4193690843a27c9a");
        when(userDto.getEmail()).thenReturn("test@tapdata.io");
        doCallRealMethod().when(userService).updateUserSetting(id, settingJson, userDetail, locale);
        userService.updateUserSetting(id, settingJson, userDetail, locale);
        verify(userLogService, new Times(1)).addUserLog(Modular.USER, Operation.UPDATE, "671b091f4193690843a27c9a", "671b091f4193690843a27c9a", "test@tapdata.io");
    }

    @Nested
    class saveTest {
        UserServiceImpl userServiceImpl;
        @BeforeEach
        void beforeEach() {
            userServiceImpl = mock(UserServiceImpl.class);
            ReflectionTestUtils.setField(userServiceImpl, "repository", repository);
            ReflectionTestUtils.setField(userServiceImpl, "ldpService", ldpService);
            ReflectionTestUtils.setField(userServiceImpl, "userLogService", userLogService);
        }
        @Test
        void testWithLdapAccount() {
            CreateUserRequest request = mock(CreateUserRequest.class);
            UserDetail userDetail = mock(UserDetail.class);
            User save = mock(User.class);
            when(repository.save(any(User.class), any(UserDetail.class))).thenReturn(save);
            ReflectionTestUtils.setField(userServiceImpl, "dtoClass", UserDto.class);
            when(userServiceImpl.convertToDto(save, UserDto.class)).thenReturn(mock(UserDto.class));
            when(save.getId()).thenReturn(new ObjectId());
            when(save.getUserId()).thenReturn("675fa0e310853b4b042db50c");
            when(save.getLdapAccount()).thenReturn("test");
            when(save.getSource()).thenReturn("createLdap");
            when(userServiceImpl.getUserDetail(any(User.class))).thenReturn(userDetail);
            doCallRealMethod().when(userServiceImpl).save(request, userDetail);
            userServiceImpl.save(request, userDetail);
            verify(userLogService, new Times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, "675fa0e310853b4b042db50c", "test", true);
        }
        @Test
        void testWithEmail() {
            CreateUserRequest request = mock(CreateUserRequest.class);
            UserDetail userDetail = mock(UserDetail.class);
            User save = mock(User.class);
            when(repository.save(any(User.class), any(UserDetail.class))).thenReturn(save);
            ReflectionTestUtils.setField(userServiceImpl, "dtoClass", UserDto.class);
            when(userServiceImpl.convertToDto(save, UserDto.class)).thenReturn(mock(UserDto.class));
            when(save.getId()).thenReturn(new ObjectId());
            when(save.getUserId()).thenReturn("675fa0e310853b4b042db50c");
            when(save.getEmail()).thenReturn("test");
            when(save.getSource()).thenReturn("create");
            when(userServiceImpl.getUserDetail(any(User.class))).thenReturn(userDetail);
            doCallRealMethod().when(userServiceImpl).save(request, userDetail);
            userServiceImpl.save(request, userDetail);
            verify(userLogService, new Times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, "675fa0e310853b4b042db50c", "test", false);
        }
    }

    @Nested
    class deleteTest {
        String id = "675fa0e310853b4b042db50c";
        UserDetail userDetail = mock(UserDetail.class);
        UpdateResult updateResult;
        @BeforeEach
        void beforeEach() {
            when(userDetail.getUserId()).thenReturn("66ea9f7af4ec565576fc87ab");
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            when(repository.getMongoOperations()).thenReturn(mongoTemplate);
            updateResult = mock(UpdateResult.class);
            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), any(Class.class))).thenReturn(updateResult);
        }
        @Test
        void testDeleted() {
            when(updateResult.getModifiedCount()).thenReturn(1L);
            UserDto userDto = mock(UserDto.class);
            when(userService.findById(any(ObjectId.class), any(Field.class))).thenReturn(userDto);
            when(userDto.getLdapAccount()).thenReturn("test");
            doCallRealMethod().when(userService).delete(id, userDetail);
            userService.delete(id, userDetail);
            verify(userLogService, new Times(1)).addUserLog(Modular.USER, Operation.DELETE, "66ea9f7af4ec565576fc87ab", id, "test");
        }
        @Test
        void testDeleteFailed() {
            when(updateResult.getModifiedCount()).thenReturn(0L);
            UserDto userDto = mock(UserDto.class);
            when(userService.findById(any(ObjectId.class), any(Field.class))).thenReturn(userDto);
            when(userDto.getEmail()).thenReturn("test");
            doCallRealMethod().when(userService).delete(id, userDetail);
            userService.delete(id, userDetail);
            verify(userLogService, new Times(0)).addUserLog(Modular.USER, Operation.DELETE, "66ea9f7af4ec565576fc87ab", id, "test");
        }
        @Test
        void testDeleteWithEmail() {
            when(updateResult.getModifiedCount()).thenReturn(1L);
            UserDto userDto = mock(UserDto.class);
            when(userService.findById(any(ObjectId.class), any(Field.class))).thenReturn(userDto);
            when(userDto.getEmail()).thenReturn("test");
            doCallRealMethod().when(userService).delete(id, userDetail);
            userService.delete(id, userDetail);
            verify(userLogService, new Times(1)).addUserLog(Modular.USER, Operation.DELETE, "66ea9f7af4ec565576fc87ab", id, "test");
        }
    }

    @Nested
    class updatePermissionRoleMappingTest {
        @Test
        void testAdds() {
            UpdatePermissionRoleMappingDto dto = new UpdatePermissionRoleMappingDto();
            List<RoleMappingDto> adds = new ArrayList<>();
            RoleMappingDto roleMappingDto1 = new RoleMappingDto();
            roleMappingDto1.setRoleId(new ObjectId());
            roleMappingDto1.setPrincipalId("111");
            adds.add(roleMappingDto1);
            List<RoleMappingDto> deletes = new ArrayList<>();
            dto.setAdds(adds);
            dto.setDeletes(deletes);
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(userService).updatePermissionRoleMapping(dto, userDetail);
            userService.updatePermissionRoleMapping(dto, userDetail);
            verify(roleMappingService, new Times(1)).addUserLogIfNeed(dto.getAdds(), userDetail);
        }
        @Test
        void testDeletes() {
            UpdatePermissionRoleMappingDto dto = new UpdatePermissionRoleMappingDto();
            List<RoleMappingDto> adds = new ArrayList<>();
            List<RoleMappingDto> deletes = new ArrayList<>();
            RoleMappingDto roleMappingDto2 = new RoleMappingDto();
            roleMappingDto2.setRoleId(new ObjectId());
            roleMappingDto2.setPrincipalId("222");
            deletes.add(roleMappingDto2);
            dto.setAdds(adds);
            dto.setDeletes(deletes);
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(userService).updatePermissionRoleMapping(dto, userDetail);
            userService.updatePermissionRoleMapping(dto, userDetail);
            verify(roleMappingService, new Times(1)).addUserLogIfNeed(dto.getDeletes(), userDetail);
        }
    }

    @Nested
    class refreshAccessCodeTest {
        UserDetail userDetail;
        @BeforeEach
        void beforeEach() {
            userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("62bc5008d4958d013d97c7a6");
        }
        @Test
        void testWhenCodeIsEmpty() {
            when(userService.randomHexString()).thenReturn("");
            doCallRealMethod().when(userService).refreshAccessCode(userDetail);
            BizException exception = assertThrows(BizException.class, () -> userService.refreshAccessCode(userDetail));
            assertEquals("AccessCode.Is.Null", exception.getErrorCode());
        }
        @Test
        void testRefreshAccessCodeNormal() {
            doCallRealMethod().when(userService).randomHexString();
            UserDto userDto = mock(UserDto.class);
            String accessCode = "b4b7fe8a499f65786764fe3654b37c48";
            when(userDto.getAccessCode()).thenReturn(accessCode);
            UpdateResult updateResult = mock(UpdateResult.class);
            when(userService.update(any(Query.class), any(Update.class))).thenReturn(updateResult);
            when(updateResult.getModifiedCount()).thenReturn(1L);
            when(userService.findById(any(ObjectId.class), any(Field.class))).thenReturn(userDto);
            doCallRealMethod().when(userService).refreshAccessCode(userDetail);
            String actual = userService.refreshAccessCode(userDetail);
            assertEquals(accessCode, actual);
            verify(userLogService, new Times(1)).addUserLog(Modular.ACCESS_CODE, Operation.UPDATE, userDetail.getUserId(), userDto.getUserId(), userDto.getAccessCode());
        }
        @Test
        void testRefreshAccessCodeNoUpdate() {
            doCallRealMethod().when(userService).randomHexString();
            UserDto userDto = mock(UserDto.class);
            String accessCode = "b4b7fe8a499f65786764fe3654b37c48";
            when(userDto.getAccessCode()).thenReturn(accessCode);
            UpdateResult updateResult = mock(UpdateResult.class);
            when(userService.update(any(Query.class), any(Update.class))).thenReturn(updateResult);
            when(updateResult.getModifiedCount()).thenReturn(0L);
            when(userService.findById(any(ObjectId.class), any(Field.class))).thenReturn(userDto);
            doCallRealMethod().when(userService).refreshAccessCode(userDetail);
            String actual = userService.refreshAccessCode(userDetail);
            assertEquals(accessCode, actual);
            verify(userLogService, new Times(0)).addUserLog(Modular.ACCESS_CODE, Operation.UPDATE, userDetail.getUserId(), userDto.getUserId(), userDto.getAccessCode());
        }
    }

    @Nested
    class checkLoginSingleSessionEnable {
        @BeforeEach
        void beforeEach() {
            doCallRealMethod().when(userService).checkLoginSingleSessionEnable();
        }

        @Test
        @DisplayName("test checkLoginSingleSessionEnable method when open is true")
        void test1() {
            Settings settings = new Settings();
            settings.setOpen(true);
            when(settingsService.getByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.LOGIN_SINGLE_SESSION)).thenReturn(settings);
            boolean result = userService.checkLoginSingleSessionEnable();
            assertTrue(result);
        }

        @Test
        @DisplayName("test checkLoginSingleSessionEnable method when settings is null")
        void test2() {
            when(settingsService.getByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.LOGIN_SINGLE_SESSION)).thenReturn(null);
            boolean result = userService.checkLoginSingleSessionEnable();
            assertFalse(result);
        }
    }
}

