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
                mb.when(() -> SettingUtil.getValue("Active_Directory", "ad.bind.password")).thenReturn("123456");
                testAdDto.setLdap_Bind_Password("*****");
                when(userService.buildDirContext(any(LdapLoginDto.class))).thenReturn(dirContext);
                TestResponseDto response = userService.testLoginByLdap(testAdDto);
                assertTrue(response.isResult());
                assertNull(response.getStack());
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
    }
}
