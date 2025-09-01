package com.tapdata.tm.init;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InitControllerTest {

    @Mock
    private UserService userService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private DataSourceDefinitionService dataSourceDefinitionService;

    @Mock
    private LiveDataPlatformService liveDataPlatformService;

    @Mock
    private SettingsService settingsService;

    @InjectMocks
    private InitController initController;

    private UserDetail mockUserDetail;
    private DataSourceDefinitionDto mockDefinitionDto;
    private DataSourceConnectionDto mockConnectionDto;

    @BeforeEach
    void setUp() {
        // 设置测试用的配置值
        ReflectionTestUtils.setField(initController, "mongodbUri", "mongodb://localhost:27017/tapdata_test?authSource=admin");
        ReflectionTestUtils.setField(initController, "ssl", false);
        ReflectionTestUtils.setField(initController, "caPath", "");
        ReflectionTestUtils.setField(initController, "keyPath", "");

        // 创建 mock 对象
        mockUserDetail = new UserDetail("6393f084c162f518b18165c3", "customerId", "admin@admin.com", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        mockUserDetail.setUsername("admin@admin.com");

        mockDefinitionDto = new DataSourceDefinitionDto();
        mockDefinitionDto.setPdkHash("test-pdk-hash");

        mockConnectionDto = new DataSourceConnectionDto();
        mockConnectionDto.setId(new ObjectId());
        mockConnectionDto.setName("FDM");
    }

    @Test
    void testModifyMongodbUriDatabase() throws Exception {
        // 使用反射访问私有方法
        Method method = InitController.class.getDeclaredMethod("modifyMongodbUriDatabase", String.class, String.class);
        method.setAccessible(true);

        // 测试基本的 URI 修改
        String originalUri = "mongodb://localhost:27017/tapdata_355?authSource=admin";
        String result = (String) method.invoke(initController, originalUri, "FDM");
        assertEquals("mongodb://localhost:27017/FDM?authSource=admin", result);

        // 测试带认证信息的 URI
        String uriWithAuth = "mongodb://user:password@localhost:27017/tapdata_355?authSource=admin";
        String resultWithAuth = (String) method.invoke(initController, uriWithAuth, "MDM");
        assertEquals("mongodb://user:password@localhost:27017/MDM?authSource=admin", resultWithAuth);

        // 测试多主机的 URI
        String multiHostUri = "mongodb://host1:27017,host2:27017/tapdata_355?authSource=admin";
        String resultMultiHost = (String) method.invoke(initController, multiHostUri, "ADM");
        assertEquals("mongodb://host1:27017,host2:27017/ADM?authSource=admin", resultMultiHost);

        // 测试没有查询参数的 URI
        String simpleUri = "mongodb://localhost:27017/tapdata_355";
        String resultSimple = (String) method.invoke(initController, simpleUri, "FDM");
        assertEquals("mongodb://localhost:27017/FDM", resultSimple);

        // 测试空值处理
        String resultNull = (String) method.invoke(initController, null, "FDM");
        assertEquals(null, resultNull);

        String resultEmptyDb = (String) method.invoke(initController, originalUri, "");
        assertEquals(originalUri, resultEmptyDb);
    }

    @Test
    void testModifyMongodbUriDatabaseWithInvalidUri() throws Exception {
        Method method = InitController.class.getDeclaredMethod("modifyMongodbUriDatabase", String.class, String.class);
        method.setAccessible(true);

        // 测试无效的 URI，应该返回 null
        String invalidUri = "invalid-uri";
        String result = (String) method.invoke(initController, invalidUri, "FDM");
        assertNull(result);
    }

    @Test
    void testDataSourceConnectionDtoCreation() {
        // 测试 DataSourceConnectionDto 对象的创建和属性设置
        DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();

        // 设置基本属性
        connectionDto.setName("FDM");
        connectionDto.setConnection_type("source_and_target");
        connectionDto.setDatabase_type("MongoDB");
        connectionDto.setPdkHash("test-hash");
        connectionDto.setPdkType("pdk");
        connectionDto.setStatus("testing");
        connectionDto.setRetry(0);
        connectionDto.setProject("");
        connectionDto.setSubmit(true);

        // 设置功能开关
        connectionDto.setShareCdcEnable(false);
        connectionDto.setLoadAllTables(true);
        connectionDto.setOpenTableExcludeFilter(false);
        connectionDto.setHeartbeatEnable(false);

        // 设置节点和调度相关
        connectionDto.setAccessNodeType("AUTOMATIC_PLATFORM_ALLOCATION");
        connectionDto.setSchemaUpdateHour("default");

        // 创建和设置 config 配置
        Map<String, Object> config = new java.util.HashMap<>();
        config.put("isUri", true);
        config.put("ssl", false);
        config.put("mongodbLoadSchemaSampleSize", 1000);
        config.put("schemaLimit", 1024);
        config.put("uri", "mongodb://localhost:27017/FDM");
        config.put("__connectionType", "source_and_target");
        connectionDto.setConfig(config);

        // 验证属性设置
        assertEquals("FDM", connectionDto.getName());
        assertEquals("source_and_target", connectionDto.getConnection_type());
        assertEquals("MongoDB", connectionDto.getDatabase_type());
        assertEquals("test-hash", connectionDto.getPdkHash());
        assertEquals("pdk", connectionDto.getPdkType());
        assertEquals("testing", connectionDto.getStatus());
        assertEquals(0, connectionDto.getRetry());
        assertEquals("", connectionDto.getProject());
        assertTrue(connectionDto.getSubmit());

        assertFalse(connectionDto.getShareCdcEnable());
        assertTrue(connectionDto.getLoadAllTables());
        assertFalse(connectionDto.getOpenTableExcludeFilter());
        assertFalse(connectionDto.getHeartbeatEnable());

        assertEquals("AUTOMATIC_PLATFORM_ALLOCATION", connectionDto.getAccessNodeType());
        assertEquals("default", connectionDto.getSchemaUpdateHour());

        assertNotNull(connectionDto.getConfig());
        assertEquals(true, connectionDto.getConfig().get("isUri"));
        assertEquals(false, connectionDto.getConfig().get("ssl"));
        assertEquals(1000, connectionDto.getConfig().get("mongodbLoadSchemaSampleSize"));
        assertEquals(1024, connectionDto.getConfig().get("schemaLimit"));
        assertEquals("mongodb://localhost:27017/FDM", connectionDto.getConfig().get("uri"));
        assertEquals("source_and_target", connectionDto.getConfig().get("__connectionType"));
    }

    @Test
    void testInit_Success() {
        // 准备测试数据
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Arrays.asList(mockDefinitionDto));
        when(liveDataPlatformService.count(any(Query.class))).thenReturn(0L);

        // 模拟数据源创建成功
        DataSourceConnectionDto fdmConnection = new DataSourceConnectionDto();
        fdmConnection.setId(new ObjectId());
        fdmConnection.setName("FDM");

        DataSourceConnectionDto mdmConnection = new DataSourceConnectionDto();
        mdmConnection.setId(new ObjectId());
        mdmConnection.setName("MDM");

        DataSourceConnectionDto admConnection = new DataSourceConnectionDto();
        admConnection.setId(new ObjectId());
        admConnection.setName("ADM");

        when(dataSourceService.add(any(DataSourceConnectionDto.class), eq(mockUserDetail)))
                .thenReturn(fdmConnection, mdmConnection, admConnection);

        // 执行测试
        initController.init();

        // 验证调用
        verify(settingsService).isCloud();
        verify(userService).loadUserByUsername("admin@admin.com");
        verify(dataSourceDefinitionService).getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash");
        verify(liveDataPlatformService).count(any(Query.class));
        verify(dataSourceService, times(3)).add(any(DataSourceConnectionDto.class), eq(mockUserDetail));
        verify(liveDataPlatformService).save(any(LiveDataPlatformDto.class), eq(mockUserDetail));
    }

    @Test
    void testInit_CloudEnvironment_ShouldReturn() {
        // 准备测试数据 - 云环境
        when(settingsService.isCloud()).thenReturn(true);

        // 执行测试
        initController.init();

        // 验证只调用了 isCloud 检查，其他方法都没有被调用
        verify(settingsService).isCloud();
        verify(userService, never()).loadUserByUsername(anyString());
        verify(dataSourceDefinitionService, never()).getByDataSourceType(any(), any(), anyString());
        verify(liveDataPlatformService, never()).count(any(Query.class));
        verify(dataSourceService, never()).add(any(DataSourceConnectionDto.class), any(UserDetail.class));
    }

    @Test
    void testInit_LiveDataPlatformAlreadyExists_ShouldReturn() {
        // 准备测试数据
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Arrays.asList(mockDefinitionDto));
        when(liveDataPlatformService.count(any(Query.class))).thenReturn(1L); // 已存在

        // 执行测试
        initController.init();

        // 验证调用
        verify(settingsService).isCloud();
        verify(userService).loadUserByUsername("admin@admin.com");
        verify(dataSourceDefinitionService).getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash");
        verify(liveDataPlatformService).count(any(Query.class));
        // 不应该创建数据源
        verify(dataSourceService, never()).add(any(DataSourceConnectionDto.class), any(UserDetail.class));
        verify(liveDataPlatformService, never()).save(any(LiveDataPlatformDto.class), any(UserDetail.class));
    }

    @Test
    void testInit_NoMongoDBDefinition_ShouldReturn() {
        // 准备测试数据 - 没有 MongoDB 定义
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Collections.emptyList());

        // 执行测试
        initController.init();

        // 验证调用
        verify(settingsService).isCloud();
        verify(userService).loadUserByUsername("admin@admin.com");
        verify(dataSourceDefinitionService).getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash");
        // 不应该检查 liveDataPlatform 或创建数据源
        verify(liveDataPlatformService, never()).count(any(Query.class));
        verify(dataSourceService, never()).add(any(DataSourceConnectionDto.class), any(UserDetail.class));
    }

    @Test
    void testInit_InvalidMongodbUri_ShouldReturn() {
        // 准备测试数据 - 无效的 MongoDB URI
        ReflectionTestUtils.setField(initController, "mongodbUri", "invalid-uri");

        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Arrays.asList(mockDefinitionDto));
        when(liveDataPlatformService.count(any(Query.class))).thenReturn(0L);

        // 执行测试
        initController.init();

        // 验证调用 - 由于 URI 无效，应该提前返回
        verify(settingsService).isCloud();
        verify(userService).loadUserByUsername("admin@admin.com");
        verify(dataSourceDefinitionService).getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash");
        verify(liveDataPlatformService).count(any(Query.class));
        // 不应该创建任何数据源
        verify(dataSourceService, never()).add(any(DataSourceConnectionDto.class), any(UserDetail.class));
        verify(liveDataPlatformService, never()).save(any(LiveDataPlatformDto.class), any(UserDetail.class));
    }

    @Test
    void testInit_WithSSL_Success() {
        // 准备测试数据 - 启用 SSL
        ReflectionTestUtils.setField(initController, "ssl", true);
        ReflectionTestUtils.setField(initController, "keyPath", "/path/to/key.pem");
        ReflectionTestUtils.setField(initController, "caPath", "/path/to/ca.pem");

        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Arrays.asList(mockDefinitionDto));
        when(liveDataPlatformService.count(any(Query.class))).thenReturn(0L);

        // 模拟数据源创建成功
        DataSourceConnectionDto fdmConnection = new DataSourceConnectionDto();
        fdmConnection.setId(new ObjectId());
        fdmConnection.setName("FDM");

        DataSourceConnectionDto mdmConnection = new DataSourceConnectionDto();
        mdmConnection.setId(new ObjectId());
        mdmConnection.setName("MDM");

        DataSourceConnectionDto admConnection = new DataSourceConnectionDto();
        admConnection.setId(new ObjectId());
        admConnection.setName("ADM");

        when(dataSourceService.add(any(DataSourceConnectionDto.class), eq(mockUserDetail)))
                .thenReturn(fdmConnection, mdmConnection, admConnection);

        // 执行测试
        initController.init();

        // 验证调用
        verify(dataSourceService, times(3)).add(any(DataSourceConnectionDto.class), eq(mockUserDetail));
        verify(liveDataPlatformService).save(any(LiveDataPlatformDto.class), eq(mockUserDetail));
    }

    @Test
    void testInit_PartialDataSourceCreation_ShouldNotCreateLiveDataPlatform() {
        // 准备测试数据
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(mockUserDetail);
        when(dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"), mockUserDetail, "pdkHash"))
                .thenReturn(Arrays.asList(mockDefinitionDto));
        when(liveDataPlatformService.count(any(Query.class))).thenReturn(0L);

        // 模拟只有 FDM 创建成功，MDM 和 ADM 失败
        DataSourceConnectionDto fdmConnection = new DataSourceConnectionDto();
        fdmConnection.setId(new ObjectId());
        fdmConnection.setName("FDM");

        when(dataSourceService.add(any(DataSourceConnectionDto.class), eq(mockUserDetail)))
                .thenReturn(fdmConnection, null, null); // MDM 和 ADM 返回 null

        // 执行测试
        initController.init();

        // 验证调用
        verify(dataSourceService, times(3)).add(any(DataSourceConnectionDto.class), eq(mockUserDetail));
        // 由于 MDM 创建失败，不应该创建 LiveDataPlatform
        verify(liveDataPlatformService, never()).save(any(LiveDataPlatformDto.class), any(UserDetail.class));
    }


}
