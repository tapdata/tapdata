package com.tapdata.tm.init;

import com.mongodb.ConnectionString;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/api/init")
public class InitController {
    @Autowired
    private UserService userService;
    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private DataSourceDefinitionService dataSourceDefinitionService;
    @Autowired
    private LiveDataPlatformService liveDataPlatformService;
    @Autowired
    private SettingsService settingsService;

    @Value("${spring.data.mongodb.default.uri}")
    private String mongodbUri;

    @Value("${spring.data.mongodb.ssl}")
    private boolean ssl;
    @Value("${spring.data.mongodb.caPath}")
    private String caPath;
    @Value("${spring.data.mongodb.keyPath}")
    private String keyPath;



    @GetMapping
    public void init(){
        if(settingsService.isCloud())return;
        UserDetail userDetail = userService.loadUserByUsername("admin@admin.com");
        List<DataSourceDefinitionDto> definitionDtos = dataSourceDefinitionService.getByDataSourceType(Arrays.asList("MongoDB"),userDetail,"pdkHash");
        if(CollectionUtils.isNotEmpty(definitionDtos)){
            long count = liveDataPlatformService.count(Query.query(Criteria.where("mode").is("service")));
            if(count > 0){
                log.info("init skip liveDataPlatform already exist.");
                return;
            }
            String mongodbPdkHash = definitionDtos.get(0).getPdkHash();

            // 初始化 FDM, MDM, ADM 三个数据源
            String[] dataSourceNames = {"FDM", "MDM", "ADM"};
            String fdmStorageConnectionId = null;
            String mdmStorageConnectionId = null;

            for (String dataSourceName : dataSourceNames) {
                String modifiedUri = modifyMongodbUriDatabase(mongodbUri, dataSourceName);
                if(StringUtils.isBlank(modifiedUri))return;
                DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
                connectionDto.setIsInit(true);
                // 设置基本属性
                connectionDto.setName(dataSourceName);
                connectionDto.setConnection_type("source_and_target");
                connectionDto.setDatabase_type("MongoDB");
                connectionDto.setPdkHash(mongodbPdkHash);
                connectionDto.setPdkType("pdk");
                connectionDto.setStatus("testing");
                connectionDto.setRetry(0);
                connectionDto.setNextRetry(null);
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
                Map<String, Object> config = new HashMap<>();
                config.put("isUri", true);
                config.put("mongodbLoadSchemaSampleSize", 1000);
                config.put("schemaLimit", 1024);
                config.put("uri", modifiedUri);
                if(ssl){
                    config.put("ssl", true);

                    // 读取 keyPath 文件内容并转换为字符串
                    if(StringUtils.isNotEmpty(keyPath)){
                        String keyContent = SSLFileUtil.readAndValidatePrivateKey(keyPath);
                        if(StringUtils.isNotEmpty(keyContent)){
                            config.put("sslKey", keyContent);
                            log.info("Successfully loaded SSL private key for {}", dataSourceName);
                        } else {
                            log.warn("Failed to load SSL private key from: {}", keyPath);
                        }
                    }

                    // 读取 caPath 文件内容并转换为字符串
                    if(StringUtils.isNotEmpty(caPath)){
                        config.put("sslValidate", true);
                        String caContent = SSLFileUtil.readAndValidateCertificate(caPath);
                        if(StringUtils.isNotEmpty(caContent)){
                            config.put("sslCA", caContent);
                            log.info("Successfully loaded SSL CA certificate for {}", dataSourceName);
                        } else {
                            log.warn("Failed to load SSL CA certificate from: {}", caPath);
                        }
                    }
                }else{
                    config.put("ssl", false);
                }
                config.put("__connectionType", "source_and_target");
                connectionDto.setConfig(config);
                DataSourceConnectionDto result = dataSourceService.add(connectionDto,userDetail);
                if(null != result && null != result.getId()){
                    if("FDM".equals(dataSourceName)){
                        fdmStorageConnectionId = result.getId().toHexString();
                    }else if("MDM".equals(dataSourceName)){
                        mdmStorageConnectionId = result.getId().toHexString();
                    }
                }
                log.info("init {} connection success", dataSourceName);
            }
            if(StringUtils.isNotEmpty(fdmStorageConnectionId) && StringUtils.isNotEmpty(mdmStorageConnectionId)){
                LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
                liveDataPlatformDto.setMode("service");
                liveDataPlatformDto.setFdmStorageCluster("self");
                liveDataPlatformDto.setFdmStorageConnectionId(fdmStorageConnectionId);
                liveDataPlatformDto.setMdmStorageCluster("self");
                liveDataPlatformDto.setMdmStorageConnectionId(mdmStorageConnectionId);
                liveDataPlatformDto.setInit(true);
                liveDataPlatformService.save(liveDataPlatformDto, userDetail);
                log.info("init liveDataPlatform success");
            }
        }else{
            log.info("init fail mongoDB data source not registered.");
        }
    }

    /**
     * 修改 MongoDB URI 中的数据库名
     * @param originalUri 原始 MongoDB URI
     * @param newDatabaseName 新的数据库名
     * @return 修改后的 MongoDB URI
     */
    private String modifyMongodbUriDatabase(String originalUri, String newDatabaseName) {
        if (StringUtils.isBlank(originalUri) || StringUtils.isBlank(newDatabaseName)) {
            return originalUri;
        }

        try {
            ConnectionString connectionString = new ConnectionString(originalUri);

            // 获取原始 URI 的各个组件
            String scheme = "mongodb://";
            String credentials = "";
            String hosts = String.join(",", connectionString.getHosts());
            String options = "";

            // 处理认证信息
            if (connectionString.getUsername() != null) {
                credentials = connectionString.getUsername();
                if (connectionString.getPassword() != null) {
                    credentials += ":" + new String(connectionString.getPassword());
                }
                credentials += "@";
            }

            // 处理连接选项
            if (originalUri.contains("?")) {
                String[] parts = originalUri.split("\\?", 2);
                if (parts.length > 1) {
                    options = "?" + parts[1];
                }
            }

            // 构建新的 URI
            return scheme + credentials + hosts + "/" + newDatabaseName + options;

        } catch (Exception e) {
            log.warn("Failed to modify MongoDB URI database name, using original URI. Error: {}", e.getMessage());
            return null;
        }
    }

}
