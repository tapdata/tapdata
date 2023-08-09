package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.dto.start.SybaseDstLocalStorage;
import io.tapdata.sybase.cdc.dto.start.SybaseExtConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseGeneralConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseReInitConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.util.ConfigPaths;
import io.tapdata.sybase.util.HostUtils;
import io.tapdata.sybase.util.YamlUtil;
import org.yaml.snakeyaml.DumperOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.map;

/**
 * @author GavinXiao
 * @description ConfigYaml create by Gavin
 * @create 2023/7/13 11:02
 **/
class ConfigYaml implements CdcStep<CdcRoot> {
    public static final String TAG = ConfigYaml.class.getSimpleName();
    List<SybaseFilterConfig> filterConfig;
    SybaseSrcConfig srcConfig;
    SybaseDstLocalStorage sybaseDstLocalStorage;
    SybaseGeneralConfig sybaseGeneralConfig;
    SybaseExtConfig extConfig;
    String configPath;
    CdcRoot root;

    protected ConfigYaml(CdcRoot root, CdcStartVariables variables) {
        this.filterConfig = variables.getFilterConfig();
        this.srcConfig = variables.getSrcConfig();
        this.configPath = root.getSybasePocPath();
        this.sybaseDstLocalStorage = variables.getSybaseDstLocalStorage();
        this.sybaseGeneralConfig = variables.getSybaseGeneralConfig();
        this.extConfig = variables.getExtConfig();
        this.root = root;
    }

    @Override
    public CdcRoot compile() {
        configSybaseFilter();
        configSybaseSrc();
        configDstLocalstorage();
        configGeneral();
        configExt();
        //configEtcHost();
        return root;
    }

    public CdcRoot configSybaseFilter() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(root.getFilterTableConfigPath());
        Object allow = yamlUtil.get("allow");
        if (filterConfig != null && !filterConfig.isEmpty()) {
//            if (allow instanceof Collection) {
//                Collection<Object> collection = (Collection<Object>) allow;
//                collection.clear();
//                collection.addAll(filterConfig);
//                yamlUtil.update();
//            } else {
            Map<String, Object> map = map();
            for (SybaseFilterConfig config : filterConfig) {
                map.putAll((Map<String, Object>) config.toYaml());
            }
            yamlUtil.update(map(entry("allow", list(map))));
//            }
        }
        return this.root;
    }

    private void configSybaseSrc() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(configPath + ConfigPaths.SYBASE_SRC_PATH, DumperOptions.ScalarStyle.DOUBLE_QUOTED);
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = (Map<String, Object>) srcConfig.toYaml();
        } catch (Exception e) {
            throw new RuntimeException("Can not get config from sybase src config set into src_sybasease.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            } else {
                allow.clear();
            }
            allow.putAll(map);
        }

        yamlUtil.update(allow);
    }

    private void configDstLocalstorage() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(configPath + ConfigPaths.DST_LOCAL_STORAGE_PATH, DumperOptions.ScalarStyle.DOUBLE_QUOTED);
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = (Map<String, Object>) sybaseDstLocalStorage.toYaml();
        } catch (Exception e) {
            throw new RuntimeException("Can not get config from sybase src config set into dst_localstorage.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            } else {
                allow.clear();
            }
            allow.putAll(map);
        }
        yamlUtil.update(allow);
    }

    private void configGeneral() {
        YamlUtil yamlUtil = new YamlUtil(configPath + ConfigPaths.GENERAL_CONFIG_PATH, DumperOptions.ScalarStyle.SINGLE_QUOTED);
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = (Map<String, Object>) sybaseGeneralConfig.toYaml();
        } catch (Exception e) {
            throw new RuntimeException("Can not get config from general config set into general.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            } else {
                allow.clear();
            }
            allow.putAll(map);
        }
        yamlUtil.update(allow);
    }

    private void configExt() {
        YamlUtil yamlUtil = new YamlUtil(configPath + ConfigPaths.EXT_CONFIG_PATH, DumperOptions.ScalarStyle.SINGLE_QUOTED);
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = (Map<String, Object>) extConfig.toYaml();
        } catch (Exception e) {
            throw new RuntimeException("Can not get config from general config set into general.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            } else {
                allow.clear();
            }
            allow.putAll(map);
        }
        yamlUtil.update(allow);
    }


    public List<Map<String, Object>> configSybaseFilter(List<Map<String, Object>> filterConfig) {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(root.getFilterTableConfigPath());
        yamlUtil.update(map(entry(SybaseFilterConfig.configKey, filterConfig)));
        return filterConfig;
    }

    public List<LinkedHashMap<String, Object>> configReInitTable(List<SybaseReInitConfig> filterConfig) {
        String taskId = root.getTaskCdcId();
        if (null == taskId) throw new CoreException("Can not get task id when write reinit.yaml");

        String path = String.format(ConfigPaths.RE_INIT_TABLE_CONFIG_PATH, configPath, taskId);
        File reInitYaml = new File(path);
        if (!reInitYaml.exists() || !reInitYaml.isFile()) {
            try {
                boolean newFile = reInitYaml.createNewFile();
            } catch (Exception e) {
                throw new CoreException("Unable create yaml which named is {}, please create by yourself", path);
            }
        }
        YamlUtil yamlUtil = new YamlUtil(path, DumperOptions.ScalarStyle.SINGLE_QUOTED);
        List<LinkedHashMap<String, Object>> list = new ArrayList<>();
        if (filterConfig != null && !filterConfig.isEmpty()) {
            list.addAll(SybaseReInitConfig.fixYaml0(filterConfig));
            yamlUtil.update(map(entry(SybaseReInitConfig.configKey, list)));
        }
        return list;
    }

    private void configEtcHost() {
        try {
            if (!HostUtils.updateHostName("time.google.com", "106.55.184.199")) {
                TapLogger.warn(TAG, "Unable add host config to etc/host or c:.../host  time.google.com:106.55.184.199");
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "Unable add host config to etc/host or c:.../host, msg: {}, time.google.com:106.55.184.199", e.getMessage());
        }
    }
}
