package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.SybaseDstLocalStorage;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseGeneralConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.util.HostUtils;
import io.tapdata.sybase.util.Utils;
import io.tapdata.sybase.util.YamlUtil;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    String configPath;
    CdcRoot root;

    protected ConfigYaml(CdcRoot root,
                         List<SybaseFilterConfig> filterConfig,
                         SybaseSrcConfig srcConfig,
                         SybaseDstLocalStorage sybaseDstLocalStorage,
                         SybaseGeneralConfig sybaseGeneralConfig) {
        this.filterConfig = filterConfig;
        this.srcConfig = srcConfig;
        this.configPath = root.getSybasePocPath();
        this.sybaseDstLocalStorage = sybaseDstLocalStorage;
        this.sybaseGeneralConfig = sybaseGeneralConfig;
        this.root = root;
    }

    @Override
    public CdcRoot compile() {
        configSybaseFilter();
        configSybaseSrc();
        configDstLocalstorage();
        configGeneral();
        configEtcHost();
        return root;
    }

    private void configSybaseFilter() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(configPath + "/config/sybase2csv/filter_sybasease.yaml");
        Object allow = yamlUtil.get("allow");
        if (filterConfig != null && !filterConfig.isEmpty()) {
            if (allow instanceof Collection) {
                Collection<Object> collection = (Collection<Object>) allow;
                collection.addAll(filterConfig);
                yamlUtil.update();
            } else {
                yamlUtil.update(filterConfig);
            }
        }
    }

    private void configSybaseSrc() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(configPath + "/config/sybase2csv/src_sybasease.yaml");
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = Utils.obj2Map(srcConfig);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can not get config from sybase src config set into src_sybasease.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) continue;
                allow.put(key.contains("_") ? key.replaceAll("_", "-") : key, value);
            }
        }
        yamlUtil.update(allow);
    }

    private void configDstLocalstorage() {
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(configPath + "/config/sybase2csv/dst_localstorage.yaml");
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = Utils.obj2Map(sybaseDstLocalStorage);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can not get config from sybase src config set into dst_localstorage.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) continue;
                allow.put(key.contains("_") ? key.replaceAll("_", "-") : key, value);
            }
        }
        yamlUtil.update(allow);
    }

    private void configGeneral() {
        YamlUtil yamlUtil = new YamlUtil(configPath + "/config/sybase2csv/general.yaml");
        Map<String, Object> allow = yamlUtil.get();
        Map<String, Object> map;
        try {
            map = Utils.obj2Map(sybaseGeneralConfig);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can not get config from general config set into general.yaml");
        }
        if (!map.isEmpty()) {
            if (allow == null) {
                allow = new LinkedHashMap<>();
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) continue;
                allow.put(key.contains("_") ? key.replaceAll("_", "-") : key, value);
            }
        }
        yamlUtil.update(allow);
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
