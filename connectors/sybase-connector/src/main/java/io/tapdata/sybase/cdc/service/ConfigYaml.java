package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.*;
import io.tapdata.sybase.util.ConfigPaths;
import io.tapdata.sybase.util.ConnectorUtil;
import io.tapdata.sybase.util.HostUtils;
import io.tapdata.sybase.util.YamlUtil;
import org.yaml.snakeyaml.DumperOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.entry;
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
    //SybaseLocalStrange sybaseLocalStrange;
    CdcRoot root;

    protected ConfigYaml(CdcRoot root, CdcStartVariables variables) {
        this.filterConfig = variables.getFilterConfig();
        this.srcConfig = variables.getSrcConfig();
        this.configPath = root.getSybasePocPath();
        this.sybaseDstLocalStorage = variables.getSybaseDstLocalStorage();
        this.sybaseGeneralConfig = variables.getSybaseGeneralConfig();
        //this.sybaseLocalStrange = variables.getSybaseLocalStrange();
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
        //configSybaseLocalStrange();
        return root;
    }

    public static final String POC_TEMP_CONFIG_PATH = "sybase-poc-temp/%s/sybase-poc/config/sybase2csv";
    public String getFilterTableConfigPath() {
        return new File(String.format(POC_TEMP_CONFIG_PATH, ConnectorUtil.getCurrentInstanceHostPortFromConfig(root.getContext()))).getAbsolutePath();
    }
    public CdcRoot configSybaseFilter() {
        this.root.checkStep();
        ConnectorUtil.createFile(getFilterTableConfigPath(),"filter_sybasease.yaml", root.getContext().getLog());
        YamlUtil yamlUtil = new YamlUtil(root.getFilterTableConfigPath());
        Object allow = yamlUtil.get("allow");
        if (filterConfig != null && !filterConfig.isEmpty()) {
//            if (allow instanceof Collection) {
//                Collection<Object> collection = (Collection<Object>) allow;
//                collection.clear();
//                collection.addAll(filterConfig);
//                yamlUtil.update();
//            } else {
            List<Map<String, Object>> list = new ArrayList<>();
            for (SybaseFilterConfig config : filterConfig) {
                list.add((Map<String, Object>) config.toYaml());
            }
            yamlUtil.update(map(entry("allow", list)));
//            }
        }
        return this.root;
    }

    private void configSybaseSrc() {
        this.root.checkStep();
        ConnectorUtil.createFile( configPath + "/config/sybase2csv","src_sybasease.yaml", root.getContext().getLog());
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
        ConnectorUtil.createFile( configPath + "/config/sybase2csv","dst_localstorage.yaml", root.getContext().getLog());
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
        ConnectorUtil.createFile( configPath + "/config/sybase2csv","general.yaml", root.getContext().getLog());
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
        ConnectorUtil.createFile( configPath + "/config/sybase2csv","ext_sybasease.yaml", root.getContext().getLog());
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

//    private void configSybaseLocalStrange() {
//        ConnectorUtil.createFile( configPath + "/config/sybase2csv","localstorage.yaml", root.getContext().getLog());
//        YamlUtil yamlUtil = new YamlUtil(configPath + ConfigPaths.LOCAL_STRANGE_PATH, DumperOptions.ScalarStyle.SINGLE_QUOTED);
//        Map<String, Object> allow = yamlUtil.get();
//        Map<String, Object> map;
//        try {
//            map = (Map<String, Object>) sybaseLocalStrange.toYaml();
//        } catch (Exception e) {
//            throw new RuntimeException("Can not get config from local strange config set into localstrange.yaml");
//        }
//        if (!map.isEmpty()) {
//            if (allow == null) {
//                allow = new LinkedHashMap<>();
//            } else {
//                allow.clear();
//            }
//            allow.putAll(map);
//        }
//        yamlUtil.update(allow);
//    }


    public List<Map<String, Object>> configSybaseFilter(List<Map<String, Object>> filterConfig) {
        ConnectorUtil.createFile(getFilterTableConfigPath(),"filter_sybasease.yaml", root.getContext().getLog());
        this.root.checkStep();
        YamlUtil yamlUtil = new YamlUtil(root.getFilterTableConfigPath());
        yamlUtil.update(map(entry(SybaseFilterConfig.configKey, filterConfig)));
        return filterConfig;
    }

    public List<LinkedHashMap<String, Object>> configReInitTable(List<SybaseReInitConfig> filterConfig) {
        String taskId = root.getTaskCdcId();
        if (null == taskId) throw new CoreException("Can not get task id when write sybasease_reinit.yaml");

        String path = String.format(ConfigPaths.RE_INIT_TABLE_CONFIG_PATH, configPath, taskId);
        ConnectorUtil.createFile( path,"sybasease_reinit.yaml", root.getContext().getLog());

//        File reInitYaml = new File(path);
//        if (!reInitYaml.exists() || !reInitYaml.isFile()) {
//            try {
//                String path0 = configPath + "/config/sybase2csv/task/" + taskId;
//                File file = new File(path0);
//                if (!file.exists()) {
//                    //ConnectorUtil.execCmd("mkdir " + path0, "Unable create dir which named is " + path0, root.getContext().getLog());
//                    file.mkdirs();
//                }
//                //ConnectorUtil.execCmd("touch " + path, "Unable create yaml which named is " + path, root.getContext().getLog());
//                boolean newFile = reInitYaml.createNewFile();
//            } catch (Exception e) {
//                throw new CoreException("Unable create yaml which named is {}, please create by yourself, {}", path, e.getMessage());
//            }
//        }
        YamlUtil yamlUtil = new YamlUtil(path + "/sybasease_reinit.yaml", DumperOptions.ScalarStyle.SINGLE_QUOTED);
        List<LinkedHashMap<String, Object>> list = new ArrayList<>();
        if (filterConfig != null && !filterConfig.isEmpty()) {
            list.addAll(ConnectorUtil.fixYaml0(filterConfig));
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
