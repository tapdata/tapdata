package io.tapdata.sybase.util;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class YamlUtil {
    private final Map<String, Object> properties;
    private FileInputStream yamlStream;
    private final File yamlFile;
    private final Yaml yaml;

    public YamlUtil(String yamlPath) {
        yamlFile = new File(yamlPath);
        properties = new HashMap<>();
        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
        dumperOptions.setSplitLines(true);
        yaml = new Yaml(dumperOptions);
        load();
    }

    private void load(){
        try {
            yamlStream = new FileInputStream(yamlFile);
            Optional.ofNullable((Map<String,Object>)yaml.load(yamlStream)).ifPresent(properties::putAll);
        } catch (Exception e) {
            throw new RuntimeException("Can not load yaml from path: " + yamlFile.getAbsolutePath() + ", msg: " + e.getMessage());
        } finally {
            Optional.ofNullable(yamlStream).ifPresent(s -> {
                try {
                    s.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * 获取字段名下的key对应的值
     *
     * @param key     键
     * @return
     */
    public Object get(String key) {
        String[] keys = key.split("\\.");
        return getFromObj(properties, keys, null);
    }
    public Map<String, Object> get() {
        return properties;
    }


    public boolean update(Object value) {
        BufferedWriter buffer = null;
        try {
            FileWriter writer = new FileWriter(yamlFile, false);
            buffer = new BufferedWriter(writer);
            yaml.dump(value, buffer);
            //yaml.dump(value, new FileWriter(yamlFile));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != buffer) {
                    buffer.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            load();
        }
        return false;
    }

    public boolean update() {
        BufferedWriter buffer = null;
        try {
            FileWriter writer = new FileWriter(yamlFile, false);
            buffer = new BufferedWriter(writer);
            yaml.dump(properties, buffer);
            //yaml.dump(properties, new FileWriter(yamlFile));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != buffer) {
                    buffer.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            load();
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
//        YamlUtil yamlUtil = new YamlUtil("D:\\GavinData\\deskTop\\sybase-poc\\config\\sybase2csv\\filter_sybasease.yaml");
//
//        String key = "allow.0.allow";
//        Map<String, Object> allowTables = (Map<String, Object>)yamlUtil.get(key);
//        allowTables.put("test1", new HashMap<>());
//        yamlUtil.update();

        File file  = new File("D:\\GavinData\\deskTop\\sybase-poc\\config\\sybase2csv\\filter_sybasease.yaml");
        String absolutePath = file.getAbsolutePath();
        int indexOf = absolutePath.lastIndexOf('.');
        System.out.println(Files.probeContentType(file.toPath()) + "---" + absolutePath.substring(indexOf + 1) + "---" + file.getName());
        file  = new File("D:\\GavinData\\deskTop\\sybase-poc\\config\\sybase2csv\\csv\\testdb\\tester\\car_claim\\testdb.tester.car_claim.part_0.csv");
        absolutePath = file.getAbsolutePath();
        indexOf = absolutePath.lastIndexOf('.');
        System.out.println(Files.probeContentType(file.toPath()) + "---" + absolutePath.substring(indexOf + 1)+ "---" + file.getName());
        file  = new File("D:\\GavinData\\deskTop\\sybase-poc\\config\\sybase2csv");
        absolutePath = file.getAbsolutePath();
        indexOf = absolutePath.lastIndexOf('.');
        System.out.println(Files.probeContentType(file.toPath()) + "---" + absolutePath.substring(indexOf + 1)+ "---" + file.getName());





    }

    private Object getFromObj(Object config, String[] keys, final Object defaultValue) {
        Object item = config;
        for (String key : keys) {
            if (null == item) return defaultValue;
            if (item instanceof Map) {
                item = getFromMap((Map<String, Object>)item, key, defaultValue);
            } else if (item instanceof Collection) {
                item = getFromList((Collection<Object>) item, key, defaultValue);
            } else if (item.getClass().isArray()) {
                item = getFromArray((Object[]) item, key, defaultValue);
            } else {
                return defaultValue;
            }
        }
        return item;
    }
    private Object getFromMap(Map<String, Object> config, String key, final Object defaultValue){
        return config.getOrDefault(key, defaultValue);
    }
    private Object getFromList(Collection<Object> config, String key, final Object defaultValue){
        if (null == config || config.isEmpty()) return defaultValue;
        try {
            int index = Integer.parseInt(key);
            ArrayList<Object> arrayList = new ArrayList<>(config);
            return index > arrayList.size()-1 ? defaultValue :arrayList.get(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    private Object getFromArray(Object[] config, String key, final Object defaultValue){
        if (null == config || config.length < 1 ) return defaultValue;
        try {
            int index = Integer.parseInt(key);
            return index > config.length - 1 ? defaultValue : config[index];
        } catch (Exception e) {
            return defaultValue;
        }
    }
}