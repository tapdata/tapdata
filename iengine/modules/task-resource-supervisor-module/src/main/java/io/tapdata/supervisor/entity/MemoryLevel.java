package io.tapdata.supervisor.entity;

import java.util.Arrays;
import java.util.Objects;

public enum MemoryLevel {
    ALL("All","All"),//全部
    DETAIL("Detail","All and detail"),//全部详情
    SUMMARY("(Summary_)(.*)", "All and outline", m -> m.replace("Summary_","")),//全部简要
    DETAIL_CONNECTOR("(Connectors_)(.*)", "Filter with associateId %s", m -> m.replace("Connectors_","")),//部分详情
    ;
    String level;
    private Memory memory;
    String desc;
    MemoryLevel(String level, String desc) {
        this.level = level;
        this.memory = l->l;
        this.desc = desc;
    }
    MemoryLevel(String level, String desc, Memory memory) {
        this.level = level;
        this.memory = memory;
        this.desc = desc;
    }

    public String level(String type){
        return this.memory.memory(type);
    }

    public String level() {
        return this.level;
    }

    public static String description(String level){
        if (Objects.isNull(level)){
            return ALL.desc;
        }
        if (ALL.level().equals(level) ) {
            return ALL.desc;
        }else if (DETAIL.level().equals(level)){
            return DETAIL.desc;
        }else if(level.matches(SUMMARY.level())){
            return SUMMARY.desc;
        }else if(level.matches(DETAIL_CONNECTOR.level())){
            return String.format(DETAIL_CONNECTOR.desc, Arrays.toString(DETAIL_CONNECTOR.level(level).split(",")));
        }else {
            return ALL.desc;
        }
    }
    public static boolean needMemory(String level){
        if (Objects.isNull(level)){
            return Boolean.FALSE;
        }
        if (ALL.level().equals(level) ) {
            return Boolean.FALSE;
        }else if (DETAIL.level().equals(level) || level.matches(SUMMARY.level()) ||level.matches(DETAIL_CONNECTOR.level())){
            return Boolean.TRUE;
        }else {
            return Boolean.FALSE;
        }
    }

    public static Boolean filter(String memoryLevel, String key) {
        if (Objects.isNull(memoryLevel)) return null;
        if (ALL.level().equals(memoryLevel) || DETAIL.level().equals(memoryLevel) || memoryLevel.matches(SUMMARY.level())) {
            return Boolean.TRUE;
        } else if (memoryLevel.matches(DETAIL_CONNECTOR.level())) {
            if (Objects.isNull(key)) return null;
            String[] filterKeys = DETAIL_CONNECTOR.level(memoryLevel).split(",");
            for (String filterKey : filterKeys) {
                if (key.trim().equals(filterKey)){
                    return Boolean.TRUE;
                }
            }
        }
        return Boolean.FALSE;
    }
    interface Memory{
        public String memory(String level);
    }

    public static void main(String[] args) {
        String regx = "(Connectors_)(.*)";
        String str = "dsfsudds,fndsjgskdj,dnfsjngjds.shbabj";
        System.out.println(str + ":" + str.matches(regx));
        str = "Connectors_dsfsudds,fndsjgskdj,dnfsjngjds.shbabj";
        System.out.println(str + ":" + str.matches(regx));
        str = "Connectors_";
        System.out.println(str + ":" + str.matches(regx));
        str = "Connectors_dsfsudds.fndsjgskdj.dnfsjngjds.shbabj";
        System.out.println(str + ":" + str.matches(regx));
        str = "Connectors_dsfsudds,fndsjgskdj,dnfsjngjds.shbabj,";
        System.out.println(str + ":" + str.matches(regx));
        str = "Connectors_dsfsudds,,,,fndsjgskdj,dnfsjngjds.shbabj";
        System.out.println(str + ":" + str.matches(regx));
    }
}
