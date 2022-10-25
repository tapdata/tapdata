package io.tapdata.coding.service.openApi;

import java.util.Map;

public enum Action {
    CreateIssue("CreateIssue"),
    CreateProjectMember("CreateProjectMember"),
    CreateIteration("CreateIteration"),

    ;
    String name;
    String table;
    Action(String name){
        this.name = name;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void putAction(Map<String,Object> map){
        if (null != map) {
            map.put("Action", name);
        }
    }
}
