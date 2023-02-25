package io.tapdata.zoho.enums;

public enum  ModuleEnums {
    TICKETS("tickets"),
    CONTACTS("contacts") ,
    ACCOUNTS("accounts") ,
    TASKS("tasks") ,
    CALLS("calls") ,
    EVENTS("events"),
    TIME_ENTRY("timeEntry"),
    PRODUCTS("products"),
    CONTRACTS("contracts"),
    ;

    String name;
    ModuleEnums(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
