package io.tapdata.supervisor.entity;

public enum MemoryLevel {
    DETAIL("Detail"),
    SUMMARY("Summary")
    ;
    String level;
    MemoryLevel(String level){
        this.level = level;
    }

    public String level(){
        return this.level;
    }
}
