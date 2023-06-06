package com.tapdata.tm.commons.util;

/**
 * @author GavinXiao
 * @description ProcessorNodeType create by Gavin 增强处理器类型，默认default值为0, 标准化js值为1
 * @create 2023/5/16 15:01
 **/
public enum ProcessorNodeType {
    DEFAULT(0),
    Standard_JS(1)
    ;
    int type;
    ProcessorNodeType(int type){
        this.type = type;
    }
    public int type(){
        return type;
    }

    public boolean contrast(int type){
        return this.type == type;
    }
}
