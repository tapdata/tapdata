package io.tapdata.common.postman.entity;

import java.util.ArrayList;
import java.util.List;

public class ApiEvent<T> extends ArrayList {
    public static ApiEvent create(){
        return new ApiEvent();
    }
    public static ApiEvent create(List event){
        ApiEvent eventList = new ApiEvent();
        eventList.addAll(event);
        return eventList;
    }

}
