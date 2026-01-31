package com.tapdata.tm.worker.entity.field;

import lombok.Getter;

/**
 * @see com.tapdata.tm.worker.entity.Worker
 * */
@Getter
public enum WorkerType {
    API_SERVER("api-server"),

    ;
    final String type;
    WorkerType(String type) {
        this.type = type;
    }
}
