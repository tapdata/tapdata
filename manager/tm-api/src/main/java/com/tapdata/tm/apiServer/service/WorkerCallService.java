package com.tapdata.tm.apiServer.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public interface WorkerCallService {

    default List<WorkerCallEntity> findData(Query query) {
        return new ArrayList<>();
    }
}
