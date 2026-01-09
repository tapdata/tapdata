package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 10:11 Create
 * @description
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class ApiMetricsRawService {
    private ApiMetricsRepository repository;


    public List<ApiMetricsRaw> find(Query query) {
        return repository.findAll(query);
    }
}
