package com.tapdata.tm.log.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.entity.LogEntityElastic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Service;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class LogService {

    @Value("${logWareHouse:mongo}")
    private String logWareHouse;

    @Autowired
    private LogServiceMongo mongo;

    @Autowired(required = false)
    private LogServiceElastic elastic;


    public boolean useElastic() {
        return logWareHouse != null && logWareHouse.equals("elasticsearch") && elastic != null;
    }

    /**
     * Process logDto to ensure date field is properly formatted
     * Convert long timestamp to Date object if needed
     */
    private void processLogDto(LogDto logDto) {
        if (logDto != null && logDto.getDate() != null) {
            Object dateObj = logDto.getDate();

            // If date is a Number (long, int, etc.), convert to Date
            if (dateObj instanceof Number) {
                long timestamp = ((Number) dateObj).longValue();
                Date convertedDate = new Date(timestamp);
                logDto.setDate(convertedDate);
                log.debug("Converted date from timestamp {} to Date {}", timestamp, convertedDate);
            }
            // If date is a String that looks like a timestamp, try to convert
            else if (dateObj instanceof String) {
                String dateStr = (String) dateObj;
                try {
                    // Check if it's a numeric string (timestamp)
                    if (dateStr.matches("\\d+")) {
                        long timestamp = Long.parseLong(dateStr);
                        Date convertedDate = new Date(timestamp);
                        logDto.setDate(convertedDate);
                        log.debug("Converted date from string timestamp {} to Date {}", timestamp, convertedDate);
                    }
                } catch (NumberFormatException e) {
                    log.warn("Failed to parse date string as timestamp: {}", dateStr);
                }
            }
            // If it's already a Date object, no conversion needed
            else if (dateObj instanceof Date) {
                log.debug("Date field is already a Date object: {}", dateObj);
            }
            // For other types, log a warning
            else {
                log.warn("Unexpected date field type: {} with value: {}", dateObj.getClass().getSimpleName(), dateObj);
            }
        }
    }

    public void save(LogDto logDto) {
        // Process date field to ensure proper type
        processLogDto(logDto);

        if (useElastic()) {
            elastic.save(logDto);
        } else {
            mongo.save(logDto);
        }
    }

    public LogDto save(LogDto logDto, UserDetail userDetail) {
        // Process date field to ensure proper type
        processLogDto(logDto);

        // 不写入这个数据了, 没啥用, 而且会存在很大量的情况, 还不知道原因
        if (useElastic()) {
            //return elastic.save(logDto, userDetail);
        } else {
            //mongo.save(logDto, userDetail);
        }
        return logDto;
    }

    public Page<LogDto> find(Filter filter, UserDetail userDetail) {
        if (useElastic()) {
            return elastic.find(filter, userDetail);
        } else {
            return mongo.find(filter, userDetail);
        }
    }

    public void export(Number start, Number end, String dataFlowId, OutputStream outputStream) {
        if (useElastic()) {
            elastic.export(start, end, dataFlowId, outputStream);
        } else {
            mongo.export(start, end, dataFlowId, outputStream);
        }
    }


    // functions blow are added to combine two services together, it may only work
    // under specific service

    public List<LogDto> findAll(org.springframework.data.mongodb.core.query.Query query) {
        if (useElastic()) {
            log.warn("wrong use of the service, should not use `findAll(org.springframework.data.mongodb.core.query.Query query)` when using elasticsearch as log warehouse");
            return new ArrayList<>();
        } else {
            return mongo.findAll(query);
        }
    }

    public CriteriaQuery find(String dataFlowId, String order, Long limit) {
        if (useElastic()) {
            return elastic.find(dataFlowId, order, limit);
        } else {
            log.warn("wrong use of the service, should not use `find(String dataFlowId, String order, Long limit)` when using mongo as log warehouse");
            return null;
        }
    }

    public SearchHits<LogEntityElastic> find(org.springframework.data.elasticsearch.core.query.CriteriaQuery query) {
        if (useElastic()) {
            return elastic.find(query);
        } else {
            log.warn("wrong use of the service, should not use `find(org.springframework.data.elasticsearch.core.query.CriteriaQuery query)` when using mongo as log warehouse");
            return null;
        }
    }

    /**
     * after created data flow
     * @param dataFlowId
     */
    public void afterCreatedDataFlow(String dataFlowId) {

    }

    /**
     * After deleted data flow
     * @param dataFlowId
     */
    public void afterDeletedDataFlow(String dataFlowId) {

        if (useElastic()) {
            elastic.deleteLogsByDataFlowId(dataFlowId);
        }
    }
}
