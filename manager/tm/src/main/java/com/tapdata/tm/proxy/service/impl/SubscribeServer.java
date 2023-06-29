package com.tapdata.tm.proxy.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.modules.api.net.entity.SubscribeURLToken;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author GavinXiao
 * @description SubscribeServer create by Gavin
 * @create 2023/6/14 19:38
 **/
@Service
@Slf4j
public class SubscribeServer {

    @Autowired
    private DataSourceDefinitionService dataSourceDefinitionService;

    @Autowired
    private DataSourceService dataSourceService;


    public Map<String, Object> connectionConfig(String subscribeId, UserDetail user) {
        if (null == subscribeId) return null;
        int indexOf = subscribeId.indexOf("#") + 1;
        String connectionId = indexOf > 0 ? subscribeId.substring(indexOf) : subscribeId;

        DataSourceConnectionDto optional = dataSourceService.findById(toObjectId(connectionId));
        if (null == optional) {
            return null;
        }

        return optional.getConfig();

//        DataSourceDefinitionDto dataSource = dataSourceDefinitionService.findByPdkHash(optional.getPdkHash(), Integer.MAX_VALUE, user);
//        if (null == dataSource) {
//            return null;
//        }
//        LinkedHashMap<String, Object> properties = dataSource.getProperties();
//        Map<String, Object> connection = new HashMap<>();
//        //获取connection配置
//        if (null == properties || properties.isEmpty()) {
//            log.debug("Reset WebHook URL error, message : Connector's jsonSchema must be not null or not empty.");
//            return connection;
//        }
//        Object connectionObj = properties.get("connection");
//        if (null == connectionObj || !(connectionObj instanceof Map) || ((Map<String, Object>) connectionObj).isEmpty()) {
//            log.debug("Reset WebHook URL error, message : Connector's connection must be not null or not empty.");
//            return connection;
//        }
//        //获取properties配置信息
//        Object connectionPropertiesObj = ((Map<String, Object>) connectionObj).get("properties");
//        if (null == connectionPropertiesObj || !(connectionPropertiesObj instanceof Map)) {
//            log.debug("Reset WebHook URL error, message : Connector's properties must be not null or not empty.");
//            return connection;
//        }
//        Map<String, Object> connectionProperties = (Map<String, Object>) connectionPropertiesObj;
//        return connectionProperties;
    }

    public boolean existSupplier(Map<String, Object> connection, SubscribeURLToken subscribeToken) {
        String supplierKey = subscribeToken.getSupplierKey();
        String randomId = subscribeToken.getRandomId();
        String key = supplierKey + "_" + randomId;
        Object listObj = connection.get("supplierConfig");
        if (listObj instanceof Collection) {
            Collection<Map<String, Object>> collection = (Collection<Map<String, Object>>) listObj;
            Set<String> supplierKeySet = collection.stream()
                    .filter(Objects::nonNull)
                    .map(map -> String.valueOf(map.get("supplierKey")) + "_" + String.valueOf(map.get("randomId")))
                    .filter(k -> k.equals(key))
                    .collect(Collectors.toSet());
            return supplierKeySet.isEmpty();
        }
        return false;
    }

    public void expireSeconds(Map<String, Object> connection, SubscribeDto subscribeDto) {
        Object expireSecondsObj = connection.get("expireSeconds");
        Long expireSeconds = -1L;
        try {
            if (expireSecondsObj instanceof Number) {
                expireSeconds = ((Number) expireSecondsObj).longValue();
            } else if (expireSecondsObj instanceof String) {
                expireSeconds = Long.getLong(String.valueOf(expireSecondsObj));
            } else if (expireSecondsObj instanceof Map) {
                Map<String, Object> obj = (Map<String, Object>) expireSecondsObj;
                //obj.get("")
            }
        } catch (Exception e) {
            expireSeconds = -1L;
        }
        try {
            subscribeDto.setExpireSeconds(expireSeconds.intValue());
        } catch (Exception e) {
            subscribeDto.setExpireSeconds(Integer.MAX_VALUE);
        }
    }
}
