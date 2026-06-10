package com.tapdata.tm.v2.api.schema.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.service.impl.RemoteCaller;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 10:13 Create
 * @description
 */
@Service
public class ReLoadApiSchemaService {
    @Resource(name = "remoteCaller")
    RemoteCaller remoteCaller;

    public void reloadApiSchema(String connectionId, String tableName, HttpServletRequest request, HttpServletResponse response, UserDetail userDetail) {
        if (StringUtils.isBlank(connectionId)) {
            throw new BizException("schema.reload.connectionId");
        }
        if (StringUtils.isBlank(tableName)) {
            throw new BizException("schema.reload.tableName");
        }
        ServiceCaller serviceCaller = new ServiceCaller();
        serviceCaller.className("DiscoverSchemaService")
                .method("discoverSpecifySchema")
                .args(new Object[]{connectionId, tableName});
        remoteCaller.callMethod(serviceCaller, request, response, userDetail);
    }
}
