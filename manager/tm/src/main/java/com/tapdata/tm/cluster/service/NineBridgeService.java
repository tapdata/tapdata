package com.tapdata.tm.cluster.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.NineBridgeCommandExecResult;
import com.tapdata.tm.cluster.dto.NineBridgeSNResult;
import com.tapdata.tm.cluster.dto.NineBridgeUpdateConfigResult;
import com.tapdata.tm.cluster.dto.NineBridgeUpgradeSNResult;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.params.NineBridgeConfigParam;
import com.tapdata.tm.utils.HttpUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/13 19:52 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class NineBridgeService {
    public static final String START_NINI_BRIDGE = "http://%s:%d/api/ninebridge/start";
    public static final String STOP_NINI_BRIDGE = "http://%s:%d/api/ninebridge/stop";
    public static final String RESTART_NINI_BRIDGE = "http://%s:%d/api/ninebridge/restart";
    public static final String UPDATE_CONFIG_NINI_BRIDGE = "http://%s:%d/api/ninebridge/config";
    public static final String UPDATE_LICENSE_NINI_BRIDGE = "http://%s:%d/api/ninebridge/license-update";
    public static final String FIND_LICENSE_NINI_BRIDGE = "http://%s:%d/api/ninebridge/license";

    private RawServerStateService rawServerStateService;

    RawServerStateDto findByServerId(String serverId) {
        if (StringUtils.isBlank(serverId)) {
            throw new BizException("nine.bridge.server.id.empty");
        }
        Filter filter = new Filter();
        filter.setWhere(new Where());
        filter.getWhere().put("serviceId", new Document().append("$eq", serverId));
        Page<RawServerStateDto> allLatest = rawServerStateService.getAllLatest(filter);
        List<RawServerStateDto> items = allLatest.getItems();
        if (CollectionUtils.isEmpty(items) || items.get(0) == null) {
            throw new BizException("nine.bridge.unable.find.server", serverId);
        }
        RawServerStateDto server = items.get(0);
        if (StringUtils.isBlank(server.getServiceIP()) || server.getServicePort() == null) {
            throw new BizException("nine.bridge.unable.invalid.server", serverId);
        }
        return server;
    }

    public NineBridgeCommandExecResult executeCommand(String serverId, String command) {
        RawServerStateDto server = findByServerId(serverId);
        String serviceIP = server.getServiceIP();
        Integer servicePort = server.getServicePort();
        String uri = switch (command) {
            case "start" -> String.format(START_NINI_BRIDGE, serviceIP, servicePort);
            case "stop" -> String.format(STOP_NINI_BRIDGE, serviceIP, servicePort);
            case "restart" -> String.format(RESTART_NINI_BRIDGE, serviceIP, servicePort);
            default -> throw new BizException("nine.bridge.unable.invalid.command", command);
        };
        try {
            return post(uri, new HashMap<>(), NineBridgeCommandExecResult.class);
        } catch (Exception e) {
            throw new BizException("nine.bridge.command.failed", command, serverId, e.getMessage());
        }
    }

    public NineBridgeUpdateConfigResult updateNineBridgeConfig(String serverId, NineBridgeConfigParam parma) {
        if (StringUtils.isBlank(parma.getOracleUrl()) && StringUtils.isBlank(parma.getMapTable())) {
            throw new BizException("nine.bridge.update.config.cannot.be.empty", serverId);
        }
        RawServerStateDto server = findByServerId(serverId);
        String serviceIP = server.getServiceIP();
        Integer servicePort = server.getServicePort();
        String uri = String.format(UPDATE_CONFIG_NINI_BRIDGE, serviceIP, servicePort);
        final Map<String, Object> body = new HashMap<>(8);
        body.put("service_id", serverId);
        body.put("flag", "s");
        if (StringUtils.isNotBlank(parma.getOracleUrl())) {
            body.put("oracle_url", parma.getOracleUrl());
        }
        if (StringUtils.isNotBlank(parma.getMapTable())) {
            body.put("map_table", parma.getMapTable());
        }
        try {
            return post(uri, body, NineBridgeUpdateConfigResult.class);
        } catch (Exception e) {
            throw new BizException("nine.bridge.update.config.failed", serverId, e.getMessage());
        }
    }

    public NineBridgeUpgradeSNResult upgradeNineBridgeSN(String serverId, String newSnContext) {
        if (StringUtils.isBlank(newSnContext)) {
            throw new BizException("nine.bridge.sn.file.empty");
        }
        RawServerStateDto server = findByServerId(serverId);
        String serviceIP = server.getServiceIP();
        Integer servicePort = server.getServicePort();
        Map<String, String> body = new HashMap<>(1);
        body.put("AuthorizationCode", newSnContext);
        String uri = String.format(UPDATE_LICENSE_NINI_BRIDGE, serviceIP, servicePort);
        try {
            return post(uri, body, NineBridgeUpgradeSNResult.class);
        } catch (Exception e) {
            throw new BizException("nine.bridge.update.sn.failed", serverId, e.getMessage());
        }
    }

    public NineBridgeSNResult findNineBridgeSN(String serverId) {
        RawServerStateDto server = findByServerId(serverId);
        String serviceIP = server.getServiceIP();
        Integer servicePort = server.getServicePort();
        String uri = String.format(FIND_LICENSE_NINI_BRIDGE, serviceIP, servicePort);
        try {
            Map<?, ?> map = get(uri, Map.class);
            NineBridgeSNResult result = new NineBridgeSNResult();
            result.setData(NineBridgeSNResult.DataInfo.parse(map.get("data")));
            result.setMessage((String) map.get("message"));
            result.setStatus((String) map.get("status"));
            return result;
        } catch (Exception e) {
            throw new BizException("nine.bridge.find.sn.failed", serverId, e.getMessage());
        }
    }

    public Boolean removeUselessServerInfo(String serverId) {
        findByServerId(serverId);
        try {
            rawServerStateService.deleteAll(serverId);
            return true;
        } catch (Exception e) {
            throw new BizException("nine.bridge.remove.server.failed", serverId, e.getMessage());
        }
    }

    <T>T get(String url, Class<T> resultClass) {
        String json = HttpUtils.sendGetData(
                url,
                new HashMap<>(),
                false ,
                false);
        return JSON.parseObject(json, resultClass);
    }


    <T>T post(String url, Object body, Class<T> resultClass) {
        String json = HttpUtils.sendPostData(
                url,
                JSON.toJSONString(body),
                new HashMap<>(),
                false,
                false);
        return JSON.parseObject(json, resultClass);
    }
}
