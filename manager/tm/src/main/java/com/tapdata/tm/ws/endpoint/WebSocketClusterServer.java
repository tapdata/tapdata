/**
 * @title: WebSocketClusterServer
 * @description:
 * @author lk
 * @date 2021/11/9
 */
package com.tapdata.tm.ws.endpoint;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.service.LogService;
import com.tapdata.tm.utils.MD5Util;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class WebSocketClusterServer extends TextWebSocketHandler {

    public static final Map<String, AgentInfo> agentMap = new ConcurrentHashMap<>();

    @Autowired
    ClusterOperationService clusterOperationService;

    @Autowired
    ClusterStateService clusterStateService;

    @Autowired(required = false)
    @Lazy
    WorkerService workerService;

    @Autowired
    LogService logService;


    /**
     * 握手后建立成功
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("WebSocketClusterServer  afterConnectionEstablished sessionId:{}, sessionUri :{}   ", session.getId(), session.getUri());
    }

    /**
     * 接收消息
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        handleMessage(session, message);
    }

    /**
     * 断开连接时
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.error("WebSocket close about cluster,id: {},uri: {},closeStatus: {}", session.getId(), session.getUri(), status);
    }

    public static void sendMessage(String uuid, String message) throws IOException {
        WebSocketSession webSocketSession = agentMap.get(uuid).getSession();
        webSocketSession.sendMessage(new TextMessage(message));
    }


    private void handleMessage(WebSocketSession session, TextMessage message) {
        try {
            Map map = JsonUtil.parseJson(message.getPayload(), Map.class);
            if (map == null || map.get("sign") == null) {
                session.sendMessage(new TextMessage("Payload or sign is null"));
                return;
            }
            String msgReceived = JsonUtil.toJsonUseJackson(map);
//            log.info("WebSocketClusterServer 接收到 message：{}", msgReceived);

            String sign = MapUtil.getStr(map, "sign");
            //先去掉sign  属性，再进行验签
            map.remove("sign");

            Long timeStamp = MapUtils.getAsLong(map, "timestamp");

            if (!checkSign(sign, timeStamp, message.getPayload())) {
                session.sendMessage(new TextMessage("Check sign failed"));
                return;
            }

            if (!map.containsKey("type")) {
                session.sendMessage(new TextMessage("Type is empty"));
                return;
            }

            String type = map.get("type").toString();
            log.info("执行方法type:{}",type);
            switch (type) {
                case "statusInfo":
                    long ttl = System.currentTimeMillis();
                    String value = MapUtils.getAsStringByPath(map, "/data/reportInterval");
                    if (StringUtils.isNotBlank(value)) {
                        ttl += Double.valueOf(value) * 2;
                    }
                    String uuid = MapUtils.getAsStringByPath(map, "/data/systemInfo/uuid");
                    log.info("statusInfo put uuid  :{}",uuid);
                    if (StringUtils.isNotBlank(uuid)) {
                        agentMap.put(uuid, new AgentInfo(uuid, session, ttl));
                    }
                    clusterStateService.statusInfo(map);
                    break;
                case "changeStatus":
                    clusterOperationService.changeStatus(map);
                    break;
                case "responseLog":
                    responseLog(map);
                    break;
                case "logs":
                    saveLogs(map);
                    break;
                case "logsFinished":
                    clusterStateService.logsFinished(map);
                    break;
                case "postMessage":
                    saveMsg(map);
                    break;
                case "version":
                    updateVersion(map);
                    break;
                case "updateMsg":
                    clusterOperationService.updateMsg(map);
                    workerService.updateMsg(map);
                    break;
                case "updateWorkerPingTime":
                    updateWorkerPingTime(map);
                    break;
                default:
                    session.sendMessage(new TextMessage("Type is not supported"));
                    break;
            }

        } catch (Exception e) {
            log.error("Handle message failed,message: {}", e.getMessage(), e);
            try {
                session.sendMessage(new TextMessage("Handle message failed,message:" + e.getMessage()));
            } catch (Exception ignored) {
                log.error("Websocket send message failed,message: {}", e.getMessage());
            }
        }
    }

    private boolean checkSign(String sign, Long time, String payLoad) {
        Map dataToEncrptMap =   (Map) JSON.parse(payLoad, Feature.OrderedField);
        dataToEncrptMap.remove("sign");
        String s= JSON.toJSONString(dataToEncrptMap);
        long currentTimeMillis = System.currentTimeMillis();
        if (time + 1000 * 60 * 10 < currentTimeMillis) {
            return false;
        }

        s = "tapdata" + s + "20200202";
        String md5Value = MD5Util.stringToMD5(s);
        return sign.equals(md5Value);
    }


    private Map<String, String> queryStr2Map(String queryStr) {
        Map<String, String> result = new HashMap<>();
        if (StringUtils.isBlank(queryStr)) {
            return result;
        }
        result = Arrays.stream(queryStr.split("&"))
                .map(kv -> kv.split("="))
                .collect(Collectors.toMap(split -> split[0], split -> split[1], (a, b) -> b));

        return result;
    }


    @Getter
    @Setter
    @AllArgsConstructor
    private static class AgentInfo {
        private String uuid;
        private WebSocketSession session;
        private Long ttl;
    }

    private void responseLog(Map map) {
        Map data = (Map) map.get("data");
        LogDto logDto = BeanUtil.mapToBean(map, LogDto.class, false, CopyOptions.create());
        logService.save(logDto);
    }


    /**
     * const server = require('./server');
     * const conn = server.models.Logs.dataSource.connector;
     * const res = await conn.collection('Logs').insertMany(logs);
     * console.info(res);
     *     todo 是否要写入log表
     *
     * @param map
     */
    private void saveLogs(Map map) {

    }




    /**
     * let result = messageUtil.checkMessage(msg.data);
     * if (result !== true) {
     * return;
     * }
     * let Message = server.models.Message;
     * messageUtil.agentMsg(msg.data, server).then(() => {
     * //todo
     */
    private void saveMsg(Map map) {

    }

    /**
     * 是否有需要增加clusterVersion 表
     *
     * @param map
     */
    private void updateVersion(Map map) {
     /*   let clusterVersion = server.models.clusterVersion;
        clusterVersion.upsertWithWhere({'uuid': msg.uuid}, {'uuid': msg.uuid, version:msg.data});*/
    }


    /**
     * const workers = server.models.Workers;
     * if(msg.data.process_id && msg.data.process_id !== ''){
     * workers.update({process_id:msg.data.process_id},{ping_time:1})
     * }
     */
    private void updateWorkerPingTime(Map map) {
        String processId = MapUtil.getStr(map, "process_id");
        Query query = Query.query(Criteria.where("process_id").is(processId));
        Update update = new Update();
        update.set("ping_time", 1);
        workerService.update(query, update);

    }


}
