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
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.service.LogService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.uploadlog.service.UploadLogService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MD5Util;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.dto.WebSocketResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class WebSocketClusterServer extends TextWebSocketHandler {

    public static final Map<String, AgentInfo> agentMap = new ConcurrentHashMap<>();
    private static final Map<String, WebSocketSession> webSocketSessionCache = new ConcurrentHashMap<>();

    @Autowired
    ClusterOperationService clusterOperationService;

    @Autowired
    ClusterStateService clusterStateService;

    @Autowired(required = false)
    @Lazy
    WorkerService workerService;

    @Autowired
    LogService logService;

    @Autowired
    TcmService tcmService;

    @Autowired
    UploadLogService uploadLogService;

    @Autowired
    MessageQueueService messageQueueService;
    @Autowired
    private UserService userService;


    /**
     * 握手后建立成功
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("WebSocketClusterServer  afterConnectionEstablished sessionId:{}, sessionUri :{}   ", session.getId(), session.getUri());
        webSocketSessionCache.put(session.getId(), session);
    }

    /**
     * 接收消息
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        log.debug("Receive {}: {}", session.getId(), message.getPayload());
        handleMessage(session, message);
    }

    /**
     * 断开连接时
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.error("WebSocket close about cluster,id: {},uri: {},closeStatus: {}", session.getId(), session.getUri(), status);
        WebSocketSession wsSession = webSocketSessionCache.remove(session.getId());
        if (wsSession != null) {
            for (String uuid : agentMap.keySet() ) {
                if (agentMap.get(uuid).session == wsSession) {
                    agentMap.remove(uuid);
                    break;
                }
            }
        }
    }

    public static void sendMessage(String uuid, String message) throws IOException {
        AgentInfo agentInfo = agentMap.get(uuid);
        if (agentInfo == null) {
            return;
        }
        WebSocketSession webSocketSession = agentInfo.getSession();
        sendMessage(webSocketSession, message);
    }

    private static void sendMessage(WebSocketSession session, String message) throws IOException {
        if (session != null && message != null) {
            log.debug("Send to {}: {}", session.getId(), message);
            session.sendMessage(new TextMessage(message));
        }
    }

    // todo 发送分开
    public void sendDistributeClusterMessage(String uuid, String message) throws IOException {
        AgentInfo agentInfo = agentMap.get(uuid);
        if (agentInfo == null) {
            MessageQueueDto queueDto = new MessageQueueDto();
            queueDto.setReceiver(uuid);
            queueDto.setData(message);
            queueDto.setType("pipeCluster");
            messageQueueService.save(queueDto);
        } else {
            WebSocketSession webSocketSession = agentInfo.getSession();
            sendMessage(webSocketSession, message);
        }
    }


    private void handleMessage(WebSocketSession session, TextMessage message) {
        try {
            Map map = JsonUtil.parseJson(message.getPayload(), Map.class);
            if (map == null || map.get("sign") == null) {
                sendMessage(session, "Payload or sign is null");
                return;
            }
            String msgReceived = JsonUtil.toJsonUseJackson(map);
//            log.info("WebSocketClusterServer 接收到 message：{}", msgReceived);

            String sign = MapUtil.getStr(map, "sign");
            //先去掉sign  属性，再进行验签
            map.remove("sign");

            Long timeStamp = MapUtils.getAsLong(map, "timestamp");

            if (!checkSign(sign, timeStamp, message.getPayload())) {
                sendMessage(session, "Check sign failed");
                return;
            }

            if (!map.containsKey("type")) {
                sendMessage(session, "Type is empty");
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
                        //根据uuid 报错session，以便可以发对uuid对应的session去
                        //为什么要不断往后退ttl
                        AgentInfo agentInfo = agentMap.get(uuid);
                        if (agentInfo != null) {
                            agentInfo.ttl = ttl;
                        } else {
                            agentMap.put(uuid, new AgentInfo(uuid, session, ttl));
                        }
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
                case "checkTapdataAgentVersion": // 获取tapdataAgent版本是否支持
                    checkTapdataAgentVersion(map, session);
                    break;
                case "downloadUrl": //  获取engine的downloadUrl
                    getLatestDownloadUrl(session);
                    break;
                case "uploadLog": // 上传日志处理心跳
                    uploadLogService.handleUploadHeartBeat(map);
                    break;
                default:
                    sendMessage(session, "Type is not supported");
                    break;
            }

        } catch (Exception e) {
            log.error("Handle message failed,message: {}", e.getMessage(), e);
            try {
                sendMessage(session, "Handle message failed,message:" + e.getMessage());
            } catch (Exception ex) {
                log.error("Websocket send message failed,message: {}", ex.getMessage(), ex);
            }
        }
    }

    private String getUserId(WebSocketSession session){
        try {
            List<String> userIds = session.getHandshakeHeaders().get("user_id");
            if (CollectionUtils.isNotEmpty(userIds)){
                UserDetail userDetail = userService.loadUserByExternalId(userIds.get(0));
                return userDetail != null ? userDetail.getExternalUserId() : null;
            }
        }catch (Exception e){
            log.error("WebSocket get userId error,message: {}", e.getMessage());
        }

        return null;
    }

    private void getLatestDownloadUrl(WebSocketSession session) {
        WebSocketResult webSocketResult = WebSocketResult.ok(tcmService.getDownloadUrl(getUserId(session)), "downloadUrl");
        try {
            sendMessage(session, JsonUtil.toJson(webSocketResult));
        } catch (Exception e) {
            log.error("Websocket send message failed,message: {}", e.getMessage(), e);
        }
    }

    private void checkTapdataAgentVersion(Map map, WebSocketSession session) {
        Object data = map.get("data");
        Object versionInfo = null;
        if (data instanceof Map){
            Object version = ((Map) data).get("version");
            if (version != null){
                versionInfo = tcmService.getVersionInfo(version.toString());
            }
        }
        WebSocketResult webSocketResult = WebSocketResult.ok(versionInfo, "checkTapdataAgentVersion");

        try {
            sendMessage(session, JsonUtil.toJson(webSocketResult));
        } catch (Exception e) {
            log.error("Websocket send message failed,message: {}", e.getMessage(), e);
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
