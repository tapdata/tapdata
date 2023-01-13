package com.tapdata.tm.clusterOperation.service;

import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.clusterOperation.constant.AgentStatusEnum;
import com.tapdata.tm.clusterOperation.dto.ClusterOperationDto;
import com.tapdata.tm.clusterOperation.entity.ClusterOperationEntity;
import com.tapdata.tm.clusterOperation.repository.ClusterOperationRepository;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.MD5Util;
import com.tapdata.tm.ws.endpoint.WebSocketClusterServer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
public class ClusterOperationService extends BaseService<ClusterOperationDto, ClusterOperationEntity, ObjectId, ClusterOperationRepository> {
/*    @Autowired
    WorkerService workerService;*/

    @Autowired
    WebSocketClusterServer webSocketClusterServer;

    @Autowired
    ClusterStateService clusterStateService;


    public ClusterOperationService(@NonNull ClusterOperationRepository repository) {
        super(repository, ClusterOperationDto.class, ClusterOperationEntity.class);
    }

    protected void beforeSave(ClusterOperationDto clusterState, UserDetail user) {

    }

    public void sendOperation() {
        Set<String> uuidList = WebSocketClusterServer.agentMap.keySet();

        if (CollectionUtils.isNotEmpty(uuidList)) {
            Query query = Query.query(Criteria.where("uuid").in(uuidList).and("status").is(AgentStatusEnum.NEED_UPDATE.getValue()));
            List<ClusterOperationDto> dtoToUpdateList = findAll(query);
            if (CollectionUtils.isNotEmpty(dtoToUpdateList)) {
                for (ClusterOperationDto dto : dtoToUpdateList) {

                    try {
                        String dataToSend = "";
                        if ("update".equals(dto.getType())) {
                            dataToSend = getSendObj(dto);
                        } else {
                            dataToSend = getOperationObj(dto);
                        }
                        WebSocketClusterServer.sendMessage(dto.getUuid(), dataToSend);
                        //send 完之后,  根据发送消息返回的结果 要更新表
                        updateAfterSend(dto.getId().toString());
                    } catch (Throwable e) {
                        log.error("agent升级 异常. uuid:{}", dto.getUuid(), e);
                    }
                }
            } else {
                log.info("没有需要更新的agent");
            }
        }
    }


    /**
     * let serverOperation = record['server'] + 'Operation';
     * set[`${serverOperation}.msg`] = '执行超时';
     */
    public void cleanOperation() {
        Long nowTime = new Date().getTime();
        Query query = Query.query(Criteria.where("ttl").lt(nowTime).and("status").is(AgentStatusEnum.UPDATING.getValue()));
        List<ClusterOperationDto> timeoutList = findAll(query);
        if (CollectionUtils.isNotEmpty(timeoutList)) {
            log.info("clean clusterOperation:{} ", timeoutList.size());

            timeoutList.forEach(clusterOperationDto -> {
                Update update = new Update();
                update.set("status", AgentStatusEnum.UPDATED_TIME_OUT.getValue());
                update.set("msg", "执行超时");
                Query updateQuery = Query.query(Criteria.where("id").is(clusterOperationDto.getId()));
                update(updateQuery, update);
            });
        }
    }


    private void updateAfterSend(String id) {
//        Long newTtl=System.currentTimeMillis()+30000L;
        Date now = new Date();
        Long newTtl = now.getTime() + 30000L;

        Query query = Query.query(Criteria.where("id").is(id));
        Update update = new Update();
//        clusterOperation.update({_id: record.id}, {'status': 1, 'operationTime': now, 'ttl': ttl});
        update.set("status", AgentStatusEnum.UPDATING.getValue());
        update.set("operationTime", now);
        update.set("msg", "更新中");
        update.set("ttl", newTtl);
        update(query, update);

    }


    /**
     * let clusterOperation = server.models.clusterOperation;
     * let clusterState = server.models.clusterState;
     * let status = msg.data.status === 'true' ? 2 : 3;
     * clusterOperation.update({'_id': mongoDB.ObjectID(msg.data._id)}, {'status': status});
     * let set = {};
     * let serverOperation = msg.data.server + 'Operation';
     * set[`${serverOperation}.status`] = status;
     * let where = {};
     * where[`${serverOperation}._id`] = mongoDB.ObjectID(msg.data._id);
     * clusterState.update(where, {'$set': set});
     */
    public void changeStatus(Map data) {
        log.info("in changeStatus  data:{}", JSON.toJSONString(data));
        Object objectData = data.get("data");
        Object receiveStatus = ((Map) objectData).get("status");
        Object id = ((Map) objectData).get("_id");
        Integer status = ("true".equals(receiveStatus) ? 2 : 3);

        Query query = Query.query(Criteria.where("id").is(id.toString()));
        Update update = new Update();
        update.set("status", status);
        update(query, update);
        Object server = ((Map) objectData).get("server");

        String queryData = server + "Operation";
        ObjectId objectId = new ObjectId(id.toString());
        Query clusterStateQuery = Query.query(Criteria.where(queryData + "._id").is(objectId));
        Update clusterUpdate = new Update();
        clusterUpdate.set(queryData + ".status", status);
        clusterStateService.update(clusterStateQuery, clusterUpdate);
    }

    /**
     * const logs = server.models.UserLogs;
     * const workers = server.models.Workers;
     * const clusterOperation = server.models.clusterOperation;
     * msg.data.type = 'update';
     * logs.create(msg.data);
     * if(msg.data.id && msg.data.id !== ''){
     * clusterOperation.update({id:msg.data.id},{status:2})
     * }
     * if(msg.data.process_id && msg.data.process_id !== ''){
     * workers.update({process_id:msg.data.process_id},
     * {progres:msg.data.progres,updateStatus:msg.data.status,updateMsg:msg.data.msg,updateTime:new Date(),updatePingTime:new Date().getTime()})
     * }
     *
     * @param map
     */
    public void updateMsg(Map map) {
        log.info("in updateMsg  map:{}", JSON.toJSONString(map));
        if (null != map.get("data")) {
            Map dataMap = (Map) map.get("data");
            if (null != dataMap.get("id")) {
                String progres = MapUtil.getStr(dataMap, "progres");
                String status = MapUtil.getStr(dataMap, "status");
                String msg = MapUtil.getStr(dataMap, "msg");

                log.info("begin update...progres:{},status:{},msg");

                String id = MapUtil.getStr(dataMap, "id");
                Query query = Query.query(Criteria.where("id").is(id));
                Update update = new Update();
                update.set("status", 2);
                update.set("msg", msg);
                update(query, update);
            }
            else {
                log.error("id 为空");
            }
        }
    }


    /**
     * const workers = server.models.Workers;
     * if(msg.data.process_id && msg.data.process_id !== ''){
     * workers.update({process_id:msg.data.process_id},{ping_time:1})
     * }
     */
    private void updateWorkerPingTime() {

    }


    public String getSendObj(ClusterOperationDto clusterOperationDto) {
        Map map = new HashMap();
        map.put("type", clusterOperationDto.getType());
        map.put("timestamp", System.currentTimeMillis());
        map.put("data", clusterOperationDto);

        String sign = signString(JsonUtil.toJsonUseJackson(map));
        map.put("sign", sign);
        return JsonUtil.toJsonUseJackson(map);
    }

    private String getOperationObj(ClusterOperationDto clusterOperationDto) {
        Map map = new HashMap();
        map.put("type", "changeStatus");
        map.put("timestamp", System.currentTimeMillis());

        Map dataMap = new HashMap();
        dataMap.put("_id", clusterOperationDto.getId().toString());
        dataMap.put("hostname", clusterOperationDto.getHostname());
        dataMap.put("uuid", clusterOperationDto.getUuid());
        dataMap.put("server", clusterOperationDto.getServer());
        dataMap.put("operation", clusterOperationDto.getOperation());
        map.put("data", dataMap);

        String sign = signString(JsonUtil.toJsonUseJackson(map));
        map.put("sign", sign);
        return JsonUtil.toJsonUseJackson(map);
    }


    private String signString(String str) {
        return MD5Util.stringToMD5("tapdata" + str + "20200202");
    }




}
