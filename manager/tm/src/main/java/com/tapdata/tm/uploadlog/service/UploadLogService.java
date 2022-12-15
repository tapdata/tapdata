package com.tapdata.tm.uploadlog.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.uploadlog.dto.UploadLogDto;
import com.tapdata.tm.utils.MD5Util;
import com.tapdata.tm.ws.endpoint.WebSocketClusterServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class UploadLogService {
    @Autowired
    TcmService tcmService;

    @Autowired
    ClusterStateService clusterStateService;

    @Autowired
    WebSocketClusterServer webSocketClusterServer;

    /**
     * 从tcm获取请求发送ws给agent
     * @param uploadLogDto
     * @return
     */
    public String upload(UploadLogDto uploadLogDto, UserDetail userDetail)  {
        try {
            // todo 增加userDetail查询
            ClusterStateDto  clusterStateDto= clusterStateService.findOne(Query.query(Criteria.where("systemInfo.process_id").
                    is(uploadLogDto.getTmInfoEngineId()).and("status").is("running")));
            if (clusterStateDto == null){
                log.error("AgentId don't exist");
                throw new BizException("NotFoundAgent", "Not found agent by id " + uploadLogDto.getTmInfoEngineId());
            }
            log.debug("send message start");
            webSocketClusterServer.sendDistributeClusterMessage(clusterStateDto.getSystemInfo().getUuid(), getSendObj(uploadLogDto));
        } catch (Exception e) {
            log.error("send message fail",e);
            throw new BizException("SendFail","Send message fail:{}" + e.getMessage());
        }
        return "success";
    }

    public String getSendObj(UploadLogDto uploadLogDto) {
        Map map = new HashMap();
        map.put("type", "uploadLog");
        map.put("timestamp", System.currentTimeMillis());
        map.put("data", uploadLogDto);
        String sign = signString(JsonUtil.toJsonUseJackson(map));
        map.put("sign", sign);
        return JsonUtil.toJsonUseJackson(map);
    }

    private String signString(String str) {
        return MD5Util.stringToMD5("tapdata" + str + "20200202");
    }


    /**
     * 从ws获取数据后传递到TCM处理
     * @param map
     * @return
     */
    public void handleUploadHeartBeat(Map map) {
        try {
            tcmService.updateUploadStatus(map);
        } catch (Exception e) {
            log.error("handle Upload HeartBeat fail:{}", e);
        }
    }

}
