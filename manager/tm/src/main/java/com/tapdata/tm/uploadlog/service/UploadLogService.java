package com.tapdata.tm.uploadlog.service;

import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.util.JsonUtil;
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

    /**
     * 从tcm获取请求发送ws给agent
     * @param uploadLogDto
     * @return
     */
    public String upload(UploadLogDto uploadLogDto) {
        try {
            ClusterStateDto  clusterStateDto= clusterStateService.findOne(Query.query(Criteria.where("systemInfo.process_id").is(uploadLogDto.getTmInfoEngineId())));
            if (clusterStateDto == null){
                return "AgentId don't exist";
            }
            WebSocketClusterServer.sendMessage(clusterStateDto.getSystemInfo().getUuid(), getSendObj(uploadLogDto));
        } catch (Exception e) {
            return e.getMessage();
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
            log.error(e.getMessage());
        }
    }

}
