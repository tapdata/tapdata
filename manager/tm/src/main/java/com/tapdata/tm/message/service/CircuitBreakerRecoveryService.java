package com.tapdata.tm.message.service;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.utils.HttpUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


@Component
@Slf4j
public class CircuitBreakerRecoveryService {

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> scheduledFuture;

    protected  AtomicBoolean checkTask = new AtomicBoolean(true);


    public void checkServiceStatus(Long count,String address){
        if(checkTask.get()){
            checkTask.compareAndSet(true,false);
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new SendMessageRunnable(count,address),0,5,TimeUnit.MINUTES);
        }

    }

    protected boolean checkAvailableAgentCount(Long count) {
        //当前活跃的引擎数
        WorkerService workerService = SpringContextHelper.getBean(WorkerService.class);
        Long availableAgentCount = workerService.getAvailableAgentCount();
        String threshold = CommonUtils.getProperty("circuit_breaker_recovery_threshold", "0.95");
        double result;
        try{
            result = Double.parseDouble(threshold);
            if(result > 1 || result <= 0){
                result = 0.95;
            }
        }catch (Exception e){
            result = 0.95;
            log.warn("Failed to obtain the threshold, the default threshold is used");
        }
        //恢复阈值
        Long agentThresholdCount = (long) Math.ceil(count * result);
        log.info("availableAgentCount:{},agentThresholdCount:{}",availableAgentCount,agentThresholdCount);
        return availableAgentCount >= agentThresholdCount;
    }

    protected class SendMessageRunnable implements Runnable {
        Long count;
        String address;
        protected SendMessageRunnable(Long count,String address){
            this.count = count;
            this.address = address;
        }
        @Override
        public void run() {
            if(checkAvailableAgentCount(count)){
                Map<String,String> map = new HashMap<>();
                String content = "熔断恢复，服务已恢复正常";
                map.put("title", "服务熔断恢复提醒");
                map.put("content", content);
                map.put("color", "green");
                map.put("groupId","oc_d6bc5fe48d56453264ec73a2fb3eec70");
                HttpUtils.sendPostData(address, JSONObject.toJSONString(map));
                log.info("Sending service circuit breaker recovery notification successfully");
                if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
                    scheduledFuture.cancel(true);
                    checkTask.compareAndSet(false,true);
                }
            }else {
                log.info("The service has not been restored and will continue to check");
            }
        }
    }
}
