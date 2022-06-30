package com.tapdata.tm.cluster.schedule;

import com.tapdata.tm.cluster.service.ClusterStateService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired} )
public class ClusterSchedule {

    private ClusterStateService clusterStateService;

    @Scheduled(fixedDelay = 6000L)
    public void stopCluster() {
        clusterStateService.stopCluster();
    }
}
