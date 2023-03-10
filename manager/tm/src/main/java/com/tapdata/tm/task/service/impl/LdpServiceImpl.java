package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.task.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class LdpServiceImpl implements LdpService {

    @Autowired
    private TaskService taskService;

    @Autowired
    private LiveDataPlatformService liveDataPlatformService;
    @Override
    public TaskDto createFdmTask(TaskDto task, UserDetail user) {
        //check fdm task
        checkFdmTask(task, user);

        task = mergeSameSourceTask(task, user);

        //add fmd type
        task.setLdpType(TaskDto.LDP_TYPE_FDM);

        //create migrate task
        return taskService.confirmById(task, user, true);
    }

    private TaskDto mergeSameSourceTask(TaskDto task, UserDetail user) {
        return null;
    }

    private void checkFdmTask(TaskDto task, UserDetail user) {
        //syncType is migrate
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(task.getSyncType())) {
            log.warn("Create fdm task, but the sync type not is migrate, sync type = {}", task.getSyncType());
            throw new BizException("");
        }

        //target need fdm connection
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        String fdmConnectionId = platformDto.getFdmStorageConnectionId();

        DAG dag = task.getDag();
        if (dag == null) {
            throw new BizException("");
        }

        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            throw new BizException("");
        }
        Node node = targets.get(0);
        String targetConId = ((DatabaseNode) node).getConnectionId();

        if (!fdmConnectionId.equals(targetConId)) {
            throw new BizException("");
        }
    }

    @Override
    public TaskDto createMdmTask(TaskDto task, UserDetail user) {
        //check mdm task
        checkMdmTask(task, user);
        //add mmd type
        task.setLdpType(TaskDto.LDP_TYPE_MDM);
        //create sync task
        return taskService.confirmById(task, user, true);
    }

    private void checkMdmTask(TaskDto task, UserDetail user) {
        //syncType is sync
        if (!TaskDto.SYNC_TYPE_SYNC.equals(task.getSyncType())) {
            log.warn("Create mdm task, but the sync type not is sync, sync type = {}", task.getSyncType());
            throw new BizException("");
        }

        //target need fdm connection
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        String fdmConnectionId = platformDto.getFdmStorageConnectionId();
        String mdmConnectionId = platformDto.getMdmStorageConnectionId();

        DAG dag = task.getDag();
        if (dag == null) {
            throw new BizException("");
        }

        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            throw new BizException("");
        }
        Node node = targets.get(0);
        String targetConId = ((DatabaseNode) node).getConnectionId();

        if (!mdmConnectionId.equals(targetConId)) {
            throw new BizException("");
        }


        List<Node> sources = dag.getSources();
        if (CollectionUtils.isEmpty(sources)) {
            throw new BizException("");
        }
        Node sourceNode = sources.get(0);
        String sourceConId = ((DatabaseNode) sourceNode).getConnectionId();

        if (!fdmConnectionId.equals(sourceConId)) {
            throw new BizException("");
        }
    }
}
