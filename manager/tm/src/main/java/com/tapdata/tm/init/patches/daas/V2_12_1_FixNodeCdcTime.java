package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;

/**
 * Fix task sync point if type is 'cdc', and value like: [{"pointType": "current"}]
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 20:52 Create
 */
@PatchAnnotation(appType = AppType.DAAS, version = "2.12-1")
public class V2_12_1_FixNodeCdcTime extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_12_1_FixNodeCdcTime.class);

    public V2_12_1_FixNodeCdcTime(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Query query = Query.query(Criteria.where("type").is("cdc"));
        query.fields().include("name", "type", "syncPoints", "dag");

        TaskDto.SyncPoint syncPoint, newSyncPoint;
        for (TaskEntity task : mongoTemplate.find(query, TaskEntity.class)) {
            if (null == task.getSyncPoints() || task.getSyncPoints().size() != 1) continue;

            syncPoint = task.getSyncPoints().get(0);
            if (null == syncPoint.getNodeId() || syncPoint.getNodeId().isEmpty()) {
                Map<String, Boolean> isSource = new HashMap<>();
                for (Edge edge : task.getDag().getEdges()) {
                    isSource.compute(edge.getTarget(), (k, v) -> false);
                    isSource.compute(edge.getSource(), (k, v) -> null == v || v);
                }

                List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
                for (Node<?> n : task.getDag().getNodes()) {
                    if (n instanceof DataParentNode && Optional.ofNullable(isSource.get(n.getId())).orElse(false)) {
                        newSyncPoint = new TaskDto.SyncPoint();
                        newSyncPoint.setNodeId(n.getId());
                        newSyncPoint.setNodeName(n.getName());
                        newSyncPoint.setConnectionId(((DataParentNode<?>) n).getConnectionId());
                        newSyncPoint.setConnectionName((String) n.getAttrs().get("connectionName"));
                        newSyncPoint.setPointType(syncPoint.getPointType());
                        newSyncPoint.setDateTime(syncPoint.getDateTime());
                        newSyncPoint.setTimeZone(syncPoint.getTimeZone());
                        syncPoints.add(newSyncPoint);
                    }
                }
                logger.info("Fix task node({}) cdc time: {}", task.getId().toHexString(), syncPoints);
                mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(task.getId())), Update.update("syncPoints", syncPoints), TaskEntity.class);
            }
        }
    }
}
