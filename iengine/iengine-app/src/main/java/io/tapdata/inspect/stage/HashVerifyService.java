package io.tapdata.inspect.stage;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.inspect.InspectService;
import io.tapdata.inspect.InspectTask;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.cdc.InspectCdcUtils;
import io.tapdata.inspect.cdc.compare.RowCountInspectCdcJob;
import io.tapdata.inspect.compare.HashVerifyInspectJob;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 执行Hash检查
 * */
public class HashVerifyService {
    private Logger logger = LogManager.getLogger(HashVerifyService.class);
    private volatile ClientMongoOperator clientMongoOperator;
    private HashVerifyService() {

    }

    protected HashVerifyService init(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
        return this;
    }

    public static HashVerifyService create(ClientMongoOperator clientMongoOperator) {
        return new HashVerifyService().init(clientMongoOperator);
    }

    public InspectTask inspect(InspectService inspectService, Inspect inspect) {
        List<String> errorMsg = checkRowCountInspect(inspect);
        if (!errorMsg.isEmpty()) {
            if (null == inspect) return null;
            updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            return null;
        }

        return new io.tapdata.inspect.InspectTask(inspectService, inspect, clientMongoOperator) {

            @Override
            public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
                if (InspectCdcUtils.isInspectCdc(inspect)) {
                    InspectCdcUtils.initCdcRunProfiles(inspect, inspectTaskContext.getTask());
                    return new HashVerifyInspectJob(inspectTaskContext);
                }
                return new HashVerifyInspectJob(inspectTaskContext);
            }
        };
    }

    /**
     * 检查行数比对参数
     *
     * @param inspect
     * @return
     */
    protected List<String> checkRowCountInspect(Inspect inspect) {
        /*
         * 1. Status must be scheduling
         * 2. tasks size must gt 0
         * 3. tasks.source、tasks.target、tasks.taskId can not be empty.
         * 4. tasks.source.connectionId、tasks.source.table can not be empty.
         */
        List<String> errorMsg = new ArrayList<>();

        if (inspect == null) {
            errorMsg.add("Inspect can not be empty.");
            return errorMsg;
        }

        if (!"scheduling".equals(inspect.getStatus())) {
            errorMsg.add("Inspect status must be scheduling");
        }

        if (inspect.getTasks() == null || !inspect.getTasks().isEmpty()) {
            errorMsg.add("Inspect sub-task can not be empty.");
            return errorMsg;
        }

        for (int i = 0; i < inspect.getTasks().size(); i++) {
            com.tapdata.entity.inspect.InspectTask task = inspect.getTasks().get(i);
            if (task == null) {
                logger.warn("Inspect.tasks[{}] is empty.",i);
                continue;
            }
            if (StringUtils.isEmpty(task.getTaskId())) {
                errorMsg.add(String.format(InspectService.INSPECT_TASKS_CANNOT_BE_EMPTY,i));
            }
            List<String> sourceErrorMsg = checkRowCountInspectTaskDataSource(String.format(InspectService.INSPECT_TASKS_PREFIX_SOURCE, i), task.getSource());
            errorMsg.addAll(sourceErrorMsg);
            List<String> targetErrorMsg = checkRowCountInspectTaskDataSource(String.format(InspectService.INSPECT_TASKS_PREFIX_TARGET, i), task.getTarget());
            errorMsg.addAll(targetErrorMsg);
        }
        return errorMsg;
    }

    public void updateStatus(String id, InspectStatus status, String msg) {

        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("id", id);

        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put(InspectService.STATUS_FIELD, status.getCode());
        updateMap.put("errorMsg", msg);

        clientMongoOperator.upsert(queryMap, updateMap, ConnectorConstant.INSPECT_COLLECTION);
    }

    protected List<String> checkRowCountInspectTaskDataSource(String prefix, InspectDataSource dataSource) {
        List<String> errorMsg = new ArrayList<>();
        if (null == dataSource){
            errorMsg.add(prefix + ".inspectDataSource can not be null.");
            return errorMsg;
        }
        if (StringUtils.isEmpty(dataSource.getConnectionId())) {
            errorMsg.add(prefix + ".connectionId can not be empty.");
        }
        if (StringUtils.isEmpty(dataSource.getTable())) {
            errorMsg.add(prefix + ".table can not be empty.");
        }
        return errorMsg;
    }
}
