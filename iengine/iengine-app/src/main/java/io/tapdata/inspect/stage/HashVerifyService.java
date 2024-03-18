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
import io.tapdata.inspect.compare.HashVerifyInspectJob;
import io.tapdata.inspect.util.InspectJobUtil;
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
    private ClientMongoOperator clientMongoOperator;
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
            errorMsg.add("Inspect(hash verify) can not be empty");
            return errorMsg;
        }

        if (!"scheduling".equals(inspect.getStatus())) {
            errorMsg.add("Inspect(hash verify) status must be scheduling");
        }
        List<com.tapdata.entity.inspect.InspectTask> tasks = inspect.getTasks();
        if (null == tasks || tasks.isEmpty()) {
            return errorMsg;
        }
        final int size = tasks.size();
        for (int index = 0; index < size; index++) {
            com.tapdata.entity.inspect.InspectTask task = tasks.get(index);
            if (task == null) {
                logger.warn("Inspect(hash verify) tasks[{}] is empty", index);
                continue;
            }
            if (StringUtils.isEmpty(task.getTaskId())) {
                errorMsg.add(String.format(InspectService.INSPECT_TASKS_CANNOT_BE_EMPTY, index));
            }
            String source = String.format(InspectService.INSPECT_TASKS_PREFIX_SOURCE, index);
            List<String> sourceErrorMsg = InspectJobUtil.checkRowCountInspectTaskDataSource(source, task.getSource());
            errorMsg.addAll(sourceErrorMsg);

            String target = String.format(InspectService.INSPECT_TASKS_PREFIX_TARGET, index);
            List<String> targetErrorMsg = InspectJobUtil.checkRowCountInspectTaskDataSource(target, task.getTarget());
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
}
