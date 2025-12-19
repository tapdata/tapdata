package io.tapdata.task.skiperrortable;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableRecoveredVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableReportVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 错误表跳过-状态存储实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/8 15:28 Create
 */
public class SkipErrorTableStorage {
    private final HttpClientMongoOperator mongoOperator;

    public SkipErrorTableStorage(HttpClientMongoOperator mongoOperator) {
        this.mongoOperator = mongoOperator;
    }

    public void reportTableSkipped(String taskId, SkipErrorTableReportVo reportVo) {
        String postUrl = String.format("task/%s/skip-error-table", taskId);
        Map<String, Object> postData = reportVo.toMap();
        mongoOperator.postOne(postData, postUrl, TaskSkipErrorTableDto.class);
    }

    public void reportTableRecovered(String taskId, String... tables) {
        String postUrl = String.format("task/%s/skip-error-table-recovered", taskId);
        Map<String, Object> postData = SkipErrorTableRecoveredVo.create(tables).toMap();
        mongoOperator.postOne(postData, postUrl, Boolean.class);
    }

    public List<SkipErrorTableStatusVo> getAllTableStatus(String taskId) {
        String postUrl = String.format("task/%s/skip-error-table-status", taskId);
        return mongoOperator.find(new HashMap<>(), postUrl, SkipErrorTableStatusVo.class);
    }
}
