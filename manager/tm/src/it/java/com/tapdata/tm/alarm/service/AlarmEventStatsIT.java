package com.tapdata.tm.alarm.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.alarm.dto.AlarmEventStatsVo;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.userLog.service.UserLogService;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** L1：真实 MongoDB 验证告警统计口径，测试数据库名称随机隔离。 */
@Timeout(60)
class AlarmEventStatsIT {
    private static String mongoUri() {
        String uri = System.getenv("TAPDATA_IT_MONGO_URI");
        assumeTrue(uri != null && !uri.isBlank(), "需要已授权临时 MongoDB：TAPDATA_IT_MONGO_URI");
        return uri;
    }

    @Test
    @DisplayName("今日恢复次数不重复统计引擎上线通知事件")
    void should_not_double_count_recovered_engine_events() {
        String database = "tap13209_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoClient client = MongoClients.create(mongoUri())) {
            MongoTemplate mongo = new MongoTemplate(client, database);
            try {
                ModuleAccess moduleAccess = mock(ModuleAccess.class);
                when(moduleAccess.allData(any(UserDetail.class), anyString())).thenReturn(true);
                AlarmEventQueryService service = new AlarmEventQueryService(mongo,
                        mock(TaskService.class), mock(InspectService.class), mock(AlarmService.class),
                        mock(NotificationDeliveryService.class), moduleAccess, mock(UserLogService.class));

                Date now = new Date();
                mongo.getCollection("AlarmInfo").insertMany(List.of(
                        recovered("TASK_RETRY_WARN", now),
                        recovered("ENGINE_OFFLINE", now),
                        // 引擎上线是恢复通知事件，其离线事件已计入恢复次数
                        recovered("ENGINE_ONLINE", now)));

                AlarmEventStatsVo stats = service.stats(mock(UserDetail.class));

                assertEquals(2, stats.getTodayRecovered());
            } finally {
                client.getDatabase(database).drop();
            }
        }
    }

    private static Document recovered(String metric, Date recoveryTime) {
        return new Document("metric", metric).append("status", "RECOVER")
                .append("level", "RECOVERY").append("recoveryTime", recoveryTime)
                .append("firstOccurrenceTime", recoveryTime);
    }
}
