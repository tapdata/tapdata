package com.tapdata.tm.audit;

import com.tapdata.tm.audit.query.AuditEventVo;
import com.tapdata.tm.audit.query.AuditQueryService;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.service.UserLogServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** L2：升级前的历史审计记录在查询、详情和导出共用的转换边界读取脱敏。 */
@Timeout(10)
class AuditLegacyRedactionIT {
    private AuditEventVo toVo(UserLogs legacy) throws Exception {
        AuditQueryService service = new AuditQueryService(new AuditItFixture().mongoTemplate, mock(UserLogServiceImpl.class));
        Method toVo = AuditQueryService.class.getDeclaredMethod("toVo", UserLogs.class, Locale.class);
        toVo.setAccessible(true);
        return (AuditEventVo) toVo.invoke(service, legacy, Locale.SIMPLIFIED_CHINESE);
    }

    @Test
    @DisplayName("历史记录的裸凭证与参数值在展示字段中已脱敏")
    void should_mask_legacy_credentials() throws Exception {
        UserLogs legacy = new UserLogs();
        legacy.setId(new ObjectId());
        legacy.setModular("accessCode");
        legacy.setParameter1("synthetic-bare-code");
        legacy.setParameter2("token=synthetic-token");

        AuditEventVo vo = toVo(legacy);

        assertThat(vo.getObjectName()).isEqualTo("***");
        assertThat(vo.getLegacyFields())
                .containsEntry("parameter1", "***")
                .containsEntry("sourceName", "***")
                .containsEntry("parameter2", "token=***");
    }

    @Test
    @DisplayName("普通历史记录字段保持原值")
    void should_keep_non_secret_legacy_fields() throws Exception {
        UserLogs legacy = new UserLogs();
        legacy.setId(new ObjectId());
        legacy.setModular("migration");
        legacy.setParameter1("orders");
        legacy.setParameter2("hardy");

        AuditEventVo vo = toVo(legacy);

        assertThat(vo.getObjectName()).isEqualTo("orders");
        assertThat(vo.getLegacyFields())
                .containsEntry("parameter1", "orders")
                .containsEntry("parameter2", "hardy");
    }
}
