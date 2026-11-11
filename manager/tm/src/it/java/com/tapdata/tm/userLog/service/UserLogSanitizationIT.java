package com.tapdata.tm.userLog.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.repository.UserLogRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2：真实审计服务、DTO转换、脱敏器和文案渲染组装，存储为外部边界。 */
@Timeout(10)
class UserLogSanitizationIT {
    @Test
    @DisplayName("审计组件链路：写入到列表输出不含凭证，历史文案同样受保护")
    void should_mask_write_and_query_boundaries() {
        UserLogRepository repository = mock(UserLogRepository.class);
        UserDetail user = mock(UserDetail.class);
        when(user.getUsername()).thenReturn("operator");
        when(user.getUserId()).thenReturn("64b000000000000000000001");
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(UserLogRepository.class, () -> repository);
            context.registerBean(UserLogServiceImpl.class, () -> new UserLogServiceImpl(repository));
            context.refresh();
            UserLogServiceImpl service = context.getBean(UserLogServiceImpl.class);
            ReflectionTestUtils.setField(service, "userLogRepository", repository);

            service.addUserLog(Modular.MCP, Operation.READ, user, (String) null,
                    "MCP POST /mcp?accessCode=synthetic-write");
            ArgumentCaptor<UserLogs> stored = ArgumentCaptor.forClass(UserLogs.class);
            verify(repository).insert(stored.capture(), eq(user));
            assertEquals("MCP POST /mcp?accessCode=***", stored.getValue().getParameter1());

            UserLogs legacy = new UserLogs();
            legacy.setId(new ObjectId());
            legacy.setUsername("operator");
            legacy.setModular("mcp");
            legacy.setOperation("unknown-operation");
            legacy.setParameter1("MCP POST /mcp?accessCode=synthetic-history");
            Filter filter = new Filter(new Where());
            when(repository.findAll(any(Filter.class))).thenReturn(List.of(legacy));
            when(repository.count(any(Where.class))).thenReturn(1L);
            UserLogDto result = service.find(filter, user, true).getItems().get(0);
            assertEquals("MCP POST /mcp?accessCode=***", result.getParameter1());
            assertFalse(result.getI18nMessage().contains("synthetic-history"));
            assertEquals("MCP POST /mcp?accessCode=synthetic-history", legacy.getParameter1());
        }
    }
}
