package com.tapdata.tm.audit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.tapdata.tm.application.controller.ApplicationController;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.aop.LogAOP;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.filter.RequestFilter;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.user.controller.UserController;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.springframework.aop.aspectj.annotation.AnnotationAwareAspectJAutoProxyCreator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AuditCredentialLoggingIT {
    @Test
    void reveal_responses_reach_clients_but_not_debug_or_trace_logs() throws Exception {
        String userId = new ObjectId().toHexString();
        String otherId = new ObjectId().toHexString();
        String clientId = new ObjectId().toHexString();
        String ownCode = "synthetic-own-access-code";
        String otherCode = "synthetic-other-access-code";
        String clientSecret = "synthetic-client-secret";
        UserDetail login = mock(UserDetail.class);
        when(login.getUserId()).thenReturn(userId);
        UserDto ownUser = new UserDto();
        ownUser.setAccessCode(ownCode);
        UserDto otherUser = new UserDto();
        otherUser.setAccessCode(otherCode);
        UserService users = mock(UserService.class);
        when(users.findById(new ObjectId(userId))).thenReturn(ownUser);
        when(users.findById(new ObjectId(otherId))).thenReturn(otherUser);
        UserController userController = spy(new UserController());
        ReflectionTestUtils.setField(userController, "userService", users);
        doReturn(login).when(userController).getLoginUser();
        ApplicationDto client = new ApplicationDto();
        client.setClientSecret(clientSecret);
        ApplicationService applications = mock(ApplicationService.class);
        when(applications.findById(eq(new ObjectId(clientId)), any(Field.class), same(login))).thenReturn(client);
        ApplicationController applicationController = spy(new ApplicationController());
        ReflectionTestUtils.setField(applicationController, "applicationService", applications);
        doReturn(login).when(applicationController).getLoginUser();

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        List<Logger> loggers = List.of(
                (Logger) LoggerFactory.getLogger(UserController.class),
                (Logger) LoggerFactory.getLogger(ApplicationController.class),
                (Logger) LoggerFactory.getLogger(RequestFilter.class),
                (Logger) LoggerFactory.getLogger("org.springframework.web.servlet.mvc.method.annotation.RequestResponseBodyMethodProcessor"));
        List<Level> levels = loggers.stream().map(Logger::getLevel).toList();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        root.addAppender(appender);
        loggers.forEach(logger -> logger.setLevel(Level.TRACE));
        try (GenericApplicationContext context = new GenericApplicationContext();
             MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class)) {
            permissions.when(() -> DataPermissionHelper.checkOfQuery(any(), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
            context.registerBean(AnnotationAwareAspectJAutoProxyCreator.class);
            context.registerBean(LogAOP.class);
            context.registerBean(UserController.class, () -> userController);
            context.registerBean(ApplicationController.class, () -> applicationController);
            context.refresh();
            MockMvc http = MockMvcBuilders.standaloneSetup(context.getBean(UserController.class),
                    context.getBean(ApplicationController.class)).addFilters(new RequestFilter()).build();
            assertThat(http.perform(post("/api/users/access-code/reveal"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString()).contains(ownCode);
            assertThat(http.perform(post("/api/users/{id}/access-code/reveal", otherId))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString()).contains(otherCode);
            assertThat(http.perform(post("/api/Applications/{id}/secret/reveal", clientId))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString()).contains(clientSecret);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).filteredOn(message -> message.contains("result:[omitted sensitive response]")).hasSize(3);
            assertThat(messages).filteredOn(message -> message.contains("Writing [SensitiveResponseMessage")).hasSize(3);
            assertThat(messages).filteredOn(message -> message.contains("[omitted credential response body]")).hasSize(3);
            assertThat(String.join("\n", messages)).doesNotContain(ownCode, otherCode, clientSecret);
        } finally {
            root.detachAppender(appender);
            appender.stop();
            for (int index = 0; index < loggers.size(); index++) {
                loggers.get(index).setLevel(levels.get(index));
            }
        }
    }
}
