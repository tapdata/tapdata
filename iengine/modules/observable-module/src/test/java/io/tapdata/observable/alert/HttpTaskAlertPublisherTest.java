package io.tapdata.observable.alert;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.logger.alert.TapAlertType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpTaskAlertPublisherTest {

    @Test
    void successShouldReturnSuccess() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(operator);

        Assertions.assertEquals(TaskAlertPublisher.PublishResult.SUCCESS, publisher.publish(sampleEvent()));
        verify(operator).insertOne(any(), anyString());
    }

    @Test
    void conflictShouldBeTreatedAsSuccess() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        doThrow(httpClientError(HttpStatus.CONFLICT)).when(operator).insertOne(any(), anyString());
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(operator);

        Assertions.assertEquals(TaskAlertPublisher.PublishResult.SUCCESS, publisher.publish(sampleEvent()));
    }

    @Test
    void badRequestShouldBeNonRetryable() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        doThrow(httpClientError(HttpStatus.BAD_REQUEST)).when(operator).insertOne(any(), anyString());
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(operator);

        Assertions.assertEquals(TaskAlertPublisher.PublishResult.NON_RETRYABLE, publisher.publish(sampleEvent()));
    }

    @Test
    void wrappedClientErrorShouldStillBeClassified() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        doThrow(new RuntimeException("tm rejected", httpClientError(HttpStatus.UNPROCESSABLE_ENTITY)))
                .when(operator).insertOne(any(), anyString());
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(operator);

        Assertions.assertEquals(TaskAlertPublisher.PublishResult.NON_RETRYABLE, publisher.publish(sampleEvent()));
    }

    @Test
    void tooManyRequestsShouldBeRetryable() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        doThrow(httpClientError(HttpStatus.TOO_MANY_REQUESTS)).when(operator).insertOne(any(), anyString());
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(operator);

        Assertions.assertEquals(TaskAlertPublisher.PublishResult.RETRYABLE, publisher.publish(sampleEvent()));
    }

    @Test
    void missingOperatorShouldBeRetryable() {
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(null);
        Assertions.assertEquals(TaskAlertPublisher.PublishResult.RETRYABLE, publisher.publish(sampleEvent()));
    }

    @Test
    void shouldResolveOperatorAfterSpringContextAppears() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        when(context.containsBean(HttpTaskAlertPublisher.CLIENT_MONGO_OPERATOR_BEAN)).thenReturn(true);
        when(context.getBean(HttpTaskAlertPublisher.CLIENT_MONGO_OPERATOR_BEAN, ClientMongoOperator.class))
                .thenReturn(operator);

        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher();
        ConfigurableApplicationContext previous = BeanUtil.configurableApplicationContext;
        try {
            BeanUtil.configurableApplicationContext = null;
            Assertions.assertEquals(TaskAlertPublisher.PublishResult.RETRYABLE, publisher.publish(sampleEvent()));
            verify(operator, never()).insertOne(any(), anyString());

            BeanUtil.configurableApplicationContext = context;
            Assertions.assertEquals(TaskAlertPublisher.PublishResult.SUCCESS, publisher.publish(sampleEvent()));
            verify(operator).insertOne(any(), eq(HttpTaskAlertPublisher.RESOURCE));
        } finally {
            BeanUtil.configurableApplicationContext = previous;
        }
    }

    @Test
    void injectedOperatorShouldNotBeReplacedByLaterBeanLookup() {
        ClientMongoOperator injected = mock(ClientMongoOperator.class);
        ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        HttpTaskAlertPublisher publisher = new HttpTaskAlertPublisher(injected);
        ConfigurableApplicationContext previous = BeanUtil.configurableApplicationContext;
        try {
            BeanUtil.configurableApplicationContext = context;
            Assertions.assertEquals(TaskAlertPublisher.PublishResult.SUCCESS, publisher.publish(sampleEvent()));
            verify(injected).insertOne(any(), anyString());
            verify(context, never()).getBean(anyString(), eq(ClientMongoOperator.class));
        } finally {
            BeanUtil.configurableApplicationContext = previous;
        }
    }

    private TaskAlertEvent sampleEvent() {
        return TaskAlertEvent.builder()
                .eventId("evt-1")
                .type(TapAlertType.DATA_INTEGRITY)
                .code("SOURCE_CDC_EVENT_DISCARDED")
                .dedupKey("CFPCN")
                .taskId("task")
                .taskName("task")
                .message("discarded")
                .occurredAt(1L)
                .build();
    }

    private HttpClientErrorException httpClientError(HttpStatus status) {
        return HttpClientErrorException.create(status, status.getReasonPhrase(), HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
    }
}
