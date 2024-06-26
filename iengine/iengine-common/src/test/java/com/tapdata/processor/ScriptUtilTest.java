package com.tapdata.processor;

import com.tapdata.cache.ICacheGetter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.entity.logger.Log;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.core.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;

import javax.script.Invocable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class ScriptUtilTest {

    @Test
    public void testGetScriptEngine3() throws Exception {
        final ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        final ICacheGetter memoryCacheGetter = mock(ICacheGetter.class);
        final Log logger = mock(Log.class);
        final Invocable result = ScriptUtil.getScriptEngine("script", "",null, clientMongoOperator, mock(ScriptConnection.class), mock(ScriptConnection.class),
                memoryCacheGetter, logger);
        Assert.assertNotNull(result);

    }
    @Test
    public void testInvokeScript()  {
        Map<String,Object> input = new HashMap<>();
        Assertions.assertThrows(TapCodeException.class,()-> ScriptUtil.invokeScript(null,"functionName", mock(MessageEntity.class)
                , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), null));
    }
    @Test
    public void testUrlClassLoader(){
        List<URL> urlList = new ArrayList<>();
        urlList.add(mock(URL.class));
        final ClassLoader[] externalClassLoader = new ClassLoader[1];
        ScriptUtil.urlClassLoader(urlClassLoader -> externalClassLoader[0] = urlClassLoader,urlList);
        Assert.assertNotNull(externalClassLoader[0]);
    }
    @Nested
    public class InvokeScriptWithTagTest{
        private Map<String,Object> input;
        private Invocable engine;
        private MessageEntity message;
        @BeforeEach
        @SneakyThrows
        void beforeEach(){
            input = new HashMap<>();
            engine = spy(ScriptUtil.getScriptEngine(
                    JSEngineEnum.GRAALVM_JS.getEngineName(),
                    "",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    true));
            message = mock(MessageEntity.class);

        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithBefore(){
            when(message.getBefore()).thenReturn(mock(HashMap.class));
            when(message.getAfter()).thenReturn(mock(HashMap.class));
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "before");
            verify(message,new Times(2)).getAfter();
            verify(message,new Times(3)).getBefore();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithoutBefore(){
            when(message.getAfter()).thenReturn(mock(HashMap.class));
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "before");
            verify(message,new Times(3)).getAfter();
            verify(message,new Times(2)).getBefore();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithAfter(){
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "after");
            verify(message,new Times(2)).getBefore();
            verify(message,new Times(1)).getAfter();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithNull(){
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), null);
            verify(message,new Times(2)).getBefore();
            verify(message,new Times(1)).getAfter();
        }

    }
}
