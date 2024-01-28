package com.tapdata.processor;

import com.tapdata.cache.ICacheGetter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.sdk.util.AppType;
import io.tapdata.entity.logger.Log;
import io.tapdata.exception.TapCodeException;
import org.apache.logging.log4j.core.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.script.Invocable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
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
                , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class)));
    }
    @Test
    public void testUrlClassLoader(){
        List<URL> urlList = new ArrayList<>();
        urlList.add(mock(URL.class));
        final ClassLoader[] externalClassLoader = new ClassLoader[1];
        ScriptUtil.urlClassLoader(urlClassLoader -> externalClassLoader[0] = urlClassLoader,urlList);
        Assert.assertNotNull(externalClassLoader[0]);
    }

}
