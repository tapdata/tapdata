package io.tapdata.flow.engine.V2.script;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.HazelcastUtil;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.*;
import java.io.IOException;
import java.io.StringReader;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TapJavaScriptEngineTest {

    @Mock
    private ScriptOptions mockScriptOptions;
    @Mock
    private URLClassLoader mockExternalJarClassLoader;

    private TapJavaScriptEngine tapJavaScriptEngineUnderTest;

    @Before
    public void setUp() {
        try(MockedStatic<HazelcastUtil> mockedStatic = Mockito.mockStatic(HazelcastUtil.class) ){
            mockedStatic.when(HazelcastUtil::getInstance).thenReturn(mock(HazelcastInstance.class));
            tapJavaScriptEngineUnderTest = new TapJavaScriptEngine(mockScriptOptions);
            ReflectionTestUtils.setField(tapJavaScriptEngineUnderTest, "externalJarClassLoader",
                    mockExternalJarClassLoader);
        }
    }

    @Test
    public void testInitScriptEngine(){
        Log log = new TapScriptLogger("test");
        ScriptEngine result = tapJavaScriptEngineUnderTest.initScriptEngine("test",log);
        Assert.assertEquals(log,result.get("log"));
    }


}
