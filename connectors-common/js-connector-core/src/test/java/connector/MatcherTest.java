package connector;

import com.sun.xml.internal.ws.api.pipe.Engine;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.util.List;

import static io.tapdata.base.ConnectorBase.toJson;

public class MatcherTest {
    @Test
    public void testMatch() {
        String functionInvoker = "(3)[\"A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")) {
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)", "");
        }
        Assert.assertEquals("+", functionInvoker, "[\"A\",\"B\",\"C\"]");

        functionInvoker = "(3)[\"(3)A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")) {
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)", "");
        }
        Assert.assertEquals("+", functionInvoker, "[\"(3)A\",\"B\",\"C\"]");

        functionInvoker = "[\"(3)A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")) {
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)", "");
        }
        Assert.assertEquals("+", functionInvoker, "[\"(3)A\",\"B\",\"C\"]");

        functionInvoker = "[\"A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")) {
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)", "");
        }
        Assert.assertEquals("+", functionInvoker, "[\"A\",\"B\",\"C\"]");

    }

    @Test
    public void testJavaScript() throws ScriptException, NoSuchMethodException {
        String script = "function test(){" +
                "   var maps = {\"key\":\"111\",\"key1\":\"values\"};" +
                "   return \"\\\"\"+Object.values(maps).join(\"\\\",\\\"\") +\"\\\"\"" +
                "}";
//        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
//        ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("graal.js");
        //ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "engine"); //script factory
        //ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));

        Object eval = engine.eval(script);
        Invocable invocable = (Invocable) engine;
        Object invoker = invocable.invokeFunction("test");
        System.out.println(invoker);
    }
    @Test
    public void testJavaScript2() throws ScriptException, NoSuchMethodException {
        String script = "function test(){" +
                "   var maps = \"1333,226\";" +
                "   return maps.split(',')" +
                "}";
//        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
//        ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("graal.js");
        //ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "engine"); //script factory
        //ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));

        Object eval = engine.eval(script);
        Invocable invocable = (Invocable) engine;
        Object invoker = invocable.invokeFunction("test");
        System.out.println(invoker);

        System.out.println(System.currentTimeMillis());
    }
}
