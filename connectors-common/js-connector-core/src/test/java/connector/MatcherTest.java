package connector;

import org.junit.Assert;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;

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
    public void function() throws ScriptException, NoSuchMethodException {
        String script = "function format(msg,args){\n" +
                "        if ('undefined' === msg || null == msg || \"\" === msg) return \"\";\n" +
                "        for(let index = 1; index < arguments.length ;index++){\n" +
                "            let arg = arguments[index];\n" +
                "            let typeArg = typeof arg;\n" +
                "            let outputArg = '';\n" +
                "            switch (typeArg){\n" +
                "                case \"bigint\":outputArg = arg;break;\n" +
                "                case \"boolean\":outputArg = arg;break;\n" +
                "                case \"number\":outputArg = arg;break;\n" +
                "                case \"string\": outputArg = arg;break;\n" +
                "                case \"undefined\": outputArg = 'undefined';break;\n" +
                "                case \"symbol\": outputArg = arg;break;\n" +
                "                case \"function\": outputArg = arg;break;\n" +
                "                case \"object\": outputArg = arg;break;\n" +
                "                default: outputArg = arg;\n" +
                "            }\n" +
                "            msg = msg.replace(new RegExp(\"(\\{\\})|(%\\s)\"), outputArg);" +
                "        }\n" +
                "        return msg;\n" +
                "    }";
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("graal.js");
        Object eval = engine.eval(script);
        Invocable invocable = (Invocable) engine;
        Map<String,Object> map = new HashMap<>();
        map.put("key","value");
        Object invoker = invocable.invokeFunction("format","{}This is message param-1:{},param-2:%s","A",10,map);
        System.out.println(invoker);
    }
}
