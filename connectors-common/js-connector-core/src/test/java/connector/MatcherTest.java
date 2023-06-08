package connector;

import com.alibaba.fastjson.JSONArray;
import io.tapdata.js.connector.base.JsUtil;
import org.junit.Assert;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatcherTest {

    final String dateUtils = "Date.prototype.format = function (fmt) {\n" +
        "    // debugger;\n" +
        "    let o = {\n" +
        "        'M+': this.getMonth() + 1, // 月份\n" +
        "        'd+': this.getDate(), // 日\n" +
        "        'h+': this.getHours(), // 小时\n" +
        "        'm+': this.getMinutes(), // 分\n" +
        "        's+': this.getSeconds(), // 秒\n" +
        "        'q+': Math.floor((this.getMonth() + 3) / 3), // 季度\n" +
        "        S: this.getMilliseconds() // 毫秒\n" +
        "    }\n" +
        "    if (/(y+)/.test(fmt)) {\n" +
        "        fmt = fmt.replace(\n" +
        "            RegExp.$1,\n" +
        "            (this.getFullYear() + '').substr(4 - RegExp.$1.length)\n" +
        "        )\n" +
        "    }\n" +
        "    for (let k in o) {\n" +
        "        if (new RegExp('(' + k + ')').test(fmt)) {\n" +
        "            fmt = fmt.replace(\n" +
        "                RegExp.$1,\n" +
        "                RegExp.$1.length === 1 ? o[k] : ('00' + o[k]).substr(('' + o[k]).length)\n" +
        "            )\n" +
        "        }\n" +
        "    }\n" +
        "    return fmt\n" +
        "}\n" +
        "\n" +
        "var dateUtils = {\n" +
        "    /**\n" +
        "     *\n" +
        "     * */\n" +
        "    timeStamp2Date: function (millSecondsStr, format){\n" +
        "        let d = format ? new Date(millSecondsStr).format(format) : new Date(millSecondsStr).format('yyyy-MM-dd hh:mm:ss') // 默认日期时间格式 yyyy-MM-dd hh:mm:ss\n" +
        "        return d.toLocaleString();\n" +
        "        //return tapUtil.timeStamp2Date(millSecondsStr, format);\n" +
        "    },\n" +
        "    /**\n" +
        "     * @type function\n" +
        "     * @author Gavin\n" +
        "     * @description 获取当前并格式化 yyyy-mm-dd\n" +
        "     * @return 格式化（yyyy-MM-dd）年-月-日 字符串\n" +
        "     * @date 2023/2/13\n" +
        "     * */\n" +
        "    nowDate: function () {\n" +
        "        return dateUtils.formatDate(new Date().getTime());\n" +
        "        //return tapUtil.nowToDateStr();\n" +
        "    },\n" +
        "\n" +
        "    /**\n" +
        "     * @type function\n" +
        "     * @author Gavin\n" +
        "     * @description 获取当前时间并格式化 yyyy-MM-dd hh:mm:ss\n" +
        "     * @return 格式化（yyyy-MM-dd hh:mm:ss）年-月-日 时:分:秒 字符串\n" +
        "     * @date 2023/2/13\n" +
        "     * */\n" +
        "    nowDateTime: function () {\n" +
        "        return dateUtils.formatDateTime(new Date().getTime());\n" +
        "        //return tapUtil.nowToDateTimeStr();\n" +
        "    },\n" +
        "\n" +
        "    /**\n" +
        "     * @type function\n" +
        "     * @author Gavin\n" +
        "     * @description 根据时间戳进行格式化输出字符串 yyyy-MM-dd\n" +
        "     * @param time 时间戳 Number\n" +
        "     * @return 格式化（yyyy-MM-dd）年-月-日 字符串\n" +
        "     * @date 2023/2/13\n" +
        "     * */\n" +
        "    formatDate: function (time) {\n" +
        "        if ('undefined' === time || null == time) return \"1000-01-01\";\n" +
        "        let date = dateUtils.timeStamp2Date(new Date().getTime(), 'yyyy-MM-dd');\n" +
        "        return date.length > 10 ? '9999-12-31' : date;\n" +
        "        //return tapUtil.longToDateStr(time);\n" +
        "    },\n" +
        "\n" +
        "    /**\n" +
        "     * @type function\n" +
        "     * @author Gavin\n" +
        "     * @description 根据时间戳进行格式化输出字符串 yyyy-MM-dd hh:mm:ss\n" +
        "     * @param time 时间戳 Number\n" +
        "     * @return 格式化（yyyy-MM-dd hh:mm:ss）年-月-日 时:分:秒 字符串\n" +
        "     * @date 2023/2/13\n" +
        "     * */\n" +
        "    formatDateTime: function (time) {\n" +
        "        if ('undefined' === time || null == time) return \"1000-01-01 00:00:00\";\n" +
        "        let date = dateUtils.timeStamp2Date(new Date().getTime(), 'yyyy-MM-dd hh:mm:ss');\n" +
        "        return date.length > 10 ? '9999-12-31 23:59:59' : date;\n" +
        "        //return tapUtil.longToDateStr(time);\n" +
        "    }\n" +
        "};";

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
    @Test
    public void functionTest() throws ScriptException, NoSuchMethodException {
        String script = "function format(msg, args) {\n" +
                "        if ('undefined' === msg || null == msg || \"\" === msg) {\n" +
                "            if (args.length>0){\n" +
                "                msg = \"{}\";\n" +
                "                for (let i = 1; i < args.length; i++) {\n" +
                "                    msg = msg + \", {}\"\n" +
                "                }\n" +
                "            }else {\n" +
                "                return \"\";\n" +
                "            }\n" +
                "        }\n" +
                "        if (typeof msg != 'string'){\n" +
                "            args = [msg];\n" +
                "            msg = \"{}\";\n" +
                "        }\n" +
                "        for (let index = 0; index < args.length; index++) {\n" +
                "            msg = msg.replace(new RegExp(\"(\\{\\})|(%\\s)\"), JSON.stringify(args[index]));\n" +
                "        }\n" +
                "        return msg;\n" +
                "    }";
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("graal.js");
        Object eval = engine.eval(script);
        Invocable invocable = (Invocable) engine;
        Map<String,Object> map = new HashMap<>();
        map.put("key","value");
        JSONArray param = new JSONArray();
        param.add(map);
        Object invoker = engine.eval("var pa = {\"key\":\"value\"};format(pa);");
        System.out.println("1: "+invoker);
        invoker = engine.eval("var pa = [{\"key\":\"value\"}];format(\"{}\",pa);");
        System.out.println("2: "+invoker);
        invoker = engine.eval("var pa = [\"{'key':'value'}\"];format(null,pa);");
        System.out.println("3: "+invoker);
        invoker = engine.eval("var pa = [{\"key\":\"value\"}];format(\"\",pa);");
        System.out.println("4: "+invoker);
        invoker = engine.eval("var pa = [{\"key\":\"value\"},0];format(\"\",pa);");
        System.out.println("5: "+invoker);
        invoker = engine.eval("var pa = [{\"key\":\"value\"},0,\"aaa\"];format(\"\",pa);");
        System.out.println("6: "+invoker);
        param.add(0);
        invoker = engine.eval("var pa = [{\"key\":\"value\"},0];format(\"{}\",pa);");
        System.out.println("7: "+invoker);
    }

    @Test
    public void testJsDateUtil() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("graal.js");
        Object eval = engine.eval(dateUtils);
        Invocable invocable = (Invocable) engine;

        Object invoker = engine.eval("dateUtils.timeStamp2Date(new Date().getTime(), \"yyyy-MM-dd'T'HH:mm:ssXXX\");");
        System.out.println(invoker + " ---- " + new JsUtil().timeStamp2Date(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ssXXX"));
    }
}
