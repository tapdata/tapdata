package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.Log4jScriptLogger;
import com.tapdata.processor.LoggingOutputStream;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.exception.TapCodeException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.graalvm.polyglot.Context;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.python.core.Py;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author GavinXiao
 * @description HazelcastPythonProcessorNodeTest create by Gavin
 * @create 2023/6/19 16:54
 **/
public class HazelcastPythonProcessorNodeTest extends BaseTest {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNodeTest.class);

//    @Test
    public void testLog() {
        try (Context context = Context.newBuilder("python")
                .allowAllAccess(true)
                .option("engine.WarnInterpreterOnly", "false")
                .logHandler(System.err)
                .out(new LoggingOutputStream(new Log4jScriptLogger(logger), Level.INFO)).build()) {
            context.eval("python", "print ('fdasfdsafdsafad')");
        }
    }

    @Test
    public void veryModules() throws ScriptException {
        Thread.currentThread().setName("test-->");
        lib("D:\\GavinData\\deskTop\\Lib\\site-packages");
        ScriptEngine e = new ScriptEngineManager().getEngineByName("python");
        ScriptEngineFactory factory = e.getFactory();
        e.put("tapLog", logger);
        //SimpleScriptContext ctxt = new SimpleScriptContext();
        //ctxt.setWriter(new OutputStreamWriter(new LoggingOutputStream(new Log4jScriptLogger(logger), Level.INFO)));
        //e.eval("tapLog.warn('Test import:')");
        //e.eval("import site;");
        //e.eval("addsitedir('D:\\GavinData\\kitSpace\\pip-lib\\requests-2.31.0\\requests')");
        //e.eval("export PYTHONPATH=D:\\GavinData\\kitSpace\\pip-lib\\requests-2.31.0/:$PYTHONPATH");
        //import json, yaml, random, time, datetime, uuid, types
        //import urllib, urllib2, requests
        //import math, hashlib, base64
        System.out.println();
        //((PyScriptEngine)e).interp.systemState.path.remove(0)
        List<String> support = new ArrayList<>();
        List<String> unSupport = new ArrayList<>();
        eval(e, support, unSupport
                ,"json"
                ,"yaml"
                ,"random"
                ,"time"
                ,"datetime"
                ,"uuid"
                ,"types"
                ,"urllib"
                ,"urllib2"
                ,"requests"
                ,"math"
                ,"hashlib"
                ,"base64");
        System.out.println(
                "Such modules are supported: " + support.toString() +
                ";\n\nBut such is not supported: " + unSupport.toString());

    }

    private void eval(ScriptEngine e, String type, List<String> support, List<String> unSupport){
        try {
            e.eval("import " + type + ";\n");
            support.add(type);
        } catch (Exception e1){
            unSupport.add(type);
        }
    }


    private void eval(ScriptEngine e, List<String> support, List<String> unSupport, String ... type) {
        for (String t : type) {
            eval(e, t, support, unSupport);
        }
    }

//    @Test
    public void fun(){
        PythonInterpreter python = new PythonInterpreter();

        StringBuilder script = new StringBuilder();
//        script.append("import ensurepip\n");
//        script.append("ensurepip._main()");
//        python.exec(script.toString());

//        script = new StringBuilder();
//        script.append("import pip\n");
//        script.append("pip.main(['install', '--upgrade', 'pip'])");

//        script = new StringBuilder();
//        script.append("import pypi\n");
//        script.append("pypi.install('requests')");


//        script = new StringBuilder();
//        script.append("import os\n");
//        script.append("os.system('pip install requests')");
//        python.exec(script.toString());

        script = new StringBuilder();
        script.append("import sys\n");
        script.append("import subprocess\n");
        script.append("import pkg_resources\n");
        script.append("from pkg_resources import DistributionNotFound, VersionConflict\n");
        script.append("\n");
        script.append("def should_install_requirement(requirement):\n");
        script.append("    should_install = False\n");
        script.append("    try:\n");
        script.append("        pkg_resources.require(requirement)\n");
        script.append("    except (DistributionNotFound, VersionConflict):\n");
        script.append("        should_install = True\n");
        script.append("    return should_install\n");
        script.append("\n");
        script.append("\n");
        script.append("def install_packages(requirement_list):\n");
        script.append("    try:\n");
        script.append("        requirements = [\n");
        script.append("            requirement\n");
        script.append("            for requirement in requirement_list\n");
        script.append("            if should_install_requirement(requirement)\n");
        script.append("        ]\n");
        script.append("        if len(requirements) > 0:\n");
        script.append("            subprocess.check_call([sys.executable, \"-m\", \"pip\", \"install\", *requirements])\n");
        script.append("        else:\n");
        script.append("            print(\"Requirements already satisfied.\")\n");
        script.append("\n");
        script.append("    except Exception as e:\n");
        script.append("        print(e)");
        script.append("requirement_list = ['requests', 'httpx==0.18.2']\n");
        script.append("install_packages(requirement_list)");
        python.exec(script.toString());
    }


//    @Test
    public void lib(String path){
        //import org.python.core.Py;
        //import org.python.core.PySystemState;
        PySystemState sys = Py.getSystemState();
        //sys.path.remove(0);
        //sys.path.add(path);
        System.out.println(sys.path.toString());
        //sys.path.add("D:\\GavinData\\deskTop\\Lib");
//        sys.path.remove("E:\\sacaapm-paserver\\src-python\\jython\\Lib");
    }

    @Test
    public void testNeedCopyBatchEventWrapper(){
        HazelcastPythonProcessNode hazelcastPythonProcessNode =  new HazelcastPythonProcessNode(mock(ProcessorBaseContext.class));
        Assertions.assertTrue(hazelcastPythonProcessNode.needCopyBatchEventWrapper());
    }
    @Test
    public void test1() throws ScriptException, NoSuchMethodException {
        HazelcastPythonProcessNode hazelcastPythonProcessNode = mock(HazelcastPythonProcessNode.class);
        doCallRealMethod().when(hazelcastPythonProcessNode).tryProcess(any(TapdataEvent.class),any());
        TapdataEvent tapdataEvent=new TapdataEvent();
        TapUpdateRecordEvent tapUpdateRecordEvent=TapUpdateRecordEvent.create();
        Map<String,Object> after=new HashMap<>();
        after.put("name","test1");
        tapdataEvent.setTapEvent(tapUpdateRecordEvent);
        HazelcastProcessorBaseNode.ProcessResult processResult=new HazelcastProcessorBaseNode.ProcessResult();
        when(hazelcastPythonProcessNode.getProcessResult(any())).thenReturn(processResult);
        ProcessorBaseContext processorBaseContext = mock(ProcessorBaseContext.class);
        TaskDto taskDto=new TaskDto();
        taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        Invocable engine = mock(Invocable.class);
        when(engine.invokeFunction(anyString(),eq(null),anyMap())).thenThrow(new ScriptException("failed"));
        ReflectionTestUtils.setField(hazelcastPythonProcessNode,"engine",engine);
        ReflectionTestUtils.setField(hazelcastPythonProcessNode,"processorBaseContext",processorBaseContext);
        ThreadLocal<Map<String, Object>> threadLocal=new ThreadLocal<>();
        Map<String, Object> context =new HashMap<>();
        threadLocal.set(context);
        ReflectionTestUtils.setField(hazelcastPythonProcessNode,"processContextThreadLocal",threadLocal);
        doCallRealMethod().when(hazelcastPythonProcessNode).getProcessorBaseContext();
        TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
            hazelcastPythonProcessNode.tryProcess(tapdataEvent, (event, result) -> {

            });
        });
        assertEquals(ScriptProcessorExCode_30.PYTHON_PROCESS_FAILED,tapCodeException.getCode());
    }

}
