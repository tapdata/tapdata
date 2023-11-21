package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.processor.Log4jScriptLogger;
import com.tapdata.processor.LoggingOutputStream;
import io.tapdata.base.BaseTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.graalvm.polyglot.Context;
import org.junit.Test;
import org.python.core.Py;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

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
}
