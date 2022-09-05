package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.processor.LoggingOutputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.junit.Test;

import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.OutputStreamWriter;
import java.util.List;

public class HazelcastJavaScriptProcessorNodeTest extends BaseTest {

  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNodeTest.class);


  @Test
  public void testLog() {
    try (Context context = Context.newBuilder("js")
            .allowAllAccess(true)
            .option("engine.WarnInterpreterOnly", "false")
            .logHandler(System.err)
            .out(new LoggingOutputStream(logger, Level.INFO)).build()) {

      context.eval("js", "console.info('fdasfdsafdsafad')");


    }
  }

  @Test
  public void testLog1() throws ScriptException {

    GraalJSScriptEngine graalJSScriptEngine = GraalJSScriptEngine
            .create(Engine.newBuilder()
                            .allowExperimentalOptions(true)
                            .option("engine.WarnInterpreterOnly", "false")
                            .option("js.v8-compat", "true")
                            .out(new LoggingOutputStream(logger, Level.INFO))
                            .err(new LoggingOutputStream(logger, Level.ERROR))
                            .build(),
                    Context.newBuilder("js")
                            .allowAllAccess(true)
                            .allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
                                    .targetTypeMapping(Value.class, Object.class,
                                            v -> v.hasArrayElements() && v.hasMembers(), v -> v.as(List.class))
                                    .build()
                            )
                            .out(new LoggingOutputStream(logger, Level.INFO))
                            .err(new LoggingOutputStream(logger, Level.ERROR))
                            .logHandler(new LoggingOutputStream(logger, Level.ERROR))
            );


    SimpleScriptContext ctxt = new SimpleScriptContext();
    ctxt.setWriter(new OutputStreamWriter(new LoggingOutputStream(logger, Level.INFO)));
    graalJSScriptEngine.eval("console.info('fdasfdsafdsafad')", ctxt);

    graalJSScriptEngine.close();

  }

}