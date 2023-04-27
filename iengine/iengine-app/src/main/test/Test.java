import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) throws ScriptException, IOException, InterruptedException {
//		Engine engine = Engine.newBuilder().option("js.load-from-url","true").option("js.nashorn-compat", "true").allowExperimentalOptions(true).build();
//
//		Context context = Context.newBuilder().allowValueSharing(true).allowAllAccess(true).allowHostClassLoading(true).allowIO(true).engine(engine).build();
////		Source mysource = Source.newBuilder("js"," load(\"iengine/iengine-app/src/main/test/index.js\")","iengine/iengine-app/src/main/test/index.js").build();
//		Source mysource = Source.newBuilder("js", new File("iengine/iengine-app/src/main/java/io/tapdata/index.js")).build();
////		Source mysource = Source.newBuilder("js", new File("/Users/aplomb/dev/code/NewTapdataProjects/tapdata_enterprise/iengine/iengine-app/src/main/test/index.js")).build();
//		context.eval(mysource);
//
//		Value callapp = context.getBindings("js").getMember("hello");
//		Map<String, Object> map = map(entry("a", 1));
//
//		System.out.println("js: " + callapp.execute(map));
//		Test test = new Test();
//		System.out.println("java: " + test.process(map));

//		int times = 1000000;
//		long time = System.currentTimeMillis();
//		for(int i = 0; i < times; i++) {
//			/**
//			 * function process(record) {
//			 *     record["date"]= new Date();
//			 *     return record;
//			 * }
//			 */
//			callapp.execute(map);
//		}
//		System.out.println("js takes " + (System.currentTimeMillis() - time));
////		js takes 577
//
//
//		time = System.currentTimeMillis();
//		for(int i = 0; i < times; i++) {
//			/**
//			 * public Map<String, Object> process(Map<String, Object> record) {
//			 * 		record.put("date", new Date());
//			 * 		return record;
//			 * }
//			 */
//			test.process(map);
//		}
//		System.out.println("java takes " + (System.currentTimeMillis() - time));
//		java takes 29


//		synchronized (engine) {
//			engine.wait(1000000L);
//		}
		Context.Builder contextBuilder = Context.newBuilder("js")
				.allowAllAccess(true)
				.option("js.nashorn-compat", "true")
				.allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
						.targetTypeMapping(Value.class, Object.class
								, v -> v.hasArrayElements() && v.hasMembers()
								, v -> v.as(List.class)
						).build()
				);
		GraalJSScriptEngine scriptEngine = GraalJSScriptEngine
				.create(null,
						contextBuilder
				);

//		context.eval(Source.newBuilder("js", new File("iengine/iengine-app/src/main/test/index.js")).build());
		scriptEngine.eval("console.log('aaaa');");
		scriptEngine.eval("this.load(\"" + "iengine/iengine-app/src/main/java/io/tapdata/index.js" + "\")");

	}

	public Map<String, Object> process(Map<String, Object> record) {
		record.put("date", new Date());
		return record;
	}
}
