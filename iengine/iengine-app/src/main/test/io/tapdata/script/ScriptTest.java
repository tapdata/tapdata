package io.tapdata.script;

import com.tapdata.processor.ScriptUtil;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.script.TapJavaScriptEngine;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import static org.junit.Assert.assertEquals;

public class ScriptTest {
	@Test
	public void test() throws ScriptException {
		ScriptEngine engine = ScriptUtil.getScriptEngine();
		ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
		ScriptEngine engine1 = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions());
		Object value = engine1.eval("1 + 1");
		assertEquals(2, value);
	}
}
