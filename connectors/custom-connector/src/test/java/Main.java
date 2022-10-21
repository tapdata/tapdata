import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import org.junit.Test;

import javax.script.ScriptEngine;

public class Main {
    @Test
    public void test(){
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
    }
}
