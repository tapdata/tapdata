package py;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.Arrays;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.junit.jupiter.api.Test;
import org.python.core.Options;
import org.python.core.PyList;
import org.python.core.PyString;
import org.python.jsr223.PyScriptEngineFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * @author GavinXiao
 * @description TestPyEngine create by Gavin
 * @create 2023/6/13 16:48
 **/
public class TestPyEngine {

    @Test
    public void testEvalString() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        new PyScriptEngineFactory().getScriptEngine().eval("print 'Hello!Gavin, I\\'m python.'");
        context.setAttribute("javax.script.filename", "sample.py", 100);
        assertNull(pythonEngine.eval("x = 5"));
        assertEquals(5, pythonEngine.eval("x"));
        assertEquals("sample.py", pythonEngine.eval("__file__"));
        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("sample.py"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testEvalStringArgv() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        context.setAttribute("javax.script.filename", "sample.py", 100);
        context.setAttribute("javax.script.argv", new String[]{"foo", "bar"}, 100);
        assertNull(pythonEngine.eval("x = 5"));
        assertEquals(5, pythonEngine.eval("x"));
        assertEquals("sample.py", pythonEngine.eval("__file__"));
        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("sample.py", "foo", "bar"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testEvalStringNoFilenameWithArgv() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        context.setAttribute("javax.script.argv", new String[]{"foo", "bar"}, 100);
        assertNull(pythonEngine.eval("x = 5"));
        assertEquals(5, pythonEngine.eval("x"));
        boolean gotExpectedException = false;

        try {
            pythonEngine.eval("__file__");
        } catch (ScriptException var6) {
            assertTrue(var6.getMessage().startsWith("NameError: "));
            gotExpectedException = true;
        }

        if (!gotExpectedException) {
            fail("Excepted __file__ to be undefined");
        }

        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("foo", "bar"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testSyntaxError() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");

        try {
            pythonEngine.eval("5q");
        } catch (ScriptException var4) {
            assertEquals(var4.getColumnNumber(), 1);
            assertEquals(var4.getLineNumber(), 1);
            assertTrue(var4.getMessage().startsWith("SyntaxError: "));
            return;
        }

        //assertTrue("Expected a ScriptException", false);
    }

    @Test
    public void testPythonException() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");

        try {
            pythonEngine.eval("pass\ndel undefined");
        } catch (ScriptException var4) {
            assertEquals(var4.getLineNumber(), 2);
            assertTrue(var4.getMessage().startsWith("NameError: "));
            return;
        }

        //assertTrue("Expected a ScriptException", false);
    }

    @Test
    public void testScriptFilename() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        SimpleScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute("javax.script.filename", "sample.py", 100);

        try {
            pythonEngine.eval("foo", scriptContext);
        } catch (ScriptException var5) {
            assertEquals("sample.py", var5.getFileName());
            return;
        }

        //assertTrue("Expected a ScriptException", false);
    }

    @Test
    public void testCompileEvalString() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        context.setAttribute("javax.script.filename", "sample.py", 100);
        CompiledScript five = ((Compilable)pythonEngine).compile("5");
        assertEquals(5, five.eval());
        assertEquals("sample.py", pythonEngine.eval("__file__"));
        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("sample.py"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testEvalReader() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        context.setAttribute("javax.script.filename", "sample.py", 100);
        assertNull(pythonEngine.eval(new StringReader("x = 5")));
        assertEquals(5, pythonEngine.eval(new StringReader("x")));
        assertEquals("sample.py", pythonEngine.eval("__file__"));
        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("sample.py"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testCompileEvalReader() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        ScriptContext context = pythonEngine.getContext();
        context.setAttribute("javax.script.filename", "sample.py", 100);
        CompiledScript five = ((Compilable)pythonEngine).compile(new StringReader("5"));
        assertEquals(5, five.eval());
        assertEquals("sample.py", pythonEngine.eval("__file__"));
        pythonEngine.eval("import sys");
        assertEquals(Arrays.asList("sample.py"), pythonEngine.eval("sys.argv"));
    }

    @Test
    public void testBindings() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.put("a", 42);
        assertEquals(42, pythonEngine.eval("a"));
        assertNull(pythonEngine.eval("x = 5"));
        assertEquals(5, pythonEngine.get("x"));
        assertNull(pythonEngine.eval("del x"));
        assertNull(pythonEngine.get("x"));
    }

    @Test
    public void testThreadLocalBindings() throws ScriptException, InterruptedException {
//        ScriptEngineManager manager = new ScriptEngineManager();
//        ScriptEngine pythonEngine = manager.getEngineByName("python");
//        pythonEngine.put("a", 42);
//        pythonEngine.put("x", 15);
//        ScriptEngineTest.ThreadLocalBindingsTest test = new ScriptEngineTest.ThreadLocalBindingsTest(pythonEngine, -7);
//        test.run();
//        assertNull(test.exception);
//        assertEquals(-7, test.x);
//        assertEquals("__builtin__", test.name);
//        ScriptEngineTest.ThreadLocalBindingsTest test2 = new ScriptEngineTest.ThreadLocalBindingsTest(pythonEngine, -22);
//        Thread thread = new Thread(test2);
//        thread.start();
//        thread.join();
//        assertNull(test2.exception);
//        assertEquals(-22, test2.x);
//        assertEquals("__builtin__", test2.name);
//        assertEquals(15, pythonEngine.get("x"));
//        assertNull(pythonEngine.eval("del x"));
//        assertNull(pythonEngine.get("x"));
//        assertEquals("__main__", pythonEngine.eval("__name__"));
    }

    @Test
    public void testInvoke() throws ScriptException, NoSuchMethodException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        Invocable invocableEngine = (Invocable)pythonEngine;
        assertNull(pythonEngine.eval("def f(x): return abs(x)"));
        assertEquals(5, invocableEngine.invokeFunction("f", new Object[]{-5}));
        assertEquals("spam", invocableEngine.invokeMethod(new PyString("  spam  "), "strip", new Object[0]));
        assertEquals("spam", invocableEngine.invokeMethod("  spam  ", "strip", new Object[0]));
    }

    @Test
    public void testInvokeFunctionNoSuchMethod() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        Invocable invocableEngine = (Invocable)manager.getEngineByName("python");

        try {
            invocableEngine.invokeFunction("undefined", new Object[0]);
        } catch (NoSuchMethodException var4) {
            return;
        }

        //assertTrue("Expected a NoSuchMethodException", false);
    }

    @Test
    public void testInvokeMethodNoSuchMethod() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        Invocable invocableEngine = (Invocable)manager.getEngineByName("python");

        try {
            invocableEngine.invokeMethod("eggs", "undefined", new Object[0]);
            fail("Expected a NoSuchMethodException");
        } catch (NoSuchMethodException var4) {
            assertEquals("undefined", var4.getMessage());
        }

    }

    @Test
    public void testGetInterface() throws ScriptException, IOException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        Invocable invocableEngine = (Invocable)pythonEngine;
        assertNull(pythonEngine.eval("def read(cb): return 1"));
        Readable readable = (Readable)invocableEngine.getInterface(Readable.class);
        assertEquals(1, readable.read((CharBuffer)null));
        assertNull(pythonEngine.eval("class C(object):\n    def read(self, cb): return 2\nc = C()"));
        readable = (Readable)invocableEngine.getInterface(pythonEngine.get("c"), Readable.class);
        assertEquals(2, readable.read((CharBuffer)null));
    }

    @Test
    public void testInvokeMethodNoSuchArgs() throws ScriptException, NoSuchMethodException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        Invocable invocableEngine = (Invocable)pythonEngine;
        Object newStringCapitalize = invocableEngine.invokeMethod("test", "capitalize", new Object[0]);
        assertEquals(newStringCapitalize, "Test");
    }

    @Test
    public void testPdb() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        String pdbString = "from pdb import set_trace; set_trace()";

        try {
            pythonEngine.eval(pdbString);
            fail("bdb.BdbQuit expected");
        } catch (ScriptException var5) {
            assertTrue(var5.getMessage().startsWith("bdb.BdbQuit"));
        }

    }

    @Test
    public void testScope_repr() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("a = 4");
        pythonEngine.eval("b = 'hi'");
        String repr = (String)pythonEngine.eval("repr(locals())");
//        Assert.assertTrue(repr.contains("'a': 4"));
//        Assert.assertTrue(repr.contains("'b': u'hi'"));
    }

    @Test
    public void testScope_iter() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("a = 4");
        pythonEngine.eval("b = 'hi'");
        PyList locals = (PyList)pythonEngine.eval("sorted((item for item in locals()))");
//        Assert.assertTrue(locals.contains("a"));
//        Assert.assertTrue(locals.contains("b"));
//        Assert.assertTrue(locals.contains("__name__"));
    }

    @Test
    public void testScope_lookup() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("a = 4");
        pythonEngine.eval("b = 'hi'");
        pythonEngine.eval("var_a = locals()['a']");
        pythonEngine.eval("arepr = repr(var_a)");
        assertEquals("4", pythonEngine.get("arepr"));
    }

    @Test
    public void testIssue1681() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("from org.python.jsr223 import PythonCallable\nclass MyPythonCallable(PythonCallable):\n    def getAString(self): return 'a string'\n\nresult = MyPythonCallable().getAString()\ntest = MyPythonCallable()\nresult2 = test.getAString()");
        assertEquals("a string", pythonEngine.get("result"));
        assertEquals("a string", pythonEngine.get("result2"));
    }

    @Test
    public void testIssue1698() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("import warnings");
        pythonEngine.eval("warnings.warn('test')");
    }

    @Test
    public void testSiteImportedByDefault() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("import sys");
        pythonEngine.eval("'site' in sys.modules");
    }

    @Test
    public void testSiteCanBeNotImported() throws ScriptException {
        try {
            Options.importSite = false;
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine pythonEngine = manager.getEngineByName("python");
            pythonEngine.eval("import sys");
            pythonEngine.eval("'site' not in sys.modules");
        } finally {
            Options.importSite = true;
        }

    }

    @Test
    public void testIssue2090() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine pythonEngine = manager.getEngineByName("python");
        pythonEngine.eval("a = 10L\nb = a-1");
        Object r = pythonEngine.get("b");
        assertEquals(new BigInteger("9"), r);
    }

    static class ThreadLocalBindingsTest implements Runnable {
        ScriptEngine engine;
        int value;
        Object x;
        Object name;
        Throwable exception;
        static final String script = String.join("\n", "try:", "    a", "except NameError:", "   pass", "else:", "   raise Exception('a is defined', a)");

        public ThreadLocalBindingsTest(ScriptEngine engine, int value) {
            this.engine = engine;
            this.value = value;
        }

        public void run() {
            try {
                Bindings bindings = this.engine.createBindings();
                this.engine.eval(script, bindings);
                //junit.framework.Assert.assertNull(this.engine.eval(script, bindings));
                bindings.put("x", this.value);
                this.x = this.engine.eval("x", bindings);
                this.name = this.engine.eval("__name__", bindings);
            } catch (Throwable var2) {
                var2.printStackTrace();
                this.exception = var2;
            }

        }
    }
}
