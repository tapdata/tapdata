package io.tapdata.js.connector.iengine;

public class EngineEvalResources {
    public static final String SCANNING_CAPABILITIES_RESOURCE  = "connectors-common/js-connector-core/src/main/java/io/tapdata/js/utils/js/scanning_capability.js";


    public static final String js = "var fun = [];\n" +
            "function testA(){}\n" +
            "function testB(){}\n" +
            "function _scanning_capabilities_in_java_script() {\n" +
            "    var key_name_dif = [];\n" +
            "    key_name_dif = Object.keys(new Object(this));\n" +
            "    tapFunctions = [];\n" +
            "    for (var i = 0; i < key_name_dif.length ; i++ ) {\n" +
            "        if (typeof this[key_name_dif[i]] == 'function'){\n" +
            "            fun.push(key_name_dif[i]);tapFunctions.push(key_name_dif[i]);\n" +
            "        }\n" +
            "    }\n" +
            "    return fun;\n" +
            "}\n" +
            "_scanning_capabilities_in_java_script();";
}
