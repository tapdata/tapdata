package io.tapdata.pdk.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author GavinXiao
 * @description PyInstall create by Gavin
 * @create 2023/6/20 16:39
 **/
public class PyInstall {
    public static void main(String[] args) {
        List<String> postList = new ArrayList<>();
        Collections.addAll(
            postList,
            "py-install"
//            ,"-s","D:\\GavinData\\kitSpace\\tapdata\\plugin-kit\\tapdata-modules\\script-engines-module\\target\\script-engine-module-1.0-SNAPSHOT.jar"
            ,"-j","jython-standalone-2.7.2.jar"
            ,"-p","/Users/xiao/kit/maven/tapdata-daas-py/repo/io/tapdata/jython-standalone/2.7.2/"
            , "-uz", "/Users/xiao/kit/idea/tapdata-py/py-lib"
//            ,"-g","D:\\GavinData\\kitSpace\\tapdata\\plugin-kit\\tapdata-modules\\script-engines-module\\src\\main\\resources\\pip-packages\\"
        );





        Main.registerCommands().execute(postList.toArray(new String[0]));
    }
}
