package com.tapdata.tm.init;


import com.tapdata.tm.utils.OEMReplaceUtil;
import com.tapdata.tm.utils.TMStartMsgUtil;
import com.tapdata.tm.utils.TmStartMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;


import java.util.HashMap;
import java.util.Map;

@Slf4j
public class InitLogMap {
    private static final Map<String, Boolean> initLogMap = new HashMap<>();

    public static void register(Class<? extends ApplicationRunner> clazz) {
        initLogMap.put(clazz.getName(), false);
    }

    public static synchronized void complete(Class<? extends ApplicationRunner> clazz) {
        initLogMap.put(clazz.getName(), true);

        for (Map.Entry<String, Boolean> entry : initLogMap.entrySet()) {
            if (!entry.getValue()) {
                return;
            }
        }
        TmStartMsg tmStartMsg = new TmStartMsg("ok","TM启动成功");
        TMStartMsgUtil.writeTMStartMsg(tmStartMsg);
        String oemType = OEMReplaceUtil.oemType();
        if (null == oemType) {
            log.info("\n" +
                    "  _______              _       _           _                    _             _                                _      _           _ \n" +
                    " |__   __|            | |     | |         | |                  | |           | |                              | |    | |         | |\n" +
                    "    | | __ _ _ __   __| | __ _| |_ __ _   | |_ _ __ ___     ___| |_ __ _ _ __| |_     ___ ___  _ __ ___  _ __ | | ___| |_ ___  __| |\n" +
                    "    | |/ _` | '_ \\ / _` |/ _` | __/ _` |  | __| '_ ` _ \\   / __| __/ _` | '__| __|   / __/ _ \\| '_ ` _ \\| '_ \\| |/ _ \\ __/ _ \\/ _` |\n" +
                    "    | | (_| | |_) | (_| | (_| | || (_| |  | |_| | | | | |  \\__ \\ || (_| | |  | |_   | (_| (_) | | | | | | |_) | |  __/ ||  __/ (_| |\n" +
                    "    |_|\\__,_| .__/ \\__,_|\\__,_|\\__\\__,_|   \\__|_| |_| |_|  |___/\\__\\__,_|_|   \\__|   \\___\\___/|_| |_| |_| .__/|_|\\___|\\__\\___|\\__,_|\n" +
                    "            | |                                                                                         | |                         \n" +
                    "            |_|                                                                                         |_|                         ");
        } else {
            log.info("\n"+
                    " _                    _             _                                _      _           _ \n" +
                    "| |                  | |           | |                              | |    | |         | |\n" +
                    "| |_ _ __ ___     ___| |_ __ _ _ __| |_     ___ ___  _ __ ___  _ __ | | ___| |_ ___  __| |\n" +
                    "| __| '_ ` _ \\   / __| __/ _` | '__| __|   / __/ _ \\| '_ ` _ \\| '_ \\| |/ _ \\ __/ _ \\/ _` |\n" +
                    "| |_| | | | | |  \\__ \\ || (_| | |  | |_   | (_| (_) | | | | | | |_) | |  __/ ||  __/ (_| |\n" +
                    " \\__|_| |_| |_|  |___/\\__\\__,_|_|   \\__|   \\___\\___/|_| |_| |_| .__/|_|\\___|\\__\\___|\\__,_|\n" +
                    "                                                              | |                         \n" +
                    "                                                              |_|                         ");
        }
    }
}
