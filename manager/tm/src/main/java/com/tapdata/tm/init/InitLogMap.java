package com.tapdata.tm.init;

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
        log.info("Tapdata tm startup complete!");
    }
}
