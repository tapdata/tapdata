package com.tapdata.tm;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class Unit4Util {
    /**
     * mock @Slf4j的log属性
     *
     * */
    public static void mockSlf4jLog(Object mockTo, Logger log) {
        try {
            Field logF = mockTo.getClass().getDeclaredField("log");
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(Field.class, MethodHandles.lookup());
            VarHandle modifiersVarHandle = lookup.findVarHandle(Field.class, "modifiers", int.class);
            modifiersVarHandle.set(logF, logF.getModifiers() & ~Modifier.FINAL);
            logF.setAccessible(true);
            logF.set(mockTo, log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
