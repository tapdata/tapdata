package com.tapdata.generator;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class GeneratorFactory {


    private final static Map<String, Generator> registerGeneratorMap = new ConcurrentHashMap<>();

    static {
        ServiceLoader<Generator> load = ServiceLoader.load(Generator.class);
        for (Generator myServiceLoader : load){
            register(myServiceLoader);
        }
    }


    public static Generator getGenerator(String type) {
        return registerGeneratorMap.get(type);
    }

    private static void register(Generator generator) {
        registerGeneratorMap.put(generator.getType(), generator);
    }
}
