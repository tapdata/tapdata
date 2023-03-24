package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.utils.ClassUtil;

import java.util.*;

/**
 * Class modify by javassist
 * @author 2749984520@qq.com Gavin
 * @time 2023/03/24
 * */
class Waver extends WBase implements Resolvable<Waver> {
    List<WBaseTarget> targets;
    List<WBaseMethod> methods;
    List<WBaseConstructor> constructors;
    ClassUtil classUtil;

    public ClassUtil getClassUtil() {
        return classUtil;
    }

    public void setClassUtil(ClassUtil classUtil) {
        this.classUtil = classUtil;
    }

    public Waver classUtil(ClassUtil classUtil) {
        this.classUtil = classUtil;
        return this;
    }

    public static Waver waver() {
        return new Waver();
    }

    @Override
    public Waver parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (!this.ignore) {
            this.targets = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_TARGET);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseTarget(
                                WZTags.toString(parserMap, WZTags.W_SAVE_TO, null),
                                WZTags.toString(parserMap, WZTags.W_JAR_FILE_PATH, null)
                        ).classUtil(this.classUtil).parser(objectMap))).ifPresent(this.targets::add);
                    }
                }
            } catch (Exception ignored) {
            }

            this.methods = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_METHOD);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseMethod().parser(objectMap))).ifPresent(this.methods::add);
                    }
                }
            } catch (Exception ignored) {
            }

            this.constructors = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_CONSTRUCTOR);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseConstructor().parser(objectMap))).ifPresent(this.constructors::add);
                    }
                }
            } catch (Exception ignored) {
            }
            return this;
        }
        return null;
    }

    public List<WBaseTarget> getTargets() {
        return targets;
    }

    public void setTargets(List<WBaseTarget> targets) {
        this.targets = targets;
    }

    public List<WBaseMethod> getMethods() {
        return methods;
    }

    public void setMethods(List<WBaseMethod> methods) {
        this.methods = methods;
    }

    public List<WBaseConstructor> getConstructors() {
        return constructors;
    }

    public void setConstructors(List<WBaseConstructor> constructors) {
        this.constructors = constructors;
    }
}
