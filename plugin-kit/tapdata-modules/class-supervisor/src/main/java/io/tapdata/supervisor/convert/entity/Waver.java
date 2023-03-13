package io.tapdata.supervisor.convert.entity;

import java.util.*;

class Waver extends WBase implements Resolvable<Waver> {
    List<WBaseTarget> targets;
    List<WBaseMethod> methods;
    List<WBaseConstructor> constructors;

    public static Waver waver() {
        return new Waver();
    }

    @Override
    public Waver parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (!this.ignore){
            this.targets = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_TARGET);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseTarget().parser(objectMap))).ifPresent(this.targets::add);
                    }
                }
            }catch (Exception ignored){
            }

            this.methods = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_METHOD);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseMethod().parser(objectMap))).ifPresent(this.methods::add);
                    }
                }
            }catch (Exception ignored){
            }

            this.constructors = new ArrayList<>();
            try {
                List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_CONSTRUCTOR);
                for (Map<String, Object> objectMap : mapList) {
                    if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                        Optional.ofNullable((new WBaseConstructor().parser(objectMap))).ifPresent(this.constructors::add);
                    }
                }
            }catch (Exception ignored){
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
