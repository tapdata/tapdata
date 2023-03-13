package io.tapdata.supervisor.convert.entity;

import cn.hutool.json.JSONUtil;
import io.tapdata.supervisor.convert.wave.JavassistHandle;
import io.tapdata.supervisor.convert.wave.JavassistWaver;
import io.tapdata.supervisor.utils.JavassistTag;
import javassist.CannotCompileException;
import javassist.NotFoundException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Parser {
    private final List<Waver> waver;

    private Parser(Map<String, Object> parserMap) {
        this.waver = new ArrayList<>();
        Optional.ofNullable(parserMap).ifPresent(map -> {
            Object calMapObj = map.get(WZTags.W_CLASS);
            if (calMapObj instanceof Collection) {
                List<?> collection = (List<?>) calMapObj;
                for (Object item : collection) {
                    if (Objects.nonNull(item) && item instanceof Map) {
                        Optional.ofNullable(Waver.waver().parser((Map<String, Object>) item)).ifPresent(this.waver::add);
                    }
                }
            } else if (calMapObj instanceof Map) {
                Optional.ofNullable(Waver.waver().parser(map)).ifPresent(this.waver::add);
            }
        });
    }

    public static Parser parser(Map<String, Object> parserMap) {
        return new Parser(parserMap);
    }
    public static Parser parser() {
        Map<String,Object> map = JSONUtil.readJSONObject(new File("plugin-kit/tapdata-modules/class-supervisor/src/main/resources/supervisor.json"), StandardCharsets.UTF_8);
        return new Parser(map);
    }

    public void wave() throws NotFoundException, CannotCompileException {
        JavassistWaver javaSsistWaver = JavassistWaver.create();
        for (Waver value : this.waver) {
            List<JavassistWaver.Builder> builders = new ArrayList<>();
            for (int jndex = JavassistTag.ZERO; jndex < value.targets.size(); jndex++) {
                WBaseTarget wBaseTarget = value.targets.get(jndex);
                String[] paths = wBaseTarget.paths();
                for (String path : paths) {
                    JavassistWaver.Builder builder = javaSsistWaver.builder(path, wBaseTarget.getSaveTo());

                    builders.add(builder);
                }
            }

            Map<String, WCodeAgent<JavassistHandle>> agentMap = new HashMap<>();
            this.waveConstructor(builders, value.getConstructors(), agentMap);
            this.waveMethod(builders, value.getMethods(), agentMap);

            for (JavassistWaver.Builder builder : builders) {
                try {
                    builder.build();
                } catch (IOException | CannotCompileException | NotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void waveMethod(List<JavassistWaver.Builder> builders, List<WBaseMethod> methods, Map<String, WCodeAgent<JavassistHandle>> agentMap) throws NotFoundException, CannotCompileException {
        for (WBaseMethod method : methods) {
            List<String> args = method.getArgs();
            List<WCode> codes = method.getCodes();
            String returnType = method.getReturnType();
            String name = method.getName();
            String createWith = method.getCreateWith();
            boolean needCreate = method.isNeedCreate();//@TODO create one
            for (JavassistWaver.Builder builder : builders) {
                JavassistHandle handle = JavassistHandle.handle(builder.method(name, args, returnType, needCreate, createWith));
                if (Objects.nonNull(handle)) {
                    for (WCode code : codes) {
                        WCodeAgent<JavassistHandle> agent = agentMap.computeIfAbsent(code.getType(), key -> this.create(code));
                        agent.agent(handle);
                    }
                }
            }
        }
    }

    private void waveConstructor(List<JavassistWaver.Builder> builders, List<WBaseConstructor> constructors, Map<String, WCodeAgent<JavassistHandle>> agentMap) throws NotFoundException, CannotCompileException {
        for (WBaseConstructor constructor : constructors) {
            List<String> args = constructor.getArgs();
            List<WCode> codes = constructor.getCodes();
            boolean scanAll = constructor.isScanAllConstructor();
            boolean needCreate = constructor.isNeedCreate();//@TODO create one
            for (JavassistWaver.Builder builder : builders) {
                List<JavassistHandle> handles = new ArrayList<>();
                if (scanAll) {
                    handles.addAll(JavassistHandle.handles(builder.constructorAll()));
                } else {
                    handles.add(JavassistHandle.handle(builder.constructor(args, null)));
                }
                for (JavassistHandle handle : handles) {
                    if (Objects.nonNull(handle)) {
                        for (WCode code : codes) {
                            WCodeAgent<JavassistHandle> agent = agentMap.computeIfAbsent(code.getType(), key -> this.create(code));
                            agent.agent(handle);
                        }
                    }
                }
            }
        }
    }


    private WCodeAgent<JavassistHandle> create(WCode code) {
        String type = code.getType();
        switch (type) {
            case WZTags.CODE_LINE_TYPE_AFTER:
                return (h) -> {
                    WCodeAfter codeAfter = (WCodeAfter) code;
                    h.appendAfter(codeAfter.isFinallyIs(), codeAfter.isRedundantIs(), codeAfter.getLine());
                };
            case WZTags.CODE_LINE_TYPE_NORMAL:
                return (h) -> {
                    WCodeNormal codeNormal = (WCodeNormal) code;
                    h.appendAt(codeNormal.getIndex(), codeNormal.isAppendIs(), codeNormal.getLine());
                };
            case WZTags.CODE_LINE_TYPE_CATCH:
                return (h) -> {
                    WCodeCatch codeCatch = (WCodeCatch) code;
                    WException exception = codeCatch.getException();
                    h.appendCatch(exception.getPath(), exception.getName(), codeCatch.getLine());
                };
            default:
                return (h) -> {
                    h.appendBefore(code.getLine());
                };
        }
    }

}
