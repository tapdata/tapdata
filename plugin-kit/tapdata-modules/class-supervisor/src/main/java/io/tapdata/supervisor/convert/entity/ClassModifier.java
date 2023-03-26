package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.convert.wave.JavassistHandle;
import io.tapdata.supervisor.convert.wave.JavassistWaver;
import io.tapdata.supervisor.utils.ClassUtil;
import io.tapdata.supervisor.utils.JavassistTag;
import javassist.CannotCompileException;
import javassist.NotFoundException;

import java.io.IOException;
import java.util.*;

/**
 * Class modify by javassist
 * @author 2749984520@qq.com Gavin
 * @time 2023/03/24
 * */
public class ClassModifier {
    private final List<Waver> waver;
    private String outputFilePath = "plugin-kit/tapdata-modules/class-supervisor/src/main";
    private String jarFilePath = "plugin-kit/tapdata-modules/class-supervisor/src/main/resources/supervisor.json";
    public static final String SUPERVISOR_CONFIG_PATH = "supervisor.json";
    ClassUtil classUtil;

    private ClassModifier(String jarFilePath, String outputPath) {
        this.jarFilePath = Objects.isNull(jarFilePath) ? this.jarFilePath : jarFilePath;
        this.outputFilePath = Objects.isNull(outputPath) ? this.outputFilePath : outputPath;
        this.waver = new ArrayList<>();
        this.classUtil = new ClassUtil(jarFilePath);
        List<Map<String, Object>> jsonSource = ClassUtil.jsonSource(this.jarFilePath, ClassModifier.SUPERVISOR_CONFIG_PATH);
        if (!jsonSource.isEmpty()) {
            for (Map<String, Object> parserMap : jsonSource) {
                Optional.ofNullable(parserMap).ifPresent(map -> {
                    Object calMapObj = map.get(WZTags.W_CLASS);
                    if (calMapObj instanceof Collection) {
                        List<?> collection = (List<?>) calMapObj;
                        for (Object item : collection) {
                            if (Objects.nonNull(item) && item instanceof Map) {
                                Map<String, Object> objectMap = (Map<String, Object>) item;
                                objectMap.put(WZTags.W_SAVE_TO, this.outputFilePath);
                                objectMap.put(WZTags.W_JAR_FILE_PATH, this.jarFilePath);
                                Optional.ofNullable(Waver.waver().classUtil(this.classUtil).parser(objectMap)).ifPresent(this.waver::add);
                            }
                        }
                    } else if (calMapObj instanceof Map) {
                        Optional.ofNullable(Waver.waver().parser(map)).ifPresent(this.waver::add);
                    }
                });
                break;
            }
        }
    }

    /**
     *
     */
    public static ClassModifier load(String jarFilePath, String outputPath) {
        return new ClassModifier(jarFilePath, outputPath);
    }

    /**
     *
     */
    public static ClassModifier load(String outputPath) {
        return new ClassModifier(null, outputPath);
    }

    /**
     * @deprecated
     */
    public static ClassModifier load() {
        //Map<String, Object> map = JSONUtil.readJSONObject(new File(this.jarFilePath), StandardCharsets.UTF_8);
        return new ClassModifier(null, null);
    }

    public void wave() throws NotFoundException, CannotCompileException {
        JavassistWaver javaSsistWaver = JavassistWaver.create(classUtil.getDependencyURLClassLoader());
        for (Waver value : this.waver) {
            List<JavassistWaver.Builder> builders = new ArrayList<>();
            //for filter repetitive class by class path name
            Set<String> pathCache = new HashSet<>();
            for (int jndex = JavassistTag.ZERO; jndex < value.targets.size(); jndex++) {
                WBaseTarget wBaseTarget = value.targets.get(jndex);
                String[] paths = wBaseTarget.paths();
                for (String path : paths) {
                    if (pathCache.contains(path)){
                        continue;
                    }
                    JavassistWaver.Builder builder = javaSsistWaver.builder(path, wBaseTarget.getSaveTo());
                    if (Objects.nonNull(builder)){
                        builders.add(builder);
                        pathCache.add(path);
                    }
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
