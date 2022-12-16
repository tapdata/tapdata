package io.tapdata.quickapi.api.comom;

//import io.tapdata.quickapi.core.annotation.ApiType;
//import io.tapdata.quickapi.core.emun.SupportApi;
//import io.tapdata.api.invoker.PostManAPIInvoker;
//import org.reflections.Reflections;

import java.util.Map;

public interface APIDocument<V> {
//    public static PostManAPIInvoker analysisDefault(String apiJson){
//        return APIDocument.analysisByType(SupportApi.POST_MAN.apiName(),apiJson);
//    }
//    public static <V>V analysisByType(String apiType,String apiJson){
//        Reflections reflections = new Reflections("io.tapdata.api");
//        //返回带有指定注解的所有类对象
//        Set<Class<?>> typesAnnotatedWithApiType = reflections.getTypesAnnotatedWith(ApiType.class);
//        Map<String,Class<?>> classMap = typesAnnotatedWithApiType.stream().filter(cls->{
//            try {
//                ApiType api = cls.getAnnotation(ApiType.class);
//                return  null != api;
//            }catch (Exception e){
//                return false;
//            }
//        }).collect(Collectors.toMap(aClass -> {
//            ApiType api = aClass.getAnnotation(ApiType.class);
//            return api.type().apiName();
//        },cls->cls,(c1,c2)->c2));
//        Class<?> apiTypeCls = classMap.get(apiType);
//        if (null != apiTypeCls) {
//            try {
//                APIDocument document = (APIDocument)apiTypeCls.newInstance();
//                return (V)document.analysis(apiJson);
//            } catch (Exception ignored) { }
//        }
//        return null;
//    }
//    public default V analysis(String apiJson,String type){
//       return APIDocument.analysisByType(type,apiJson);
//    }
    public V analysis(String apiJson,String type, Map<String, Object> params);
}
