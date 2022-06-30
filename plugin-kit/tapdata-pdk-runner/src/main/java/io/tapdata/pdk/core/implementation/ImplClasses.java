package io.tapdata.pdk.core.implementation;

import java.util.Map;

public class ImplClasses {
    private Map<String, ImplClass> typeClassHolderMap;

    public Map<String, ImplClass> getTypeClassHolderMap() {
        return typeClassHolderMap;
    }

    public void setTypeClassHolderMap(Map<String, ImplClass> typeClassHolderMap) {
        this.typeClassHolderMap = typeClassHolderMap;
    }

    public Class<?> anyImplementationClass() {
        if(typeClassHolderMap != null && !typeClassHolderMap.isEmpty()) {
            ImplClass implClass = typeClassHolderMap.values().stream().findFirst().get();
            return implClass.getClazz();
        }
        return null;
    }

    public ImplClass getImplementationClass(String type) {
        if(typeClassHolderMap == null)
            return null;
        return typeClassHolderMap.get(type);
    }
}
