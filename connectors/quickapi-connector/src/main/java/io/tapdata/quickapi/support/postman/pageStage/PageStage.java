package io.tapdata.quickapi.support.postman.pageStage;

import java.util.*;

public interface PageStage {
    public static String stagePackageName(String pageTag){
        char[] chars = pageTag.toCharArray();
        StringBuilder builder = new StringBuilder(PageStage.class.getPackage().getName());
        builder.append(".").append((""+((char)chars[0])).toUpperCase(Locale.ROOT));
        for (int i = 1; i < chars.length; i++) {
            char aChar = chars[i];
            if(aChar == '_' && i+1 >= chars.length){
                break;
            }
            builder.append((""+(aChar)).toLowerCase(Locale.ROOT));
            if(i+1 >= chars.length) break;
            if (chars.length <= i+2 && '_' == chars[i+1]){
                break;
            }
            if (chars.length > i+1 && '_' == chars[i+1]){
                builder.append((""+((char)chars[i=i+2])).toUpperCase(Locale.ROOT));
            }
        }
        return builder.toString();
    }
    public static PageStage stage(String pageTag){
        if (Objects.isNull(pageTag)) return null;
        try {
            Class<? extends PageStage> pageStage = (Class<? extends PageStage>) Class.forName(PageStage.stagePackageName(pageTag));
            return pageStage.newInstance();
        }catch (Exception e){

        }
        return null;
    }
    public void page(TapPage tapPage);

}
