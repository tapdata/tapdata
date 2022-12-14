package io.tapdata.quickapi.core.emun;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public enum TapApiTag {
    TAP_TABLE("TAP_TABLE",".*(TAP_TABLE\\[[^\\]]+).*",""),
    TAP_LOGIN("TAP_LOGIN","",""),

    PAGE_SIZE_PAGE_INDEX("PAGE_SIZE_PAGE_INDEX",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_SIZE_PAGE_INDEX)).*",""),
    FROM_TO("FROM_TO",".*(TAP_TABLE\\[[^\\]]+\\(FROM_TO)).*",""),
    PAGE_LIMIT("PAGE_LIMIT",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_LIMIT)).*",""),
    PAGE_TOKEN("PAGE_TOKEN",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_TOKEN)).*",""),
    PG_NONE("PG_NONE",".*(TAP_TABLE\\[[^\\]]+\\(PG_NONE)).*",""),

    TAP_GET_TOKEN("TAP_GET_TOKEN","",""),
    TAP_TABLE_COUNT("TAP_TABLE_COUNT","",""),
    ;

    String tagName;
    String tagDescription;
    String tagRegex;

    TapApiTag(String tagName, String tagRegex, String tagDescription){
        this.tagDescription = tagDescription;
        this.tagName = tagName;
        this.tagRegex = tagRegex;
    }
    public String tagRegex(){
        return this.tagRegex;
    }
    public String tagName(){
        return this.tagName;
    }
    public String description(){
        return this.tagDescription;
    }
    public static boolean isLabeled(String apiName){
        return !labels(apiName).isEmpty();
    }
    public static List<TapApiTag> labels(String apiName){
        List<TapApiTag> labeled = new ArrayList<>();
        if (null == apiName || "".equals(apiName)) return labeled;
        TapApiTag[] tags = values();
        for (TapApiTag tag : tags) {
            if (apiName.contains(tag.tagName) && apiName.matches(tag.tagRegex)){
                labeled.add(tag);
            }
        }
        return labeled;
    }
    public static boolean hasPageStage(String apiName){
        return Objects.nonNull(getPageStage(apiName));
    }
    public static String getPageStage(String apiName){
        String suf = TAP_TABLE.tagName + "[";
        String per = "]";
        if (Objects.nonNull(apiName) && apiName.startsWith(suf)){
            int start = apiName.indexOf(suf);
            start += suf.length();
            int end = apiName.indexOf(per,start);
            if (end <= start){
                return null;
            }
            start = end + per.length();
            String pageSuf = "(";
            String pagePer = ")";
            int pageStart = apiName.indexOf(pageSuf,start);
            if (pageStart < start){
                return null;
            }
            pageStart += 1;
            int pageEnd = apiName.indexOf(pagePer,pageStart);
            if (pageEnd <= pageStart) {
                return null;
            }
            String pageStage = apiName.substring(pageStart, pageEnd);
            return Objects.equals(PAGE_SIZE_PAGE_INDEX.tagName,pageStage)
                    || Objects.equals(FROM_TO.tagName,pageStage)
                    || Objects.equals(PAGE_LIMIT.tagName,pageStage)
                    || Objects.equals(PAGE_TOKEN.tagName,pageStage)
                    || Objects.equals(PG_NONE.tagName,pageStage)
                    ? pageStage : null;
        }
        return null;
    }

    public static boolean isTableName(String apiName){
        return Objects.nonNull(analysisTableName(apiName));
    }
    public static String analysisTableName(String apiName){
//        if (Objects.nonNull(apiName) && apiName.matches(TapApiTag.TAP_TABLE.tagRegex())){
        String suf = TAP_TABLE.tagName + "[";
        if (Objects.nonNull(apiName) && apiName.startsWith(suf)){
            int start = apiName.indexOf(suf);
            start += suf.length();
            String per = "]";
            int end = apiName.indexOf(per,start);
            if (end <= start){
                return null;
            }
            return apiName.substring(start,end);
        }
        return null;
    }

    public static boolean isTokenApi(String apiName){
        return (Objects.nonNull(apiName) && apiName.startsWith(TAP_GET_TOKEN.tagName));
    }

    public static void main(String[] args) {

        System.out.println(analysisTableName("TAP_TABLE[sss"));
        System.out.println(analysisTableName("TAP_TABLE[sss]"));
        System.out.println(analysisTableName("TAP_TABLE[]"));
        System.out.println(analysisTableName("sssTAP_TABLE[sss"));
        System.out.println(analysisTableName("sssTAP_TABLE[sss]"));
        System.out.println(analysisTableName("sssTAP_TABLE[sss]eee"));
        System.out.println(analysisTableName("TAP_TABLE[sss]eee"));
        System.out.println(analysisTableName("TAP_TABLEddd[sss]eee"));


        System.out.println(getPageStage("TAP_TABLE[sss(PAGE_SIZE_PAGE_INDEX)"));
        System.out.println(getPageStage("TAP_TABLE[sss]()"));
        System.out.println(getPageStage("TAP_TABLE[sss](SSS)"));
        System.out.println(getPageStage("TAP_TABLE[sss](PAGE_SIZE_PAGE_INDEX)"));
    }
}
