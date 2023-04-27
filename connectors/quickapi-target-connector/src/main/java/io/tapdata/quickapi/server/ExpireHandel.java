package io.tapdata.quickapi.server;

import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.entity.error.CoreException;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.postman.util.ApiMapUtil;

import java.util.*;

public class ExpireHandel {
    APIResponse response;
    String expireStatus;
    String tokenName;
    public static ExpireHandel create(APIResponse response,String expireStatus,String tokenName){
        ExpireHandel expireHandel = new ExpireHandel();
        return expireHandel.builderExpireStatus(expireStatus).builderHttpResponse(response).builderTokenName(tokenName);
    }
    private ExpireHandel(){
        super();
    }

    public ExpireHandel builderHttpResponse(APIResponse response){
        this.response = response;
        return this;
    }
    public ExpireHandel builderExpireStatus(String expireStatus){
        this.expireStatus = expireStatus;
        return this;
    }
    public ExpireHandel builderTokenName(String tokenName){
        this.tokenName = tokenName;
        return this;
    }
    public static final String REGEX_MATCH = "(regex()(.*?)())";//".*(regex([^\\])+).*";
    public static final String RANGE_MATCH = "(\\[)(.*?)(])";
    public boolean builder(){
        if(Objects.isNull(this.expireStatus)) return false;
        String[] lines =  expireStatus.split("\\\n");
        if (lines.length>0){
            boolean result = false;
            for (String line : lines) {
                String[] split = line.split("&&");
                if (split.length>0){
                    boolean equalResult = false;
                    for (String equalItem : split) {
                        String[] splitItem = equalItem.split("=");
                        equalResult = this.equalsOne(splitItem);
                        if(!equalResult) {
                            break;
                        }
                    }
                    result = equalResult;
                }else {
                    result = false;
                }
                if (result) return true;
            }
            return false;
        }else {
            return false;
        }
    }
    private boolean equalsOne(String [] split){
        if (split.length == 2) {
            String key = split[0];
            if (key.startsWith("header.")){
                key = key.substring(7);
                return headerEquals(key,split[1]);
            }else if("code".equals(key)){
                return codeEquals(key,split[1]);
            }else if(key.startsWith("body.")){
                key = key.substring(5);
                return bodyEquals(Api.PAGE_RESULT_PATH_DEFAULT_PATH + "."+key,split[1]);
            }else {
                return bodyEquals(key,split[1]);
            }
        }else {
            return false;
        }
    }
    private boolean bodyEquals(String key, String val){
        Map<String, Object> body = this.response.result();
        Object keyFromMap = ApiMapUtil.getKeyFromMap(body, key);
        return this.equal(val,String.valueOf(keyFromMap));//Objects.equals(keyFromMap,val);
    }
    private boolean headerEquals(String key, String val){
        Map<String, Object> header = this.response.headers();
        Object keyFromMap = ApiMapUtil.getKeyFromMap(header, key);
        return this.equal(val,String.valueOf(keyFromMap));
    }
    private boolean codeEquals(String key, String val){
        Integer code = this.response.httpCode();
        return this.equal(val,String.valueOf(code));//Objects.equals(code,val);
    }

    private boolean equal(String before,String after){
        if (before.matches(REGEX_MATCH)){////判断是否通过正则表达式
            return this.regexMatchEquals(before,after);
        }else if(before.matches(RANGE_MATCH)){//判断是否范围值
            return this.rangeEquals(before,after);
        }else {//普通运算符
            return Objects.equals(before,""+after);
        }
    }
    private boolean rangeEquals(String before,String after){
        //[11]    >=11
        //[11,]   >=11
        //[,11]   <=11
        //[11,22] >=11 && <=22
        int indexOfSpilt = before.indexOf(",");
        String prefixStr = indexOfSpilt==1 ? "" : before.substring(1,indexOfSpilt<0?before.indexOf("]"):indexOfSpilt);
        int indexOfSuf = before.indexOf("]");
        String suffixStr = (indexOfSpilt + 1) == indexOfSuf || indexOfSpilt < 0 ? "" : before.substring(indexOfSpilt + 1,indexOfSuf);
        if( !"".equals(prefixStr) && !prefixStr.matches("-?[0-9]+.?[0-9]*")){
            throw new CoreException(String.format("The range value %s is set incorrectly. Please set a reasonable range",before));
        }
        if( !"".equals(suffixStr) && !suffixStr.matches("-?[0-9]+.?[0-9]*")){
            throw new CoreException(String.format("The range value %s is set incorrectly. Please set a reasonable range",before));
        }
        Double prefix = prefixStr.equals("")?Double.MIN_VALUE: Double.parseDouble(prefixStr);
        Double suffix = suffixStr.equals("")?Double.MAX_VALUE: Double.parseDouble(suffixStr);
        Double afterNumber = Double.valueOf(after);
        return afterNumber >= prefix && afterNumber <= suffix;
    }
    private boolean regexMatchEquals(String before,String after){
        //before = before.replace("regex('", "");
        try {
            String regex = before.substring(7,before.length()-2);
            return after.matches(regex);
        }catch (Exception e){
            throw new CoreException(String.format("Please ensure that the regular expression you entered is legal, and the wrong expression is %s.",before));
        }
    }

    private List<String> properties(){
        String[] propertiesArr = this.expireStatus.split("\\|\\||&&");
        Set<String> properties = new HashSet<>();
        properties.addAll(Arrays.asList(propertiesArr));
        return new ArrayList<String>(){{addAll(properties);}};
    }


    public boolean refreshComplete(APIResponse tokenResponse,Map<String,Object> apiParam){
        if(Objects.isNull(apiParam)) throw new CoreException("Api param context must not be null");
        if(Objects.isNull(tokenResponse) || Objects.isNull(tokenResponse.result())) throw new CoreException("Token Failed to obtain token. Please check the api description on API Document.");
        Map<String, Object> result = tokenResponse.result();
        Object value = null;
        if(Objects.nonNull(tokenName) && !"".equals(tokenName)) {
            String[] split = tokenName.split("=");
            if (split.length != 2){
                throw new CoreException(String.format("The wrong token rule description %s, please declare according to the correct rule, such as PostManVariableParam=HttpResponseParam. ",tokenName));
            }
            String keyNameToToken = split[0];
            String keyNameFromBody = split[1];
            value = ApiMapUtil.getKeyFromMap(result,keyNameFromBody);
            if (Objects.isNull(value)) {
                value = ApiMapUtil.depthSearchParamFromMap(result,keyNameFromBody);
            }
            if (Objects.nonNull(value)) {
                apiParam.put(keyNameToToken, value);
                return true;
            }
        }
        Map.Entry<String,Object> matchTokenEntry = this.fuzzyMatching(result);
        value = matchTokenEntry.getValue();
        List<String> list = new ArrayList<>();
        Object finalValue = value;
        apiParam.forEach((key, val)->{
            if ((""+key).equals(matchTokenEntry.getKey()) ||
                    String.valueOf(finalValue).length() == String.valueOf(val).length()){
                list.add(key);
            }
        });
        if (list.isEmpty()) {
            throw new CoreException("Not match any key, Unable to decide which to use as a valid value. Please manually set the global variable to which the obtained token is assigned on the connection page, such as PostManVariableParam=HttpResponseParam.");
        }

        if (list.size() == 1){
            apiParam.put(list.get(0), value);
            return true;
        }else {
            throw new CoreException("Too many matches found. Unable to decide which to use as a valid value. Please manually set the global variable to which the obtained token is assigned on the connection page, such as PostManVariableParam=HttpResponseParam.");
        }
    }

    //从获取TOKEN的HTTP返回结果中模糊找出token
    public static final String HEIGHT_MATCH = "access_token";
    public static final String MIDDLE_MATCH = "token";
    public static final String LOW_MATCH = "access";
    private Map.Entry<String, Object> fuzzyMatching(Map<String, Object> responseData){
        Object tokenValue = ApiMapUtil.depthSearchParamFromMap(responseData, HEIGHT_MATCH);
        if (Objects.nonNull(tokenValue)) return new AbstractMap.SimpleEntry<>(HEIGHT_MATCH,String.valueOf(tokenValue));
        List<Map.Entry<String,Object>> tokens = new ArrayList<>();
        ApiMapUtil.depthSearchParamFromMap(responseData, MIDDLE_MATCH,tokens);
        if (tokens.size() == 1){
            return tokens.get(0);
        }else if(tokens.size() > 1){
            throw new CoreException("Too many matches found in variable map. Unable to decide which to use as a valid value. Please manually set the global variable to which the obtained token is assigned on the connection page, such as PostManVariableParam=HttpResponseParam.");
        }else {
            ApiMapUtil.depthSearchParamFromMap(responseData, LOW_MATCH,tokens);
            if (tokens.size() == 1){
                return tokens.get(0);
            }else if(tokens.size() > 1){
                throw new CoreException("Too many matches found in variable map. Unable to decide which to use as a valid value. Please manually set the global variable to which the obtained token is assigned on the connection page, such as PostManVariableParam=HttpResponseParam.");
            }else {
                throw new CoreException("Not match any key in variable map., Unable to decide which to use as a valid value. Please manually set the global variable to which the obtained token is assigned on the connection page, such as PostManVariableParam=HttpResponseParam.");
            }
        }
    }

    //从全局变量中模糊找出token变量名称
    private String tokenNameFromVariable(Map<String,Object> apiParam){

        return "access_token";
    }

    private List<Map<String,Object>> widthTokenNameKeyAndValue(Map<String,Object> map){

        return null;
    }
}
