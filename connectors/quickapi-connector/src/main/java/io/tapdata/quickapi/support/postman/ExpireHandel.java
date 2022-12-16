package io.tapdata.quickapi.support.postman;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.quickapi.support.postman.util.ApiMapUtil;

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
    public boolean builder(){
        Integer code = this.response.httpCode();
        Map<String, Object> body = this.response.result();
        Map<String, Object> header = this.response.headers();
        if(Objects.isNull(this.expireStatus)) return false;
        String[] split = expireStatus.split("=");
        if (split.length == 2) {
            String val = split[1];
            Object keyFromMap = ApiMapUtil.getKeyFromMap(body, split[0]);
            return Objects.equals(keyFromMap,val);
        }else {
            return false;
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
                throw new CoreException(String.format("The wrong token rule description %s, please declare according to the correct rule, such as AParamName=BParamName. ",tokenName));
            }
            String keyNameFromBody = split[0];
            String keyNameToToken = split[1];
            value = ApiMapUtil.getKeyFromMap(result,keyNameFromBody);
            if (Objects.nonNull(value)) {
                apiParam.put(keyNameToToken, value);
                return true;
            }
        }
        value = Objects.nonNull(value) ? value : fuzzyMatching();

        return true;
    }

    private String fuzzyMatching(){

        return "access_token";
    }

    private List<Map<String,Object>> widthTokenNameKeyAndValue(Map<String,Object> map){

        return null;
    }
}
