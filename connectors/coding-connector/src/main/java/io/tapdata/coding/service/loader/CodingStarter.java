package io.tapdata.coding.service.loader;

import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class CodingStarter {
    private static final String TAG = CodingStarter.class.getSimpleName();

    public static final String CONNECTION_URL = "https://%s.coding.net";
    public static final String OPEN_API_URL = "https://%s.coding.net/open-api";//%{s}---》teamName
    public static final String TOKEN_URL = "https://%s.coding.net/api/me";
    public static final String TOKEN_PREF = "token ";

    protected TapConnectionContext tapConnectionContext;
    protected boolean isVerify;
    ContextConfig contextConfig;
    public CodingStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        this.isVerify = Boolean.FALSE;
    }

    protected static Entry entry(String key, Object value){
        return new Entry(key,value);
    }

    public String tokenSetter(String token){
        return Checker.isNotEmpty(token) ?
                (token.startsWith(TOKEN_PREF) ? token : TOKEN_PREF + token)
                : token;
    }

    public ContextConfig veryContextConfigAndNodeConfig(){
        this.verifyConnectionConfig();
        DataMap connectionConfigConfigMap = this.tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfigConfigMap.getString("projectName");
        String token = connectionConfigConfigMap.getString("token");
        token = this.tokenSetter(token);
        String teamName = connectionConfigConfigMap.getString("teamName");
        String streamReadType = connectionConfigConfigMap.getString("streamReadType");
        String connectionMode = connectionConfigConfigMap.getString("connectionMode");
        ContextConfig config = ContextConfig.create()
                .projectName(projectName)
                .teamName(teamName)
                .token(token)
                .streamReadType(streamReadType)
                .connectionMode(connectionMode);
        if (this.tapConnectionContext instanceof TapConnectorContext) {
            DataMap nodeConfigMap = ((TapConnectorContext)this.tapConnectionContext).getNodeConfig();
            if (null == nodeConfigMap) {
                config.issueType(IssueType.ALL);
                config.iterationCodes("-1");
                //TapLogger.debug(TAG,"TapTable' NodeConfig is empty. ");
                //throw new IllegalArgumentException("TapTable' NodeConfig cannot be null");
            }else{
                //iterationName is Multiple selection values separated by commas
//                Object iterationCodeList = nodeConfigMap.getObject("iterations");
//                if (iterationCodeList instanceof Collection){
//                    ((List)iterationCodeList).spliterator().
//                }
                String iterationCodeArr = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                if (null != iterationCodeArr) iterationCodeArr = iterationCodeArr.trim();
                String issueType = nodeConfigMap.getString("issueType");
                if (null != issueType) issueType = issueType.trim();
                String issueCodes = nodeConfigMap.getString("issueCodes");
                if (null != issueCodes) issueCodes = issueCodes.trim();
                //if (Checker.isEmpty(issueCodes)){
                    //TapLogger.info(TAG, "Connection node config issueCodes exception: {} ", projectName);
                //}
                //if (Checker.isEmpty(iterationCodeArr)) {
                    //TapLogger.info(TAG, "Connection node config iterationName exception: {} ", projectName);
                //}
                //if (Checker.isEmpty(issueType)) {
                    //TapLogger.info(TAG, "Connection node config issueType exception: {} ", token);
                //}
                config.issueType(issueType).iterationCodes(iterationCodeArr).issueCodes(issueCodes);
            }
        }
        this.contextConfig = config;
        return config;
    }

    /**
     * 校验connectionConfig配置字段
     */
    public void verifyConnectionConfig(){
        if(this.isVerify){
            return;
        }
        if (null == this.tapConnectionContext){
            throw new IllegalArgumentException("TapConnectorContext cannot be null");
        }
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (null == connectionConfig ){
            throw new IllegalArgumentException("TapTable' DataMap cannot be null");
        }
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");
        String streamReadType = connectionConfig.getString("streamReadType");
        String connectionMode = connectionConfig.getString("connectionMode");
        if ( null == projectName || "".equals(projectName)){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", projectName);
        }
        if ( null == token || "".equals(token) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", token);
        }
        if ( null == teamName || "".equals(teamName) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", teamName);
        }
        if ( null == streamReadType || "".equals(streamReadType) ){
            TapLogger.info(TAG, "Connection parameter streamReadType exception: {} ", token);
        }
        if ( null == connectionMode || "".equals(connectionMode) ){
            TapLogger.info(TAG, "Connection parameter connectionMode exception: {} ", teamName);
        }
        this.isVerify = Boolean.TRUE;
    }
}
