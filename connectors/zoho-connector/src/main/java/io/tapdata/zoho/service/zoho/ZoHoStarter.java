package io.tapdata.zoho.service.zoho;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.ContextConfig;

public abstract class ZoHoStarter {
    private static final String TAG = ZoHoStarter.class.getSimpleName();

    protected TapConnectionContext tapConnectionContext;
    protected boolean isVerify;
    protected ZoHoStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        this.isVerify = Boolean.FALSE;
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
            TapLogger.info(TAG, "Connection parameter exception: {} ", projectName);
        }
        if ( null == token || "".equals(token) ){
            TapLogger.info(TAG, "Connection parameter exception: {} ", token);
        }
        if ( null == teamName || "".equals(teamName) ){
            TapLogger.info(TAG, "Connection parameter exception: {} ", teamName);
        }
        if ( null == streamReadType || "".equals(streamReadType) ){
            TapLogger.info(TAG, "Connection parameter streamReadType exception: {} ", token);
        }
        if ( null == connectionMode || "".equals(connectionMode) ){
            TapLogger.info(TAG, "Connection parameter connectionMode exception: {} ", teamName);
        }
        this.isVerify = Boolean.TRUE;
    }
    public ContextConfig veryContextConfigAndNodeConfig(){
        this.verifyConnectionConfig();
        DataMap connectionConfigConfigMap = this.tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfigConfigMap.getString("projectName");
        String token = connectionConfigConfigMap.getString("token");
        String teamName = connectionConfigConfigMap.getString("teamName");
        String streamReadType = connectionConfigConfigMap.getString("streamReadType");
        String connectionMode = connectionConfigConfigMap.getString("connectionMode");
        ContextConfig config = ContextConfig.create().projectName(projectName)
                .teamName(teamName)
                .token(token)
                .streamReadType(streamReadType)
                .connectionMode(connectionMode);
        if (this.tapConnectionContext instanceof TapConnectorContext) {
            DataMap nodeConfigMap = ((TapConnectorContext)this.tapConnectionContext).getNodeConfig();
            if (null == nodeConfigMap) {
//                config.issueType(IssueType.ALL);
                config.iterationCodes("-1");
                TapLogger.debug(TAG,"TapTable' NodeConfig is empty. ");
                //throw new IllegalArgumentException("TapTable' NodeConfig cannot be null");
            }else{
                //iterationName is Multiple selection values separated by commas
                String iterationCodeArr = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                if (null != iterationCodeArr) iterationCodeArr = iterationCodeArr.trim();
                String issueType = nodeConfigMap.getString("issueType");
                if (null != issueType) issueType = issueType.trim();

                if (null == iterationCodeArr || "".equals(iterationCodeArr)) {
                    TapLogger.info(TAG, "Connection node config iterationName exception: {} ", projectName);
                }
                if (null == issueType || "".equals(issueType)) {
                    TapLogger.info(TAG, "Connection node config issueType exception: {} ", token);
                }
//                config.issueType(issueType).iterationCodes(iterationCodeArr);
            }
        }
        return config;
    }
}