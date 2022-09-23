package io.tapdata.zoho.service.zoho;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.List;
import java.util.Map;

public class TicketLoader extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TicketLoader.class.getSimpleName();

    private TicketLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TicketLoader create(TapConnectionContext tapConnectionContext){
        return new TicketLoader(tapConnectionContext);
    }

    /**
     * 根据工单ID获取一个工单详情
     * */
    public Map<String,Object> getOne(){
        String ticketId = "";
        String url = "/api/v1/tickets/{ticketID}";

        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization","");
        HttpEntity<String,String> resetFull = HttpEntity.create().build("ticketID",ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.POST,header).resetFull(resetFull);
        return null;
    }

    /**
     * 获取工单列表
     *
     * */
    public List<Map<String,Object>> list(){
        return null;
    }

    public static void main(String[] args) {
        String url = "/api/v1/tickets/{ticketID}";
        System.out.println(String.format(ZO_HO_BASE_URL,url));

        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization","1");
        HttpEntity<String,String> resetFull = HttpEntity.create().build("ticketID","815");
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.POST,header).resetFull(resetFull);
        http.post();
        System.out.println(http.getUrl());
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
