package io.tapdata.coding.service.loader;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;


public abstract class CodingStarter {
    private static final String TAG = CodingStarter.class.getSimpleName();

    public static final String CONNECTION_URL = "https://%s.coding.net";
    public static final String OPEN_API_URL = "https://%s.coding.net/open-api";//%{s}---》teamName
    public static final String TOKEN_URL = "https://%s.coding.net/api/me";
    public static final String TOKEN_PREF = "token ";
    public static final String TOKEN_PREF_OAUTH = "Bearer ";

    private final AtomicReference<String> accessToken;

    protected TapConnectionContext tapConnectionContext;

    CodingConnector codingConnector;

    ContextConfig codingConfig;

    public synchronized AtomicReference<String> accessToken() {
        return this.accessToken;
    }


    public CodingStarter connectorInit(CodingConnector codingConnector) {
        this.codingConnector = codingConnector;
        return this;
    }

    public CodingStarter connectorOut() {
        this.codingConnector = null;
        return this;
    }

    public boolean sync() {
        synchronized (codingConnector) {
            return null != codingConnector && codingConnector.isAlive();
        }
    }

    protected boolean isVerify;

    CodingStarter(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        this.tapConnectionContext = tapConnectionContext;
        this.accessToken = accessToken;
        this.isVerify = Boolean.FALSE;
    }

    protected static Entry entry(String key, Object value) {
        return new Entry(key, value);
    }

    public String tokenSetter(String token) {
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        Object isOAuth = connectionConfig.get("isOAuth");
        if (Objects.isNull(isOAuth) || !"true".equals(String.valueOf(isOAuth))) {
            return Checker.isNotEmpty(token) ?
                    (token.startsWith(TOKEN_PREF) ? token : TOKEN_PREF + token)
                    : token;
        } else {
            return Checker.isNotEmpty(token) ?
                    (token.startsWith(TOKEN_PREF_OAUTH) ? token : TOKEN_PREF_OAUTH + token)
                    : token;
        }
    }

    public ContextConfig veryContextConfigAndNodeConfig() {
        if (Objects.nonNull(codingConfig)) {
            return codingConfig;
        }
        this.verifyConnectionConfig();
        DataMap connectionConfigConfigMap = this.tapConnectionContext.getConnectionConfig();

        String loginMode = connectionConfigConfigMap.getString("loginMode");
        String clientId = connectionConfigConfigMap.getString("clientId");
        String clientSecret = connectionConfigConfigMap.getString("clientSecret");
        String refreshToken = connectionConfigConfigMap.getString("refreshToken");

        String projectName = connectionConfigConfigMap.getString("projectName");
        String token = connectionConfigConfigMap.getString("token");
        token = this.tokenSetter(token);
        String teamName = connectionConfigConfigMap.getString("teamName");
        String streamReadType = connectionConfigConfigMap.getString("streamReadType");
        String connectionMode = connectionConfigConfigMap.getString("connectionMode");
        ContextConfig config = ContextConfig.create()
                .loginMode(loginMode)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .refreshToken(refreshToken)
                .projectName(projectName)
                .teamName(teamName)
                .token(token)
                .streamReadType(streamReadType)
                .connectionMode(connectionMode);
        if (this.tapConnectionContext instanceof TapConnectorContext) {
            DataMap nodeConfigMap = this.tapConnectionContext.getNodeConfig();
            if (null == nodeConfigMap) {
                config.issueType(IssueType.ALL);
                config.iterationCodes("-1");
            } else {
                String iterationCodeArr = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                if (null != iterationCodeArr) iterationCodeArr = iterationCodeArr.trim();
                String issueType = nodeConfigMap.getString("issueType");
                if (null != issueType) issueType = issueType.trim();
                String issueCodes = nodeConfigMap.getString("issueCodes");
                if (null != issueCodes) issueCodes = issueCodes.trim();

                config.issueType(issueType).iterationCodes(iterationCodeArr).issueCodes(issueCodes);
            }
        }
        return codingConfig = config;
    }

    /**
     * 校验connectionConfig配置字段
     */
    public void verifyConnectionConfig() {
        if (this.isVerify) {
            return;
        }
        if (null == this.tapConnectionContext) {
            throw new IllegalArgumentException("TapConnectorContext cannot be null");
        }
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (null == connectionConfig) {
            throw new IllegalArgumentException("TapTable' DataMap cannot be null");
        }
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");
        String streamReadType = connectionConfig.getString("streamReadType");
        String connectionMode = connectionConfig.getString("connectionMode");
        if (null == projectName || "".equals(projectName)) {
            TapLogger.debug(TAG, "Connection parameter exception: {} ", projectName);
        }
        if (null == token || "".equals(token)) {
            TapLogger.debug(TAG, "Connection parameter exception: {} ", token);
        }

        if (null == accessToken().get() || accessToken().get().trim().equals("")) {
            accessToken().set(this.tokenSetter(token));
        }

        if (null == teamName || "".equals(teamName)) {
            TapLogger.debug(TAG, "Connection parameter exception: {} ", teamName);
        }
        if (null == streamReadType || "".equals(streamReadType)) {
            TapLogger.info(TAG, "Connection parameter streamReadType exception: {} ", token);
        }
        if (null == connectionMode || "".equals(connectionMode)) {
            TapLogger.info(TAG, "Connection parameter connectionMode exception: {} ", teamName);
        }
        this.isVerify = Boolean.TRUE;
    }

    public String refreshTokenByOAuth2() {
        HttpRequest request = HttpUtil.createPost(String.format(
                "https://%s.coding.net/api/oauth/access_token?refresh_token=%s&client_id=%s&client_secret=%s&grant_type=refresh_token",
                codingConfig.getTeamName(),
                codingConfig.refreshToken(),
                codingConfig.clientId(),
                codingConfig.clientSecret()
        ));
        request.contentLength(253);
        request.header("Content-Type", "application/x-www-form-urlencoded");
        request.header("Host", "muo.suuuy.dev.coding.io");
        HttpResponse execute = request.execute();
        StringBuilder errorMsg = new StringBuilder();
        if (Objects.nonNull(execute)) {
            String body = execute.body();
            if (Objects.nonNull(body)) {
                Map<?, ?> json = fromJson(body, Map.class);
                if (Objects.nonNull(json)) {
                    Object token = json.get("access_token");
                    if (Objects.nonNull(token)) {
                        accessToken().set(String.format("Bearer %s", token));
                        codingConfig.token(accessToken().get());
                        return accessToken().get();
                    } else {
                        errorMsg.append("Cannot get refresh token from response body. ").append(Optional.ofNullable((String) json.get(CodingHttp.ERROR_KEY)).orElse(""));
                    }
                }
            }
        } else {
            errorMsg.append("Cannot get refresh token from response body, body content is empty. ");
        }
        throw new CoreException(errorMsg.toString());
    }
}
