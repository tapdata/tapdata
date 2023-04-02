package io.tapdata.coding.entity;

import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.utils.tool.Checker;

import java.util.*;

public class ContextConfig {
    public static ContextConfig create() {
        return new ContextConfig();
    }

    private String projectName;
    private Long projectId;
    private String token;
    private String teamName;
    private String iterationCodes;
    private IssueType issueType;
    private String connectionMode;
    private String streamReadType;

    private String loginMode;
    private String refreshToken;
    private String clientId;
    private String clientSecret;

    private List<Integer> issueCodes;

    public String loginMode() {
        return this.loginMode;
    }

    public ContextConfig loginMode(String loginMode) {
        this.loginMode = loginMode;
        return this;
    }

    public String refreshToken() {
        return this.refreshToken;
    }

    public ContextConfig refreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public String clientId() {
        return this.clientId;
    }

    public ContextConfig clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public String clientSecret() {
        return this.clientSecret;
    }

    public ContextConfig clientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

    public ContextConfig projectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public ContextConfig projectId(Long projectId) {
        this.projectId = projectId;
        return this;
    }

    public ContextConfig streamReadType(String streamReadType) {
        this.streamReadType = streamReadType;
        return this;
    }

    public ContextConfig connectionMode(String connectionMode) {
        this.connectionMode = connectionMode;
        return this;
    }

    public ContextConfig token(String token) {
        this.token = token;
        return this;
    }

    public ContextConfig teamName(String teamName) {
        this.teamName = teamName;
        return this;
    }

    public ContextConfig iterationCodes(String iterationCodes) {
        this.iterationCodes = iterationCodes;
        return this;
    }

    public ContextConfig issueType(String issueTypeName) {
        this.issueType = IssueType.issueType(issueTypeName);
        return this;
    }

    public ContextConfig issueType(IssueType issueType) {
        this.issueType = issueType;
        return this;
    }

    public ContextConfig issueCodes(List<Integer> issueCodes) {
        this.issueCodes = issueCodes;
        return this;
    }

    public ContextConfig issueCodes(String issueCodes) {
        if (Checker.isNotEmpty(issueCodes)) {
            Set<Integer> list = new HashSet<Integer>();
            String[] issueCodeArr = issueCodes.split(",");
            for (String issueCode : issueCodeArr) {
                list.add(Integer.parseInt(issueCode));
            }
            this.issueCodes = new ArrayList<>(list);
        }
        return this;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public String getIterationCodes() {
        return iterationCodes;
    }

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public void setIterationCodes(String iterationCodes) {
        this.iterationCodes = iterationCodes;
    }

    public IssueType getIssueType() {
        return issueType;
    }

    public void setIssueType(IssueType issueType) {
        this.issueType = issueType;
    }

    public String getConnectionMode() {
        return connectionMode;
    }

    public void setConnectionMode(String connectionMode) {
        this.connectionMode = connectionMode;
    }

    public String getStreamReadType() {
        return streamReadType;
    }

    public void setStreamReadType(String streamReadType) {
        this.streamReadType = streamReadType;
    }

    public List<Integer> issueCodes() {
        return this.issueCodes;
    }
}
