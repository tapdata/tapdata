package io.tapdata.coding.entity;

import io.tapdata.coding.enums.IssueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ContextConfig {
    public static ContextConfig create(){
        return new ContextConfig();
    }
    private String projectName;
    private String token;
    private String teamName;
    private String iterationCodes;
    private IssueType issueType;

    public ContextConfig projectName(String projectName){
        this.projectName = projectName;
        return this;
    }
    public ContextConfig token(String token){
        this.token = token;
        return this;
    }
    public ContextConfig teamName(String teamName){
        this.teamName = teamName;
        return this;
    }
    public ContextConfig iterationCodes(String iterationCodes){
        this.iterationCodes = iterationCodes;
        return this;
    }
    public ContextConfig issueType(String issueTypeName){
        this.issueType = IssueType.issueType(issueTypeName);
        return this;
    }
    public ContextConfig issueType(IssueType issueType){
        this.issueType = issueType;
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
//    public List<String> getIterationCodes() {
//        List<String> result = new ArrayList<>();
//        return null==iterationCodes ? result: new ArrayList<String>(){{Arrays.asList(iterationCodes.split(","));}};
//    }

    public void setIterationCodes(String iterationCodes) {
        this.iterationCodes = iterationCodes;
    }

    public IssueType getIssueType() {
        return issueType;
    }

    public void setIssueType(IssueType issueType) {
        this.issueType = issueType;
    }
}
