package io.tapdata.coding.service.schema;

import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class EnabledSchemas {
    public static final String TAG = EnabledSchemas.class.getSimpleName();
    AtomicReference<String> accessToken;
    public static void getAllSchemas(TapConnectionContext tapConnectionContext, Set<Class<? extends SchemaStart>> allImplClass, AtomicReference<String> accessToken) {
        EnabledSchemas enabledSchemas = new EnabledSchemas();
        enabledSchemas.accessToken = accessToken;
        StringBuilder errorMessage = new StringBuilder();
        try {
            Class<? extends SchemaStart> issue = enabledSchemas.issue(tapConnectionContext);
            if (null != allImplClass) {
                allImplClass.add(issue);
            }
        } catch (InvalidParameterValueException ie) {
            errorMessage.append(ie.getMessage()).append(";\r\n");
        } catch (Exception e) {
            throw e;
        }


        try {
            Class<? extends SchemaStart> iteration = enabledSchemas.iteration(tapConnectionContext);
            if (null != allImplClass) {
                allImplClass.add(iteration);
            }
        } catch (InvalidParameterValueException ie) {
            TapLogger.info(TAG, ie.getMessage());
            errorMessage.append(ie.getMessage()).append(";\r\n");
        } catch (Exception e) {
            throw e;
        }

        try {
            Class<? extends SchemaStart> projectMember = enabledSchemas.projectMember(tapConnectionContext);
            if (null != allImplClass) {
                allImplClass.add(projectMember);
            }
        } catch (InvalidParameterValueException ie) {
            TapLogger.info(TAG, ie.getMessage());
            errorMessage.append(ie.getMessage()).append(";\r\n");
        } catch (Exception e) {
            throw e;
        }

        int lastIndexOfNextLine = errorMessage.lastIndexOf(";\r\n");
        if (lastIndexOfNextLine > 0) {
            errorMessage.delete(lastIndexOfNextLine, errorMessage.length());
        }
        if (errorMessage.length() > 0) {
            throw new CoreException(errorMessage.toString());
        }
    }

    public Class<? extends SchemaStart> projectMember(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        HashMap<String, String> headers = new HashMap<>();//存放请求头，可以存放多个请求头
        headers.put("Authorization", accessToken.get());
        connectionConfig.put("token", accessToken.get());
        Map<String, Object> resultMap = CodingHttp.create(
                headers,
                HttpEntity.create().builderIfNotAbsent("Action", "DescribeTeamMembers").builder("PageNumber", 1).builder("PageSize", 1).getEntity(),
                String.format(CodingStarter.OPEN_API_URL, connectionConfig.get("teamName"))
        ).postWithError();
        //Response->Error->Code
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeTeamMembers action.");
        }
        Object res = resultMap.get("Response");
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeTeamMembers action.");
        }
        Object err = ((Map<String, Object>) res).get("Error");
        if (Checker.isNotEmpty(err)
                && Checker.isNotEmpty(((Map<String, Object>) err).get("Code"))
                && "InvalidParameterValue".equals(String.valueOf(((Map<String, Object>) err).get("Code")))) {
            throw new InvalidParameterValueException("Lose Team Members schema's permission," + String.valueOf(((Map<String, Object>) err).get("Message")));
        }
        return ProjectMembers.class;
    }


    public Class<? extends SchemaStart> iteration(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        HashMap<String, String> headers = new HashMap<>();//存放请求头，可以存放多个请求头
        Object projectName = connectionConfig.get("projectName");
        headers.put("Authorization", accessToken.get());
        connectionConfig.put("token", accessToken.get());
        Map<String, Object> resultMap = CodingHttp.create(
                headers,
                HttpEntity.create()
                        .builderIfNotAbsent("Action", "DescribeIterationList")
                        .builder("ProjectName", projectName).getEntity(),
                String.format(CodingStarter.OPEN_API_URL, connectionConfig.get("teamName"))
        ).postWithError();
        //Response->Error->Code
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeIterationList action.");
        }
        Object res = resultMap.get("Response");
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeIterationList action.");
        }
        Object err = ((Map<String, Object>) res).get("Error");
        if (Checker.isNotEmpty(err)
                && Checker.isNotEmpty(((Map<String, Object>) err).get("Code"))
                && "InvalidParameterValue".equals(String.valueOf(((Map<String, Object>) err).get("Code")))) {
            throw new InvalidParameterValueException("Lose Iteration schema's permission," + String.valueOf(((Map<String, Object>) err).get("Message")));
        }
        return Iterations.class;
    }


    public Class<? extends SchemaStart> issue(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        HashMap<String, String> headers = new HashMap<>();//存放请求头，可以存放多个请求头
        Object projectName = connectionConfig.get("projectName");
        headers.put("Authorization", accessToken.get());
        connectionConfig.put("token", accessToken.get());
        Map<String, Object> resultMap = CodingHttp.create(
                headers,
                HttpEntity.create().builderIfNotAbsent("Action", "DescribeIssueListWithPage")
                        .builder("ProjectName", projectName)
                        .builder("IssueType", "ALL")
                        .builder("PageNumber", 1)
                        .builder("PageSize", 1).getEntity(),
                String.format(CodingStarter.OPEN_API_URL, connectionConfig.get("teamName"))
        ).postWithError();
        //Response->Error->Code
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeIssueListWithPage action.");
        }
        Object res = resultMap.get("Response");
        if (Checker.isEmpty(resultMap)) {
            throw new CoreException("Http request error when execute DescribeIssueListWithPage action.");
        }
        Object err = ((Map<String, Object>) res).get("Error");
        if (Checker.isNotEmpty(err)
                && Checker.isNotEmpty(((Map<String, Object>) err).get("Code"))
                && "InvalidParameterValue".equals(String.valueOf(((Map<String, Object>) err).get("Code")))) {
            throw new InvalidParameterValueException("Lose Issue schema's permission," + String.valueOf(((Map<String, Object>) err).get("Message")));
        }
        return Issues.class;
    }

    public String tokenSetter(String token) {
        return Checker.isNotEmpty(token) ?
                (token.startsWith(CodingStarter.TOKEN_PREF) ? token : CodingStarter.TOKEN_PREF + token)
                : token;
    }

    class InvalidParameterValueException extends RuntimeException {
        public InvalidParameterValueException(String msg) {
            super(msg);
        }
    }
}
