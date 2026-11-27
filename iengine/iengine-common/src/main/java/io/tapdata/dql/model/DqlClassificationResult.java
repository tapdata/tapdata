package io.tapdata.dql.model;

import lombok.Data;

/**
 * Classification output shared by Engine capture components.
 */
@Data
public class DqlClassificationResult {
    private DqlExceptionScope exceptionScope;
    private DqlRouteDecision routeDecision;
    private DqlErrorType errorType;
    private String classificationReason;
    private DqlClassificationConfidence classificationConfidence;

    public void applyTo(DqlEventReport report) {
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        report.setExceptionScope(exceptionScope);
        report.setRouteDecision(routeDecision);
        report.setErrorType(errorType);
        report.setClassificationReason(classificationReason);
        report.setClassificationConfidence(classificationConfidence);
    }
}
