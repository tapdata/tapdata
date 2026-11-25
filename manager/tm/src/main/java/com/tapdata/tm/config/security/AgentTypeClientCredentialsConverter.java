package com.tapdata.tm.config.security;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.endpoint.OAuth2ParameterNames;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2ClientCredentialsAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.web.authentication.OAuth2ClientCredentialsAuthenticationConverter;
import org.springframework.security.web.authentication.AuthenticationConverter;

import java.util.Map;

/**
 * Carries the {@code agent_type} request parameter into the token request.
 *
 * <p>Why this exists: the stock converter builds the authentication from a
 * fixed list of parameters, so an extra one a caller sends is dropped before the
 * token customizer ever runs, and the resulting JWT carries no trace of it.
 * The db2z connector has to tell TM which kind of Reader a token is meant for;
 * without that claim on the token, a token minted for any other application of
 * the same authorization server verifies just as well at the Reader, which is
 * exactly what the Reader's check exists to prevent.
 *
 * <p>This delegates to the stock converter for everything else - grant type
 * detection, the client principal, scope handling - and only adds the extra
 * parameter on top.  Delegating rather than reimplementing keeps the standard
 * behaviour, including its error responses, exactly as before.
 *
 * <p>Register it <em>before</em> the stock converter: the filter stops at the
 * first converter that returns a non-null authentication, so a converter added
 * after it would never see a client credentials request.
 *
 * <p>The value is passed through verbatim; whether it is trustworthy is not
 * this class's decision.
 */
public class AgentTypeClientCredentialsConverter implements AuthenticationConverter {

    /** Request parameter carrying the service type, e.g. {@code Db2 for z cdc reader}. */
    public static final String AGENT_TYPE_PARAMETER = "agent_type";

    private final OAuth2ClientCredentialsAuthenticationConverter delegate =
            new OAuth2ClientCredentialsAuthenticationConverter();

    @Override
    public Authentication convert(HttpServletRequest request) {
        String grantType = request.getParameter(OAuth2ParameterNames.GRANT_TYPE);
        if (!AuthorizationGrantType.CLIENT_CREDENTIALS.getValue().equals(grantType)) {
            return null;
        }
        /*
         * The stock object is final and its additional parameters are not
         * mutable through an accessor, so it is rebuilt here from the values it
         * derived.  Reading them back from the delegate keeps one implementation
         * of the standard parsing.
         */
        Authentication converted = delegate.convert(request);
        if (!(converted instanceof OAuth2ClientCredentialsAuthenticationToken stock)) {
            return converted;
        }
        /*
         * Absent is not an error: a caller that sends nothing gets a token that
         * names no service type, and the Reader decides whether to accept it.
         * Writing the claim is the token customizer's job; this only moves the
         * value as far as the authentication.
         */
        String agentType = StringUtils.trimToNull(request.getParameter(AGENT_TYPE_PARAMETER));
        if (agentType == null) {
            return stock;
        }
        Map<String, Object> parameters = new java.util.HashMap<>(stock.getAdditionalParameters());
        parameters.put(AGENT_TYPE_PARAMETER, agentType);
        return new OAuth2ClientCredentialsAuthenticationToken(
                (Authentication) stock.getPrincipal(), stock.getScopes(), parameters);
    }
}
