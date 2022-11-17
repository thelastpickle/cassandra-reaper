package io.cassandrareaper.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;

public class RequestUtils {
    private static final Logger log = LoggerFactory.getLogger(RequestUtils.class);

    public static final String ALLOW_ALL_OPTIONS_REQUESTS_ENV_VAR_NAME = "ALLOW_ALL_OPTIONS_REQUESTS";

    public static boolean getAllowAllOptionsRequestsFromEnvironment() {
        String allowAllOptionsRequestsEnvVarValue = System.getenv(ALLOW_ALL_OPTIONS_REQUESTS_ENV_VAR_NAME);
        if (allowAllOptionsRequestsEnvVarValue != null) {
            try {
                return Boolean.parseBoolean(allowAllOptionsRequestsEnvVarValue.trim().toLowerCase());
            } catch (Exception e) {
                log.warn("Unable to parse environment variable \"" + ALLOW_ALL_OPTIONS_REQUESTS_ENV_VAR_NAME + "\" value \"" + allowAllOptionsRequestsEnvVarValue + "\", default of false will be used.", e);
                return false;
            }
        }
        return false;
    }
    public static boolean isOptionsRequest(ServletRequest request) {
        if(request != null && request instanceof HttpServletRequest){
            if(((HttpServletRequest) request).getMethod().equalsIgnoreCase(HttpMethod.OPTIONS)){
                return true;
            }
        }
        return false;
    }
}
