package com.socrata.kettle.plugin;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * @author malindac
 * @version 1.0
 * 2017-09-20
 */
public class SocrataPublishUtil {

    private static HttpClient httpClient;
    private static String appToken = "Eu1EiCmxZN4DWT9UmiXCLQpl6";
    private static ObjectMapper mapper = new ObjectMapper();

    private static HttpClient getClient() {
        if (httpClient == null) {
            httpClient = new HttpClient();
        }

        return httpClient;
    }

    public static JsonNode execute(HttpMethod method, LogChannelInterface log) throws KettleException {
        HttpClient client = getClient();
        JsonNode response = null;
        try {
            log.logDebug("Executing: " + method.getURI());
            int statusCode = client.executeMethod(method);
            log.logBasic("Request status code: " + statusCode);

            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
                throw new KettleStepException("Request failed: " + method.getStatusLine());
            }

            String responseString = method.getResponseBodyAsString();
            log.logDebug(responseString);
            response = mapper.readTree(responseString);
        } catch (Exception ex) {
            log.logError(ex.getMessage());
            throw new KettleException("Failure executing request: " + ex.getMessage());
        } finally {
            method.releaseConnection();
            log.logDebug("HttpClient connection released");
        }
        return response;
    }

    public static GetMethod get(String url, String host, String auth, String contentType) {
        GetMethod get = new GetMethod(url);
        get.setRequestHeader("Authorization", "Basic " + auth);
        get.setRequestHeader("Content-Type", contentType);
        get.setRequestHeader("X-App-Token", appToken);
        get.setRequestHeader("X-Socrata-Host", host);

        get.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(2, false));

        return get;
    }

    public static PostMethod getPost(String url, String host, String auth, String contentType) {
        PostMethod post = new PostMethod(url);
        post.setRequestHeader("Authorization", "Basic " + auth);
        post.setRequestHeader("Content-Type", contentType);
        post.setRequestHeader("X-App-Token", appToken);
        post.setRequestHeader("X-Socrata-Host", host);

        post.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(2, false));

        return post;
    }

    public static PutMethod getPut(String url, String host, String auth, String contentType) {
        PutMethod put = new PutMethod(url);

        put.setRequestHeader("Authorization", "Basic " + auth);
        put.setRequestHeader("Content-Type", contentType);
        put.setRequestHeader("X-App-Token", appToken);
        put.setRequestHeader("X-Socrata-Host", host);

        put.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(2, false));

        return put;
    }

    public static String setHost(SocrataPluginMeta meta) {
        String host = "";
        String domain = meta.getDomain();

        if (domain.startsWith("https://")) {
            host = domain.replace("https://", "");
        } else if (domain.startsWith("http://")) {
            host = domain.replace("http://", "");
        } else {
            host = domain;
            meta.setDomain("https://" + domain);
        }

        return host;
    }

    public static boolean hasValue(String option) {
        return option != null && !option.isEmpty();
    }
}
