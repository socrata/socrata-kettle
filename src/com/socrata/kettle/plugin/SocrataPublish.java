package com.socrata.kettle.plugin;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.File;

/**
 * @author malindac
 * @version 1.0
 * 2017-07-06
 */
public class SocrataPublish {

    static void upsert(String domain, String authorize, String apiKey, String datasetId, String fileLocation, LogChannelInterface log)
            throws Exception {

        String host = getHost(domain);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(domain + "/resource/" + datasetId + ".json");
            post.setHeader("Authorization", "Basic " + authorize);
            post.setHeader("Content-Type", "text/csv");
            post.setHeader("X-App-Token", apiKey);
            post.setHeader("X-Socrata-Host", host);

            File file = new File(fileLocation);
            FileEntity entity = new FileEntity(file);
            entity.setContentType("text/csv");

            post.setEntity(entity);

            log.logDebug("Executing request: " + post.getRequestLine());
            for (Header h : post.getAllHeaders()) {
                log.logDebug(h.getName() + " : " + h.getValue());
            }

            try (CloseableHttpResponse response = httpClient.execute(post)) {
                log.logBasic("----------------------------------------");
                log.logBasic(response.getStatusLine().toString());
                log.logBasic(EntityUtils.toString(response.getEntity()));
            }
        }
    }

    static void replace(String domain, String authorize, String apiKey, String datasetId, String fileLocation, LogChannelInterface log)
            throws Exception {

        String host = getHost(domain);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPut put = new HttpPut(domain + "/resource/" + datasetId + ".json");
            put.setHeader("Authorization", "Basic " + authorize);
            put.setHeader("Content-Type", "text/csv");
            put.setHeader("X-App-Token", apiKey);
            put.setHeader("X-Socrata-Host", host);

            File file = new File(fileLocation);
            FileEntity entity = new FileEntity(file);
            entity.setContentType("text/csv");

            put.setEntity(entity);

            log.logDebug("Executing request: " + put.getRequestLine());
            for (Header h : put.getAllHeaders()) {
                log.logDebug(h.getName() + " : " + h.getValue());
            }

            try(CloseableHttpResponse response = httpClient.execute(put)) {
                log.logBasic("----------------------------------------");
                log.logBasic(response.getStatusLine().toString());
                log.logBasic(EntityUtils.toString(response.getEntity()));
            }
        }
    }

    private static String getHost(String domain) {
        String host = "";
        if (domain.startsWith("https://")) {
            host = domain.replace("https://", "");
        } else if (domain.startsWith("http://")) {
            host = domain.replace("http://", "");
        } else {
            //domain = "https://" + domain;
            host = domain;
        }

        return host;
    }
}
