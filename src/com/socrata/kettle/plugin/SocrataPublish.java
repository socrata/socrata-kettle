package com.socrata.kettle.plugin;

import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.codehaus.jackson.JsonNode;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.File;
import java.io.IOException;
import java.util.Base64;

/**
 * @author malindac
 * @version 1.0
 * 2017-09-20
 */
public class SocrataPublish {

    private LogChannelInterface log;
    private String host;
    private String domain;
    private String authorize;
    private String datasetId;
    private String importConfigName;
    private File file;

    private int revisionSeq;
    private String createSourcePath;
    private String applyRevisionPath;
    private String uploadDataPath;
    private String rowErrorPath;
    private String latestOutputPath;
    private int outputSchemaId;
    private String createRevisionPath;
    private String buildConfigPath;

    public void publish(SocrataPluginMeta meta, File file, LogChannelInterface log) throws IOException {
        this.log = log;
        host = SocrataPublishUtil.setHost(meta);
        domain = meta.getDomain();
        String credentials = meta.getUser() + ":" + meta.getPassword();
        authorize = Base64.getEncoder().encodeToString(credentials.getBytes());
        datasetId = meta.getDatasetName();
        importConfigName = meta.getImportConfig();
        this.file = file;

        if (SocrataPublishUtil.hasValue(importConfigName)) {
            createRevisionFromConfig();
            createSource(this.file.toString());
            uploadSourceData(this.file.toString());
            getLatestOutput();
            applyRevision();
        } else {
            
        }
    }

    private void createRevision() throws IOException {

        String url = domain + "/api/publishing/v1/revision/" + datasetId;
        PostMethod httpPost = SocrataPublishUtil.getPost(url, host, authorize, "application/json");

        JsonNode results = SocrataPublishUtil.execute(httpPost, log);
        //Get revision_seq value
        if (results != null) {
            JsonNode revSeq = results.findValue("revision_seq");
            revisionSeq = revSeq.asInt();
            log.logDebug("Created revision sequence number: " + revisionSeq);

            JsonNode csp = results.findValue("create_source");
            createSourcePath = csp.asText();
            log.logDebug(createSourcePath);

            JsonNode apply = results.findValue("apply");
            applyRevisionPath = apply.asText();
            log.logDebug(applyRevisionPath);
        }
    }

    private void createSource(String filename) throws IOException {

        PostMethod httpPost = SocrataPublishUtil.getPost(domain + createSourcePath, host, authorize, "application/json");

        String json = "{ \"source_type\": {\"type\": \"upload\", \"filename\": \"" + filename + "\" }}";
        StringRequestEntity string = new StringRequestEntity(json, "application/json", "UTF-8");
        log.logDebug(json);

        httpPost.setRequestEntity(string);

        JsonNode results = SocrataPublishUtil.execute(httpPost, log);

        if (results != null) {
            JsonNode bytes = results.findValue("bytes");
            uploadDataPath = bytes.asText();
            log.logDebug(uploadDataPath);
        }
    }

    private void uploadSourceData(String filename) throws IOException {

        PostMethod httpPost = SocrataPublishUtil.getPost(domain + uploadDataPath, host, authorize, "text/csv");

        Part[] parts = {
                new FilePart(filename, file)
        };

        httpPost.setRequestEntity(new MultipartRequestEntity(parts, httpPost.getParams()));

        JsonNode results = SocrataPublishUtil.execute(httpPost, log);

        if (results != null) {
            JsonNode errors = results.findValue("error_count");
            int errorCount = errors.asInt();
            log.logBasic("Error count: " + errorCount);
            if (errorCount > 0) {
                throw new IOException("Errors detected in source file upload");
            }

            JsonNode rowErrors = results.findValue("row_errors");
            rowErrorPath = rowErrors.asText();
            log.logDebug(rowErrorPath);

            JsonNode latestOutput = results.findValue("latest_output");
            latestOutputPath = latestOutput.asText();
            log.logDebug(latestOutputPath);
        }
    }

    private void createRevisionFromConfig() throws IOException {

        String url = domain;
        if (createRevisionPath == null || createRevisionPath.isEmpty()) {
            url += "/api/publishing/v1/revision/" + datasetId + "?config=" + importConfigName;
        } else {
            url += createRevisionPath;
        }

        PostMethod httpPost = SocrataPublishUtil.getPost(url, host, authorize, "application/json");

        JsonNode results = SocrataPublishUtil.execute(httpPost, log);

        if (results != null) {
            JsonNode revSeq = results.findValue("revision_seq");
            revisionSeq = revSeq.asInt();
            log.logDebug("Created revision sequence number: " + revisionSeq);

            JsonNode csp = results.findValue("create_source");
            createSourcePath = csp.asText();
            log.logDebug(createSourcePath);

            JsonNode apply = results.findValue("apply");
            applyRevisionPath = apply.asText();
            log.logDebug(applyRevisionPath);
        }
    }

    private void applyRevision() throws IOException {
        PutMethod httpPut = SocrataPublishUtil.getPut(domain + applyRevisionPath, host, authorize, "application/json");

        String json = "{ \"output_schema_id\": " + outputSchemaId + " }";
        StringRequestEntity string = new StringRequestEntity(json, "application/json", "UTF-8");
        log.logDebug(json);

        httpPut.setRequestEntity(string);

        JsonNode results = SocrataPublishUtil.execute(httpPut, log);
        if(results != null) {
            JsonNode statusNode = results.findValue("status");
            String status = statusNode.asText();
            log.logDebug(status);
        }
    }

    private void getLatestOutput() throws IOException {

        JsonNode results = SocrataPublishUtil.execute(
                SocrataPublishUtil.get(domain + latestOutputPath, host, authorize, "application/json"), log);

        if (results != null) {
            JsonNode id = results.path("resource").path("id");
            outputSchemaId = id.asInt();
            log.logDebug("Output Schema ID: " + outputSchemaId);

            JsonNode buildConfig = results.findValue("build_config");
            buildConfigPath = buildConfig.asText();
            log.logDebug(buildConfigPath);
        }
    }
}
