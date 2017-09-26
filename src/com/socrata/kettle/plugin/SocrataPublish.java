package com.socrata.kettle.plugin;

import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
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
    private String writerMode;
    private File file;
    private SocrataTextFileField[] outputFields;

    private int revisionSeq;
    private String createSourcePath;
    private String applyRevisionPath;
    private String uploadDataPath;
    private String rowErrorPath;
    private String latestOutputPath;
    private int outputSchemaId;
    private String createRevisionPath;
    private String buildConfigPath;

    public void publish(SocrataPluginMeta meta, File file, LogChannelInterface log) throws Exception {
        this.log = log;
        host = SocrataPublishUtil.setHost(meta);
        domain = meta.getDomain();
        String credentials = meta.getUser() + ":" + meta.getPassword();
        authorize = Base64.getEncoder().encodeToString(credentials.getBytes());
        datasetId = meta.getDatasetName();
        importConfigName = meta.getImportConfig();
        writerMode = meta.getWriterMode();
        outputFields = meta.getOutputFields();
        this.file = file;

        if (!SocrataPublishUtil.hasValue(importConfigName)) {
            createImportConfig(meta);
        }
        createRevisionFromConfig();
        createSource(this.file.toString());
        uploadSourceData();
        getLatestOutput();
        applyRevision();
    }

    private void createSource(String filename) throws Exception {

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

    private void uploadSourceData() throws Exception {

        PostMethod httpPost = SocrataPublishUtil.getPost(domain + uploadDataPath, host, authorize, "text/csv");
        FileRequestEntity fre = new FileRequestEntity(file, "text/csv");
        httpPost.setRequestEntity(fre);

        JsonNode results = SocrataPublishUtil.execute(httpPost, log);

        if (results != null) {

            String inputSchemaId = "";
            JsonNode schemas = results.path("resource").path("schemas");
            for (JsonNode schema : schemas) {
                JsonNode outputSchemas = schema.path("output_schemas");
                for (JsonNode outputSchema : outputSchemas) {
                    JsonNode outputSchemaId = outputSchema.path("id");
                    System.out.println(outputSchemaId.asInt());

                    JsonNode inputSchema = outputSchema.path("input_schema_id");
                    inputSchemaId = inputSchema.asText();
                }
            }

            JsonNode errors = results.findValue("error_count");
            int errorCount = errors.asInt();
            log.logBasic("Error count: " + errorCount);
            if (errorCount > 0) {
                throw new IOException("Errors detected in source file upload");
            }

            JsonNode rowErrors = results.findValue("row_errors");
            rowErrorPath = rowErrors.asText().replace("{input_schema_id}", inputSchemaId);
            log.logDebug(rowErrorPath);

            JsonNode latestOutput = results.findValue("latest_output");
            latestOutputPath = latestOutput.asText().replace("{input_schema_id}", inputSchemaId);
            log.logDebug(latestOutputPath);
        }
    }

    private void createRevisionFromConfig() throws Exception {

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

    private void applyRevision() throws Exception {
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

    private void getLatestOutput() throws Exception {

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

    private void createImportConfig(SocrataPluginMeta meta) throws Exception {
        String newImportConfigName = datasetId + ":" + LocalDateTime.now();
        String dataAction;
        if(writerMode.equalsIgnoreCase("upsert")) {
            dataAction = "update";
        } else if (writerMode.equalsIgnoreCase("replace")) {
            dataAction = "replace";
        } else {
            throw new IOException("Invalid publishing data action");
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.put("name", newImportConfigName);
        ObjectNode parseOptions = mapper.createObjectNode();
        parseOptions.put("quote_char", "\"");
        parseOptions.put("header_count", 1);
        parseOptions.put("encoding", "utf8");
        parseOptions.put("column_separator", ",");
        parseOptions.put("column_header", 1);
        root.put("parse_options", parseOptions);
        root.put("data_action", dataAction);

        ArrayNode columns = mapper.createArrayNode();
        for (SocrataTextFileField field : outputFields) {
            String transformExpr = "";
            String dataType = field.getTypeDesc();
            switch (dataType) {
                case "Number":
                    transformExpr = "to_number(`" + field.getFieldName() + "`)";
                    break;
                case "String":
                    transformExpr = "to_text(`" + field.getFieldName() + "`)";
                    break;
                case "Location":
                    break;
                case "Boolean":
                    transformExpr = "to_boolean(`" + field.getFieldName() + "`)";
                    break;
                case "Date":
                    break;
                case "Integer":
                    transformExpr = "to_number(`" + field.getFieldName() + "`)";
                    break;
                case "Point":
                    break;
                case "Timestamp":
                    transformExpr = "to_fixed_timestamp(`" + field.getFieldName() + "`)";
                    break;
                case "BigNumber":
                    transformExpr = "to_number(`" + field.getFieldName() + "`)";
                    break;
                default:
                    log.logError("Unrecognized column type: " + field.getName());
            }

            ObjectNode column = mapper.createObjectNode();
            column.put("display_name", field.getName());
            column.put("field_name", field.getFieldName());
            column.put("transform_expr", transformExpr);
            columns.add(column);
        }
        root.put("columns", columns);

        String url = domain + "/api/publishing/v1/config";
        PostMethod httpPost = SocrataPublishUtil.getPost(url, host, authorize, "application/json");
        StringRequestEntity stringEntity = new StringRequestEntity(root.toString(), "application/json", "UTF-8");
        httpPost.setRequestEntity(stringEntity);
        JsonNode result = SocrataPublishUtil.execute(httpPost, log);
        log.logDebug(result.asText());

        SocrataPluginMeta updated = (SocrataPluginMeta) meta.clone();
        updated.setImportConfig(newImportConfigName);
        meta.replaceMeta(updated);
    }
}
