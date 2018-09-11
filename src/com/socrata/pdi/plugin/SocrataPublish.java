package com.socrata.pdi.plugin;

import org.apache.commons.httpclient.methods.*;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Base64;

/**
 * @author malindac
 * @version 1.0
 * 2017-09-20
 */
public class SocrataPublish {

    private LogChannelInterface log;
    private SocrataPublishUtil publishUtil;
    private String host;
    private String domain;
    private String authorize;
    private String datasetId;
    private String writerMode;
    private String errorFileLocation;
    private File file;
    private SocrataTextFileField[] outputFields;
    private SocrataPluginMeta meta;
    private boolean isCreate = false;

    private int revisionSeq;
    private String createSourcePath;
    private String applyRevisionPath;
    private String uploadDataPath;
    private String schemaErrorPath;
    private String latestOutputPath;
    private int outputSchemaId;

    public void publish(SocrataPluginMeta meta, File file, LogChannelInterface log, SocrataPublishUtil socrataPublishUtil)
            throws IOException, KettleStepException {
        this.log = log;
        this.publishUtil = socrataPublishUtil;
        host = publishUtil.setHost(meta);
        domain = meta.getDomain();
        String credentials = meta.getUser() + ":" + meta.getPassword();
        authorize = Base64.getEncoder().encodeToString(credentials.getBytes());
        datasetId = meta.getDatasetName();
        writerMode = meta.getWriterMode();
        errorFileLocation = meta.getErrorFileLocation();
        outputFields = meta.getOutputFields();
        this.file = file;
        this.meta = meta;

        if (writerMode.equalsIgnoreCase("Create")) {
            writerMode = "Update";
            isCreate = true;
        }

        createRevision();
        createSource(this.file.toString());
        uploadSourceData();
        getLatestOutput();
        applyRevision();
    }

    private void createRevision() throws IOException, KettleStepException {
        String url = domain + "/api/publishing/v1/revision/" + datasetId;
        PostMethod httpPost = publishUtil.getPost(url, host, authorize, "application/json");

        String permission;
        if (meta.isPublicDataset()) {
            permission = "public";
        } else {
            permission = "private";
        }

        String json = "{ \"action\": {\"type\": \"" + writerMode.toLowerCase() + "\", \"permission\": \"" + permission + "\" }}";
        StringRequestEntity string = new StringRequestEntity(json, "application/json", "UTF-8");

        httpPost.setRequestEntity(string);

        JsonNode results = publishUtil.execute(httpPost, log, meta);
        log.logBasic("Create revision status: " + httpPost.getStatusLine());

        //Get revision_seq value
        if (results != null) {

            JsonNode revisionSequence = results.path("resource").path("revision_seq");
            revisionSeq = revisionSequence.asInt();
            log.logDebug("New revision sequence: " + revisionSeq);

            JsonNode createSource = results.path("links").path("create_source");
            createSourcePath = createSource.asText();
            log.logDebug(createSourcePath);

            JsonNode apply = results.path("links").path("apply");
            applyRevisionPath = apply.asText();
            log.logDebug(applyRevisionPath);
        }
    }

    private void createSource(String filename) throws IOException, KettleStepException {

        PostMethod httpPost = publishUtil.getPost(domain + createSourcePath, host, authorize, "application/json");

        String json = "{ \"source_type\": {\"type\": \"upload\", \"filename\": \"" + filename + "\" }}";
        StringRequestEntity string = new StringRequestEntity(json, "application/json", "UTF-8");
        log.logDebug(json);

        httpPost.setRequestEntity(string);
        JsonNode results = publishUtil.execute(httpPost, log, meta);
        log.logBasic("Create source status: " + httpPost.getStatusLine());

        if (results != null) {
            JsonNode bytes = results.findValue("bytes");
            uploadDataPath = bytes.asText();
            log.logDebug(uploadDataPath);
        }
    }

    private void uploadSourceData() throws IOException, KettleStepException {

        PostMethod httpPost = publishUtil.getPost(domain + uploadDataPath, host, authorize, "text/csv");
        FileRequestEntity fre = new FileRequestEntity(file, "text/csv");
        httpPost.setRequestEntity(fre);
        JsonNode results = publishUtil.execute(httpPost, log, meta);
        log.logBasic("Upload source status: " + httpPost.getStatusLine());

        if (results != null) {

            String inputSchemaId = "";
            JsonNode schemas = results.path("resource").path("schemas");

            for (JsonNode schema : schemas) {
                JsonNode inputSchema = schema.path("id");
                inputSchemaId = inputSchema.asText();
            }

            JsonNode latestOutput = results.findValue("latest_output");
            latestOutputPath = latestOutput.asText().replace("{input_schema_id}", inputSchemaId);
            log.logDebug(latestOutputPath);
        }
    }

    private void getLatestOutput() throws IOException, KettleStepException {

        JsonNode results = publishUtil.execute(
                publishUtil.get(domain + latestOutputPath, host, authorize, "application/json"), log, meta);
        log.logDebug(results.toString());

        if (outputSchemaId == 0) {
            JsonNode outputId = results.path("resource").path("id");
            outputSchemaId = outputId.asInt();
            log.logDebug("Latest output schema id: " + outputSchemaId);
        }

        JsonNode completedAt = results.path("resource").path("completed_at");
        log.logDebug("Latest completedAt value: " + completedAt.asText());
        // Wait until processing is complete
        while (completedAt.asText() == null || completedAt.asText().equalsIgnoreCase("null") || completedAt.asText().isEmpty()) {
            log.logBasic("Transformation processing ...");
            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                // do nothing
            }
            results = publishUtil.execute(
                    publishUtil.get(domain + latestOutputPath, host, authorize, "application/json"), log, meta);
            completedAt = results.path("resource").path("completed_at");
        }

        JsonNode schemaErrors = results.findValue("schema_errors");
        schemaErrorPath = schemaErrors.asText();
        log.logDebug(schemaErrorPath);

        if (isCreate) {
            createOutputSchema(results);
            isCreate = false;
            results = publishUtil.execute(
                    publishUtil.get(domain + latestOutputPath, host, authorize, "application/json"), log, meta);
            completedAt = results.path("resource").path("completed_at");

            JsonNode outputId = results.path("resource").path("id");
            outputSchemaId = outputId.asInt();
            log.logDebug("Latest output schema id: " + outputSchemaId);

            while (completedAt.asText() == null || completedAt.asText().equalsIgnoreCase("null") || completedAt.asText().isEmpty()) {
                log.logBasic("Transformation processing ...");
                try {
                    Thread.sleep(3000);
                } catch (Exception ex) {
                    // do nothing
                }
                results = publishUtil.execute(
                        publishUtil.get(domain + latestOutputPath, host, authorize, "application/json"), log, meta);
                completedAt = results.path("resource").path("completed_at");
            }
        }

        JsonNode errors = results.path("resource").path("error_count");
        int errorCount = errors.asInt();
        log.logBasic("Error count: " + errorCount);

        if (errorCount > 0) {
            if (errorFileLocation != null && !errorFileLocation.isEmpty()) {
                GetMethod getCsv = publishUtil.get(domain + schemaErrorPath, host, authorize, "text/csv");

                publishUtil.executeCsv(getCsv, log, meta);

                if (meta.isSetAsideErrors()) {
                    log.logBasic("Errors transforming data during publish. Error rows can be viewed here: "
                            + errorFileLocation);
                } else {
                    throw new KettleStepException("Errors transforming data during publish. Error rows can be viewed here: "
                            + errorFileLocation);
                }
            } else {
                if (meta.isSetAsideErrors()) {
                    log.logBasic("Errors transforming data during publish. Error rows have not been saved.");
                } else {
                    throw new KettleStepException("Errors transforming data during publish. Error rows can be viewed here: "
                            + domain + schemaErrorPath);
                }
            }
        }
    }

    private void applyRevision() throws IOException, KettleStepException {
        PutMethod httpPut = publishUtil.getPut(domain + applyRevisionPath, host, authorize, "application/json");

        String json = "{ \"output_schema_id\": " + outputSchemaId + " }";
        StringRequestEntity string = new StringRequestEntity(json, "application/json", "UTF-8");
        log.logDebug(json);

        httpPut.setRequestEntity(string);

        JsonNode results = publishUtil.execute(httpPut, log, meta);
        if(results != null) {
            JsonNode finishedAt = results.findValue("finished_at");
            String url = domain + "/api/publishing/v1/revision/" + datasetId + "/" + revisionSeq;
            while (finishedAt.asText() == null || finishedAt.asText().equalsIgnoreCase("null")
                    || finishedAt.asText().isEmpty()) {
                JsonNode statusNode = results.findValue("status");
                String status = statusNode.asText();
                log.logBasic("Current job status: " + status);

                GetMethod getStatus = publishUtil.get(url, host, authorize, "application/json");
                results = publishUtil.execute(getStatus, log, meta);
                finishedAt = results.path("resource").path("task_sets").path(0).path("finished_at");
                try {
                    Thread.sleep(3000);
                } catch (Exception ex) {
                    // do nothing
                }
            }
        }
    }

    private void createOutputSchema(JsonNode node) throws IOException, KettleStepException {
        //Create new output schema with Kettle data types
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        ArrayNode columns = mapper.createArrayNode();
        JsonNode outputColumns = node.path("resource").path("output_columns");
        for (JsonNode column : outputColumns) {
            ObjectNode col = mapper.createObjectNode();

            int position = column.path("position").asInt();
            col.put("position", position);

            String fieldName = column.path("field_name").asText();
            col.put("field_name", fieldName);

            String displayName = column.path("display_name").asText();
            col.put("display_name", displayName);

            SocrataTextFileField field = outputFields[position - 1];
            String transformExpr = "";
            String dataType = field.getTypeDesc();
            switch (dataType) {
                case "Number":
                    transformExpr = "to_number(`" + fieldName + "`)";
                    break;
                case "String":
                    transformExpr = "to_text(`" + fieldName + "`)";
                    break;
                case "Location":
                    transformExpr = "to_location(`" + fieldName + "`)";
                    break;
                case "Boolean":
                    transformExpr = "to_boolean(`" + fieldName + "`)";
                    break;
                case "Date":
                    transformExpr = "to_floating_timestamp(`" + fieldName + "`)";
                    break;
                case "Integer":
                    transformExpr = "to_number(`" + fieldName + "`)";
                    break;
                case "Point":
                    transformExpr = "to_point(`" + fieldName + "`)";
                    break;
                case "Timestamp":
                    transformExpr = "to_fixed_timestamp(`" + fieldName + "`)";
                    break;
                case "BigNumber":
                    transformExpr = "to_number(`" + fieldName + "`)";
                    break;
                default:
                    log.logError("Unrecognized column type: " + field.getName());
            }

            ObjectNode transform = mapper.createObjectNode();
            ArrayNode transformInputColumns = mapper.valueToTree(column.path("transform").path("transform_input_columns"));
            transform.put("transform_input_columns", transformInputColumns);
            transform.put("transform_expr", transformExpr);
            col.put("transform", transform);
            columns.add(col);
        }

        root.put("output_columns", columns);
        String outputJson = mapper.writeValueAsString(root);
        log.logDebug(outputJson);

        String url = domain + latestOutputPath.replace("/output/latest", "");
        PostMethod post = publishUtil.getPost(url, host, authorize, "application/json");
        StringRequestEntity requestEntity = new StringRequestEntity(outputJson, "application/json", "UTF-8");
        post.setRequestEntity(requestEntity);

        try {
            publishUtil.execute(post, log, meta);
            log.logBasic("Create output schema status: " + post.getStatusLine());
        } catch (KettleStepException ex) {
            throw new KettleStepException("Unable to update initial output schema. The request made had invalid values.");
        }
    }
}
