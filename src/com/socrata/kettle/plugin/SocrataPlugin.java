package com.socrata.kettle.plugin;

import com.socrata.api.SodaImporter;
import com.socrata.datasync.DatasetUtils;
import com.socrata.datasync.PublishMethod;
import com.socrata.datasync.config.controlfile.ControlFile;
import com.socrata.datasync.job.IntegrationJob;
import com.socrata.datasync.config.userpreferences.UserPreferencesLib;
import com.socrata.datasync.job.JobStatus;
import com.socrata.model.importer.Column;
import com.socrata.model.importer.Dataset;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.jsoninput.JsonReader;
import org.pentaho.di.ui.spoon.Spoon;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * @author malindac
 * @version 1.0
 * 2015-07-23
 */
public class SocrataPlugin extends BaseStep implements StepInterface {

    private SocrataPluginData data;
    private SocrataPluginMeta meta;
    private byte[] binarySeparator = environmentSubstitute(",").getBytes();
    private byte[] binaryEnclosure = environmentSubstitute("\"").getBytes();
    private byte[] binaryNewline = environmentSubstitute(System.getProperty("line.separator")).getBytes();
    private byte[][] binaryNullValue;
    private File filename;
    private UserPreferencesLib userPrefs;
    private String appToken = "Eu1EiCmxZN4DWT9UmiXCLQpl6";
    private List<String> fieldNames;
    private List<String> names;
    private Set<String> ignoreColumns;

    public SocrataPlugin(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
        super(s, stepDataInterface, c, t, dis);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (SocrataPluginMeta) smi;
        data = (SocrataPluginData) sdi;

        // Write out file as a csv in the temp directory
        boolean result = true;
        Object[] r = getRow();

        String writerMode = meta.getWriterMode();

        if (first) {
            first = false;
            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
            openNewFile();
            if (!writerMode.equalsIgnoreCase("create")) {
                getDatasetInfo();
            }

            data.fieldnrs = new int[meta.getOutputFields().length];
            for (int i = 0; i < meta.getOutputFields().length; i++) {
                data.fieldnrs[i] = data.outputRowMeta.indexOfValue(meta.getOutputFields()[i].getName());
                if (data.fieldnrs[i] < 0) {
                    throw new KettleStepException( "Field [" + meta.getOutputFields()[i].getName()
                            + "] could not be found in the input stream!");
                }
            }

            writeHeader();
        }

        if (r == null) {
            logDebug("Last row of the file has processed");
            // Publish temp file using DataSync
            if (writerMode.equalsIgnoreCase("create")) {
                createDataset();
            } else {
                sendToDatasync(meta.getDatasetName(), meta.getWriterMode());
            }
            setOutputDone();
            return false;
        }

        writeRowtoFile(data.outputRowMeta, r);



        return result;
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (SocrataPluginMeta) smi;
        data = (SocrataPluginData) sdi;

        if (super.init(smi, sdi)) {
            try {
                binaryNullValue = new byte[meta.getOutputFields().length][];
                for (int i = 0; i < meta.getOutputFields().length; i++) {
                    binaryNullValue[i] = null;
                    String nullString = meta.getOutputFields()[i].getNullString();
                    if (!Const.isEmpty(nullString)) {
                        binaryNullValue[i] = nullString.getBytes();
                    }
                }
            } catch (Exception e) {
                logError("Couldn't initialize binary data fields", e);
                setErrors(1);
                stopAll();
            }

            return true;

        }

        return false;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (SocrataPluginMeta) smi;
        data = (SocrataPluginData) sdi;

        closeFile();

        super.dispose(smi, sdi);
    }

    private void openNewFile() throws KettleException {
        data.writer = null;
        try {
            String tempDir = System.getProperty("java.io.tmpdir");
            File dir = new File(tempDir);
            filename = File.createTempFile("tempLoad", ".csv", dir);
            logBasic("Temp file location: " + filename.toString());
            data.fos = new FileOutputStream(filename);
            data.writer = new BufferedOutputStream(data.fos, 5000);
        } catch (Exception e) {
            throw new KettleException("Error opening new file: " + e.toString());
        }
    }

    private boolean closeFile() {
        boolean retval = false;

        try {
            if (data.writer != null) {
                if (log.isDebug()) {
                    logDebug("Closing output stream");
                }
                data.writer.close();
                if (log.isDebug()) {
                    logDebug("Closed output stream");
                }
                data.writer = null;
            }
            if (data.fos != null) {
                if (log.isDebug()) {
                    logDebug("Closing normal file");
                }
                data.fos.close();
                data.fos = null;
            }

            retval = true;
        } catch (Exception e) {
            logError("Exception trying to close file: " + e.toString());
            setErrors(1);
            retval = false;
        }

        return retval;
    }

    private void writeHeader() throws KettleStepException {
        RowMetaInterface r = data.outputRowMeta;

        try {
            if (meta.getOutputFields() != null && meta.getOutputFields().length > 0) {
                for (int i = 0; i < meta.getOutputFields().length; i++) {
                    String fieldName = "";
                    if (meta.getWriterMode().equalsIgnoreCase("create")) {
                        fieldName = meta.getOutputFields()[i].getName();
                    } else {
                        fieldName = meta.getOutputFields()[i].getFieldName();
                    }

                    if ( i > 0) {
                        data.writer.write(binarySeparator);
                    }

                    data.writer.write(fieldName.getBytes());
                }
                data.writer.write(binaryNewline);
            }

        } catch (Exception e) {
            throw new KettleStepException("Unable to write header", e);
        }
    }

    private void writeRowtoFile(RowMetaInterface rowMeta, Object[] r) throws KettleStepException {
        try {
            if (meta.getOutputFields() == null || meta.getOutputFields().length == 0) {
                // Write all values in stream to text file
                for (int i = 0; i < rowMeta.size(); i++) {
                    if (i > 0 && binarySeparator.length > 0) {
                        data.writer.write(binarySeparator);
                    }
                    ValueMetaInterface v = rowMeta.getValueMeta(i);
                    Object valueData = r[i];

                    // No special null value default was specified since no fields are specified at all
                    // As such, we pass null
                    writeField(v, valueData, null);
                }
                data.writer.write(binaryNewline);
            } else {
                // Only write the fields specified
                for (int i = 0; i < meta.getOutputFields().length; i++) {
                    if (i > 0) {
                        data.writer.write(binarySeparator);
                    }

                    ValueMetaInterface v = rowMeta.getValueMeta(data.fieldnrs[i]);
                    Object valueData = r[data.fieldnrs[i]];
                    writeField(v, valueData, binaryNullValue[i]);
                }
                data.writer.write(binaryNewline);
            }

            incrementLinesOutput();

        } catch (Exception e) {
            throw new KettleStepException("Error writing line", e);
        }
    }

    private void writeField(ValueMetaInterface v, Object valueData, byte[] nullString) throws KettleStepException {
        try {
            byte[] str;

            // First check whether or not we have a null string set
            // These values should be set when a null value passes
            if (nullString != null && v.isNull(valueData)) {
                str = nullString;
            } else {
                str = formatField(v, valueData);
            }

            if (str != null && str.length > 0) {
                List<Integer> enclosures = null;
                boolean writeEnclosures = false;

                if (v.isString() && containsSeparatorOrEnclosure(str, binarySeparator, binaryEnclosure)) {
                    writeEnclosures = true;
                }

                if (writeEnclosures) {
                    data.writer.write(binaryEnclosure);
                    enclosures = getEnclosurePositions(str);
                }

                if (enclosures == null) {
                    data.writer.write(str);
                } else {
                    int from = 0;
                    for (int position : enclosures) {
                        data.writer.write(str, from, position + binaryEnclosure.length - from);
                        data.writer.write(binaryEnclosure); // write enclosure a second time
                        from = position + binaryEnclosure.length;
                    }
                    if (from < str.length) {
                        data.writer.write(str, from, str.length - from);
                    }
                }

                if (writeEnclosures) {
                    data.writer.write(binaryEnclosure);
                }
            }
        } catch (Exception e) {
            throw new KettleStepException("Error writing field content to file", e);
        }
    }

    private byte[] formatField(ValueMetaInterface v, Object valueData) throws KettleValueException {
        if (v.isString()) {
            if (v.isStorageBinaryString() && v.getTrimType() == ValueMetaInterface.TRIM_TYPE_NONE && v.getLength() < 0
                    && Const.isEmpty( v.getStringEncoding())) {
                return (byte[]) valueData;
            } else {
                String svalue = (valueData instanceof String) ? (String) valueData : v.getString(valueData);

                // trim or cut to size if needed.
                return convertStringToBinaryString(v, Const.trimToType(svalue, v.getTrimType()));
            }
        } else {
            return v.getBinaryString(valueData);
        }
    }

    private byte[] convertStringToBinaryString(ValueMetaInterface v, String string) throws KettleValueException {
        int length = v.getLength();

        if (string == null) {
            return new byte[] {};
        }

        if (length > -1 && length < string.length()) {
            // we need to truncate
            String tmp = string.substring(0, length);
            if (Const.isEmpty(v.getStringEncoding())) {
                return tmp.getBytes();
            } else {
                try {
                    return tmp.getBytes(v.getStringEncoding());
                } catch (UnsupportedEncodingException e) {
                    throw new KettleValueException("Unable to convert String to Binary with specified string encoding ["
                            + v.getStringEncoding() + "]", e);
                }
            }
        } else {
            byte[] text;
            if (Const.isEmpty(v.getStringEncoding())) {
                text = string.getBytes();
            } else {
                try {
                    text = string.getBytes(v.getStringEncoding());
                } catch (UnsupportedEncodingException e) {
                    throw new KettleValueException("Unable to convert String to Binary with specified string encoding ["
                            + v.getStringEncoding() + "]", e);
                }
            }
            if (length > string.length()) {
                // we need to pad this
                int size = 0;
                byte[] filler = null;
                try {
                    filler = " ".getBytes();
                    size = text.length + filler.length * (length - string.length());
                } catch (Exception e) {
                    throw new KettleValueException(e);
                }
                byte[] bytes = new byte[size];
                System.arraycopy(text, 0, bytes, 0, text.length);
                if (filler.length == 1) {
                    java.util.Arrays.fill(bytes, text.length, size, filler[0]);
                } else {
                    int currIndex = text.length;
                    for (int i = 0; i < (length - string.length()); i++) {
                        for (int j = 0; j < filler.length; j++) {
                            bytes[currIndex++] = filler[j];
                        }
                    }
                }
                return bytes;
            } else {
                // do not need to pad or truncate
                return text;
            }
        }
    }

    public boolean containsSeparatorOrEnclosure( byte[] source, byte[] separator, byte[] enclosure ) {
        boolean result = false;

        boolean enclosureExists = enclosure != null && enclosure.length > 0;
        boolean separatorExists = separator != null && separator.length > 0;

        // Skip entire test if neither separator nor enclosure exist
        if (separatorExists || enclosureExists) {

            // Search for the first occurrence of the separator or enclosure
            for (int index = 0; !result && index < source.length; index++) {
                if (enclosureExists && source[index] == enclosure[0]) {

                    // Potential match found, make sure there are enough bytes to support a full match
                    if (index + enclosure.length <= source.length) {
                        // First byte of enclosure found
                        result = true; // Assume match
                        for (int i = 1; i < enclosure.length; i++) {
                            if (source[index + i] != enclosure[i]) {
                                // Enclosure match is proven false
                                result = false;
                                break;
                            }
                        }
                    }

                } else if (separatorExists && source[index] == separator[0]) {

                    // Potential match found, make sure there are enough bytes to support a full match
                    if (index + separator.length <= source.length) {
                        // First byte of separator found
                        result = true; // Assume match
                        for (int i = 1; i < separator.length; i++) {
                            if (source[index + i] != separator[i]) {
                                // Separator match is proven false
                                result = false;
                                break;
                            }
                        }
                    }

                }
            }

        }

        return result;
    }

    private List<Integer> getEnclosurePositions(byte[] str) {
        List<Integer> positions = null;
        // +1 because otherwise we will not find it at the end
        for (int i = 0, len = str.length - binaryEnclosure.length + 1; i < len; i++) {
            // verify if on position i there is an enclosure
            boolean found = true;
            for (int x = 0; found && x < binaryEnclosure.length; x++) {
               if (str[i+x] != binaryEnclosure[x]) {
                   found = false;
               }
            }
            if (found) {
                if (positions == null) {
                    positions = new ArrayList<Integer>();
                }
                positions.add(i);
            }
        }
        return positions;
    }

    private void setUserPrefencesLib() {
        userPrefs = new UserPreferencesLib();
        userPrefs.setDomain(meta.getDomain());
        userPrefs.setUsername(meta.getUser());
        userPrefs.setPassword(meta.getPassword());
        userPrefs.setAppToken(appToken);
        if (meta.getProxyHost() != null && !meta.getProxyHost().equalsIgnoreCase("")) {
            userPrefs.setProxyHost(meta.getProxyHost());
            userPrefs.setProxyPort(meta.getProxyPort());
        }
        if (meta.getProxyUsername() != null && !meta.getProxyUsername().equalsIgnoreCase("")) {
            userPrefs.setProxyUsername(meta.getProxyUsername());
        }
        if (meta.getProxyPassword() != null && !meta.getProxyPassword().equalsIgnoreCase("")) {
            userPrefs.setProxyPassword(meta.getProxyPassword());
        }

        userPrefs.load();
    }

    private void getDatasetInfo() throws KettleException {
        if (userPrefs == null) {
            setUserPrefencesLib();
        }

        fieldNames = new ArrayList<String>();
        names = new ArrayList<String>();

        try {
            logDebug("Getting Dataset Info");
            Dataset dataset = DatasetUtils.getDatasetInfo(userPrefs, meta.getDatasetName());
            List<Column> columns = dataset.getColumns();
            for (Column c : columns) {
                fieldNames.add(c.getFieldName());
                names.add(c.getName());
            }
            logDebug("API Field Names Size: " + fieldNames.size());
            logDebug("Readable Names Size: " + names.size());
            Set<String> flowFieldNames = new LinkedHashSet<String>();
            Set<String> flowNames = new LinkedHashSet<String>();

            for (int i = 0; i < meta.getOutputFields().length; i++) {
                String fieldName = meta.getOutputFields()[i].getFieldName();
                String name = meta.getOutputFields()[i].getName();
                if (fieldName != null && !fieldName.isEmpty()) {
                    flowFieldNames.add(fieldName);
                }
                if (!name.isEmpty()) {
                    flowNames.add(name);
                }
                if (fieldName == null) {
                    int index = names.indexOf(name);

                    if (index < 0) {
                        index = fieldNames.indexOf(name);
                    }

                    if (index != -1) {
                        meta.getOutputFields()[i].setFieldName(fieldNames.get(index));
                    } else {
                        meta.getOutputFields()[i].setFieldName(meta.getOutputFields()[i].getName().replace(" ", "_"));
                    }
                }
            }

            logDebug("Determining field name mapping and ignore columns");
            // Start with assuming that flowNames contains human readable field names
            ignoreColumns = new HashSet<String>(flowNames);
            ignoreColumns.removeAll(names);

            if (ignoreColumns.size() == flowNames.size()) {
                // Nothing was removed so flowNames must contain api field names
                ignoreColumns.removeAll(fieldNames);
                logDebug("Number of Columns to Ignore: " + ignoreColumns.size());
                if (ignoreColumns.size() == flowNames.size()) {
                    // ERROR
                    logError("No matching field names present");
                }
            }

            for (String s : ignoreColumns) {
                ignoreColumns.remove(s);
                logBasic("IGNORING COLUMN: " + s);

                String updated = s.replace(" ", "_");
                ignoreColumns.add(updated);
            }
        } catch (Exception e) {
            throw new KettleException("Error getting dataset information", e);
            //logError(e.getMessage());
        }
    }

    private void sendToDatasync(String datasetId, String writerMode) throws KettleStepException {
        // Use DataSync to publish file
        // First close the file
        logDebug("Closing File");
        closeFile();

        if (userPrefs == null) {
            logDebug("User Prefs were not set.  Setting now ....");
            setUserPrefencesLib();
        }


        try {
            logDebug("Beginning Send to DataSync");
            PublishMethod publishMethod = PublishMethod.valueOf(writerMode.toLowerCase());

            ControlFile controlFile = ControlFile.generateControlFile(filename.toString(), publishMethod,
                    null, meta.isUseSocrataGeocoding());

            Set<String> formats = new LinkedHashSet<String>();
            for (SocrataTextFileField field : meta.getOutputFields()) {
                if (field.getTypeDesc().equalsIgnoreCase("Date")) {
                    if (field.getFieldName() != null && !field.getFieldName().isEmpty()) {
                        formats.add(field.getFormat());
                        logDebug("Date Format: " + field.getFormat());
                    }
                }
            }
            logDebug("Number of date formats: " + formats.size());
            if (formats.size() > 0) {
                controlFile.csv.fixedTimestampFormat = formats.toArray(new String[formats.size()]);
                controlFile.csv.floatingTimestampFormat = formats.toArray(new String[formats.size()]);
            }

            if (!ignoreColumns.isEmpty()) {
                logDebug("Setting Columns to Ignore");
                controlFile.csv.ignoreColumns(ignoreColumns.toArray(new String[ignoreColumns.size()]));
            }
            logDebug("ControlFile created");
            IntegrationJob job = new IntegrationJob(userPrefs);
            job.setDatasetID(datasetId);
            job.setFileToPublish(filename.toString());
            job.setPublishMethod(publishMethod);
            job.setPublishViaDi2Http(true);
            job.setFileToPublishHasHeaderRow(true);
            job.setControlFile(controlFile);
            logBasic("DataSync job created");
            // Redirect System.out to log for DataSync details to appear
            PrintStream ps = createLoggingProxy(System.out);
            System.setOut(ps);
            JobStatus status = job.run();

            if (status.isError()) {
                throw new KettleStepException("DataSync job failed: " + status.getMessage());
            }

            logBasic("Job Status: " + status.getMessage());
            System.setOut(System.out);
            ps.close();
        } catch (Exception e) {
            logError(e.getMessage());
            throw new KettleStepException("DataSync Job Failed");
        }
    }

    private PrintStream createLoggingProxy(final PrintStream original) {
        return new PrintStream(original) {
            public void print(final String string) {
                original.print(string);
                logBasic(string);
            }
        };
    }

    private void createDataset() throws KettleStepException {
        try {
            String datasetId = "";

            String user = meta.getUser() + ":" + meta.getPassword();
            String auth = Base64.getEncoder().encodeToString(user.getBytes());

            String domain = "";
            String host = "";
            if (meta.getDomain().startsWith("https://")) {
                domain = meta.getDomain();
                host = meta.getDomain().replace("https://", "");
            } else if (meta.getDomain().startsWith("http://")){
                domain = meta.getDomain();
                host = meta.getDomain().replace("http://", "");
            } else {
                domain = "https://" + meta.getDomain();
                host = meta.getDomain();
            }

            CloseableHttpClient httpClient = HttpClients.createDefault();
            try {
                // TODO: detect Point type to set nbe=true
                HttpPost httpPost = new HttpPost(domain + "/api/views");
                httpPost.setHeader("Authorization", "Basic " + auth);
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setHeader("X-App-Token", appToken);
                httpPost.setHeader("X-Socrata-Host", host);


                StringEntity string = new StringEntity("{ \"name\": \"" + meta.getNewDatasetName() + "\" }", ContentType.APPLICATION_JSON);
                httpPost.setEntity(string);

                logDebug("Creating new dataset");

                CloseableHttpResponse response = httpClient.execute(httpPost);

                //logDebug(IOUtils.toString(response.getEntity().getContent()));

                JsonFactory factory = new JsonFactory();
                JsonParser parser = factory.createJsonParser(response.getEntity().getContent());
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldname = parser.getCurrentName();
                    if ("id".equalsIgnoreCase(fieldname)) {
                        parser.nextToken();
                        datasetId = parser.getText();
                        logBasic("New Dataset ID: " + datasetId);
                        break;
                    }
                }
                parser.close();
                response.close();

                if (!datasetId.isEmpty()) {
                    httpPost.setURI(new URI(domain + "/api/views/" + datasetId + "/columns"));

                    for (SocrataTextFileField field : meta.getOutputFields()) {
                        String name = field.getName();
                        String type = field.getTypeDesc().toLowerCase();
                        String fieldName = name.toLowerCase().replace(" ", "_");

                        if (type.equalsIgnoreCase("string")) {
                            type = "text";
                        } else if (type.equalsIgnoreCase("date") || type.equalsIgnoreCase("timestamp")) {
                            type = "calendar_date";
                        } else if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("bignumber")) {
                            type = "number";
                        } else if (type.equalsIgnoreCase("boolean")) {
                            type = "checkbox";
                        }

                        StringEntity newColumn = new StringEntity("{\"name\": \"" + name + "\",\"dataTypeName\": \"" + type + "\",\"fieldName\": \"" + fieldName + "\"}", ContentType.APPLICATION_JSON);
                        httpPost.setEntity(newColumn);

                        logDebug("Creating column: " + name);

                        response = httpClient.execute(httpPost);
                        logBasic("Created column: " + name + " Status: " + response.getStatusLine());

                        response.close();
                    }
                }

                // Publish Dataset
                if (meta.isPublishDataset()) {
                    httpPost.setURI(new URI(domain + "/api/views/" + datasetId + "/publication.json"));
                    httpPost.setEntity(null);
                    logDebug("Starting publish dataset");
                    response = httpClient.execute(httpPost);
                    logBasic("Publish status: " + response.getStatusLine());
                    response.close();
                }

                // Public Dataset
                if (meta.isPublicDataset()) {
                    HttpPut httpPut = new HttpPut(domain + "/api/views/" + datasetId + ".json?accessType=WEBSITE&method=setPermission&value=public.read");
                    httpPut.setHeader("Authorization", "Basic " + auth);
                    httpPut.setHeader("Content-Type", "application/json");
                    httpPut.setHeader("X-App-Token", appToken);
                    httpPut.setHeader("X-Socrata-Host", host);
                    logDebug("Beginning make dataset public");
                    response = httpClient.execute(httpPut);
                    logBasic("Public status: " + response.getStatusLine());
                    response.close();
                }

                httpClient.close();
            } finally {
                httpClient.close();
            }

            SocrataPluginMeta updated = (SocrataPluginMeta) meta.clone();
            updated.setDatasetName(datasetId);
            updated.setWriterMode("Upsert");
            meta.replaceMeta(updated);

            if(repository != null) {
                logBasic("Saving update to repository");
                meta.saveRep(repository, metaStore, getTrans().getObjectId(), getObjectId());
            } else {
                logBasic("Saving update to existing transformation file");
                getTransMeta().addOrReplaceStep(meta.getParentStepMeta());
                getTransMeta().writeXML(getTrans().getFilename());
            }

            getDatasetInfo();
            sendToDatasync(datasetId, "Upsert");
        } catch (Exception ex) {
            logError(ex.getMessage());
            throw new KettleStepException("Error creating dataset");
        }
    }
}
