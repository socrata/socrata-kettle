package com.socrata.pdi.plugin;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jackson.JsonNode;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.*;
import java.nio.file.Files;
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
            if (meta.getOutputFields().length == 0) {
                throw new KettleStepException("No output fields selected. Please complete the Fields tab.");
            }
            // Publish temp file
            if (writerMode.equalsIgnoreCase("create")) {
                createDataset();
            } else {
                publishData();
            }

            if (meta.isDeleteTempFile()) {
                //Delete temp file
                try {
                    Files.delete(filename.toPath());
                } catch (IOException ex) {
                    logError(ex.getMessage());
                    throw new KettleStepException("Error deleting temp csv file");
                }
            }
            setOutputDone();
            return false;
        }

        writeRowtoFile(data.outputRowMeta, r);

        return true;
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
        boolean retval;

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

        try {
            if (meta.getOutputFields() != null && meta.getOutputFields().length > 0) {
                for (int i = 0; i < meta.getOutputFields().length; i++) {
                    String fieldName;
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
                int size;
                byte[] filler;
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
                        for (byte b : filler) {
                            bytes[currIndex++] = b;
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

    private boolean containsSeparatorOrEnclosure( byte[] source, byte[] separator, byte[] enclosure ) {
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
                    positions = new ArrayList<>();
                }
                positions.add(i);
            }
        }
        return positions;
    }

    private void getDatasetInfo() throws KettleException {

        fieldNames = new ArrayList<>();
        names = new ArrayList<>();

        try {
            logDebug("Getting Dataset Info");
            String user = meta.getUser() + ":" + meta.getPassword();
            String auth = Base64.getEncoder().encodeToString(user.getBytes());
            logDebug("User auth: " + auth);

            String host = SocrataPublishUtil.setHost(meta);
            String domain = meta.getDomain();
            logDebug("Host:" + host);
            logDebug("Domain: " + domain);

            String url = domain + "/api/views/" + meta.getDatasetName() + ".json";
            logDebug("Request URL: " + url);
            GetMethod get = SocrataPublishUtil.get(url, host, auth, "application/json");
            JsonNode response = SocrataPublishUtil.execute(get, log, meta);

            if (response != null) {
                JsonNode columns = response.path("columns");
                logDebug(columns.toString());
                for (JsonNode node : columns) {
                    logDebug("Each node? " + node.toString());
                    fieldNames.add(node.path("fieldName").asText().toLowerCase());
                    names.add(node.path("name").asText().toLowerCase());
                    logDebug("Adding fieldName: " + node.path("fieldName").asText() +
                            " and name: " + node.path("name").asText());
                }
            }

            logDebug("API Field Names Size: " + fieldNames.size());
            logDebug("Readable Names Size: " + names.size());
            Set<String> flowFieldNames = new LinkedHashSet<String>();
            Set<String> flowNames = new LinkedHashSet<String>();

            SocrataTextFileField[] fields = meta.getOutputFields();
            for (SocrataTextFileField field : fields) {
                logDebug(field.getName());
                logDebug(field.getFieldName());
            }

            for (int i = 0; i < fields.length; i++) {
                String fieldName = fields[i].getFieldName();
                String name = fields[i].getName().toLowerCase();
                if (fieldName != null && !fieldName.isEmpty()) {
                    fieldName = fieldName.toLowerCase();
                    flowFieldNames.add(fieldName);
                }
                if (!name.isEmpty()) {
                    flowNames.add(name.toLowerCase());
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
            ignoreColumns = new HashSet<>(flowNames);
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

    private void createDataset() throws KettleStepException {
        String datasetId = "";

        String user = meta.getUser() + ":" + meta.getPassword();
        String auth = Base64.getEncoder().encodeToString(user.getBytes());

        String host = SocrataPublishUtil.setHost(meta);
        String domain = meta.getDomain();

        /*boolean isNbe = false;

        for (SocrataTextFileField field : meta.getOutputFields()) {
            if (field.getTypeDesc().equalsIgnoreCase("point")) {
                isNbe = true;
            }
        }*/

        try {
            /*String nbe = "";
            if (isNbe) {
                nbe = "?nbe=true";
            }*/

            String url = domain + "/api/views?nbe=true";
            PostMethod httpPost = SocrataPublishUtil.getPost(url, host, auth, "application/json");
            StringRequestEntity data = new StringRequestEntity("{\"name\": \"" + meta.getNewDatasetName() + "\"}",
                    "application/json", "UTF-8");
            httpPost.setRequestEntity(data);

            logDebug("Creating new dataset");

            JsonNode response = SocrataPublishUtil.execute(httpPost, log, meta);
            logBasic("Create datatset status: " + httpPost.getStatusLine());
            httpPost.releaseConnection();

            if (response != null) {
                JsonNode id = response.path("id");
                datasetId = id.asText();
                logBasic("New Dataset ID: " + datasetId);
            }

            if (!datasetId.isEmpty()) {
                for (SocrataTextFileField field : meta.getOutputFields()) {
                    field.setFieldName(field.getName().toLowerCase().replace(" ", "_"));
                }

                SocrataPluginMeta updatedMeta = (SocrataPluginMeta) meta.clone();
                updatedMeta.setDatasetName(datasetId);
                meta.replaceMeta(updatedMeta);

                publishData();
            }

            SocrataPluginMeta updated = (SocrataPluginMeta) meta.clone();
            updated.setWriterMode("Update");
            meta.replaceMeta(updated);

            if(repository != null) {
                logBasic("Saving update to repository");
                meta.saveRep(repository, metaStore, getTrans().getObjectId(), getObjectId());
            } else {
                logBasic("Saving update to existing transformation file");
                getTransMeta().addOrReplaceStep(meta.getParentStepMeta());
                getTransMeta().writeXML(getTrans().getFilename());
            }

        } catch (Exception ex) {
            logError(ex.getMessage());
            throw new KettleStepException("Error creating dataset");
        }
    }

    private void publishData() throws KettleStepException {
        // First close the file
        logDebug("Closing File");
        closeFile();

        logDebug("Number of output rows: " + getLinesOutput());

        SocrataPublish publish = new SocrataPublish();
        try {
            publish.publish(meta, filename, log);
        } catch (IOException e) {
            throw new KettleStepException("Error publishing data: " + e.getMessage());
        }
    }
}
