package com.socrata.kettle.plugin;

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * @author malindac
 * @version 1.0
 * 2015-07-23
 */
public class SocrataPluginMeta extends BaseStepMeta implements StepMetaInterface {

    private String domain;
    private String user;
    private String password;
    private String datasetName;
    private boolean publishDataset;
    private boolean publicDataset;
    private String writerMode;
    private boolean useSocrataGeocoding;
    private SocrataTextFileField[] outputFields;
    private String proxyHost;
    private String proxyPort;
    private String proxyUsername;
    private String proxyPassword;

    protected String[] types = new String[]{"checkbox", "date",
            "email", "location", "money", "number", "percent", "stars", "text", "url"};

    public SocrataPluginMeta() {
        super();
    }

    public String getDomain() {

        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    protected String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public boolean isPublishDataset() {
        return publishDataset;
    }

    public void setPublishDataset(boolean publishDataset) {
        this.publishDataset = publishDataset;
    }

    public boolean isPublicDataset() {
        return publicDataset;
    }

    public void setPublicDataset(boolean publicDataset) {
        this.publicDataset = publicDataset;
    }

    public String getWriterMode() {
        return writerMode;
    }

    public void setWriterMode(String writerMode) {
        this.writerMode = writerMode;
    }

    public boolean isUseSocrataGeocoding() {
        return useSocrataGeocoding;
    }

    public void setUseSocrataGeocoding(boolean useSocrataGeocoding) {
        this.useSocrataGeocoding = useSocrataGeocoding;
    }

    public SocrataTextFileField[] getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(SocrataTextFileField[] outputFields) {
        this.outputFields = outputFields;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public String getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(String proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public void allocate(int nrfields) {
        outputFields = new SocrataTextFileField[nrfields];
    }

    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder(800);

        retval.append( "    " ).append(XMLHandler.addTagValue("domain", domain));
        retval.append( "    " ).append(XMLHandler.addTagValue("user", user));
        retval.append( "    " ).append(XMLHandler.addTagValue("password", password));
        retval.append( "    " ).append(XMLHandler.addTagValue("datasetName", datasetName));
        retval.append( "    " ).append(XMLHandler.addTagValue("publishDataset", publishDataset));
        retval.append( "    " ).append(XMLHandler.addTagValue("publicDataset", publicDataset));
        retval.append( "    " ).append(XMLHandler.addTagValue("writerMode", writerMode));
        retval.append( "    " ).append(XMLHandler.addTagValue("useSocrataGeocoding", useSocrataGeocoding));
        retval.append( "    " ).append(XMLHandler.addTagValue("proxyHost", proxyHost));
        retval.append( "    " ).append(XMLHandler.addTagValue("proxyPort", proxyPort));
        retval.append( "    " ).append(XMLHandler.addTagValue("proxyUsername", proxyUsername));
        retval.append( "    " ).append(XMLHandler.addTagValue("proxyPassword", proxyPassword));

        retval.append( "    <fields>" ).append( Const.CR );
        for (SocrataTextFileField field : outputFields) {

            if ( field.getName() != null && field.getName().length() != 0 ) {
                retval.append( "      <field>" ).append( Const.CR );
                retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getFormat() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "currency", field.getCurrencySymbol() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", field.getDecimalSymbol() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "group", field.getGroupingSymbol() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "nullif", field.getNullString() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "trim_type", field.getTrimTypeCode() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "length", field.getLength() ) );
                retval.append( "        " ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
                retval.append( "      </field>" ).append( Const.CR );
            }
        }
        retval.append( "    </fields>" ).append( Const.CR );

        return retval.toString();
    }

    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) {

        for (SocrataTextFileField field : outputFields) {
            ValueMetaInterface v = r.searchValueMeta( field.getName() );
            if ( v != null ) {
                v.setLength(field.getLength());
                v.setPrecision(field.getPrecision());
                v.setConversionMask(field.getFormat());
                v.setDecimalSymbol(field.getDecimalSymbol());
                v.setGroupingSymbol(field.getGroupingSymbol());
                v.setCurrencySymbol(field.getCurrencySymbol());
                v.setTrimType( field.getTrimType() );

                // enable output padding by default to be compatible with v2.5.x
                //
                v.setOutputPaddingEnabled( true );
            }
        }
    }

    public Object clone() {
        SocrataPluginMeta retval = (SocrataPluginMeta) super.clone();
        int nrfields = outputFields.length;

        retval.allocate(nrfields);

        for (int i = 0; i < nrfields; i++) {
            retval.outputFields[i] = (SocrataTextFileField) outputFields[i].clone();
        }

        return retval;
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        try {
            domain = XMLHandler.getTagValue(stepnode, "domain");
            if (domain == null) {
                domain = "";
            }
            user = XMLHandler.getTagValue(stepnode, "user");
            if (user == null) {
                user = "";
            }
            password = XMLHandler.getTagValue(stepnode, "password");
            if (password == null) {
                password = "";
            }
            datasetName = XMLHandler.getTagValue(stepnode, "datasetName");
            if (datasetName == null) {
                datasetName = "";
            }
            publishDataset = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "publishDataset"));
            publicDataset = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "publicDataset"));
            writerMode = XMLHandler.getTagValue(stepnode, "writerMode");
            if (writerMode == null) {
                writerMode = "";
            }
            useSocrataGeocoding = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "useSocrataGeocoding"));
            proxyHost = XMLHandler.getTagValue(stepnode, "proxyHost");
            if (proxyHost == null) {
                proxyHost = "";
            }
            proxyPort = XMLHandler.getTagValue(stepnode, "proxyPort");
            if (proxyPort == null) {
                proxyPort = "";
            }
            proxyUsername = XMLHandler.getTagValue(stepnode, "proxyUsername");
            if (proxyUsername == null) {
                proxyUsername = "";
            }
            proxyPassword = XMLHandler.getTagValue(stepnode, "proxyPassword");
            if (proxyPassword == null) {
                proxyPassword = "";
            }

            Node fields = XMLHandler.getSubNode(stepnode, "fields");
            int nrfields = XMLHandler.countNodes(fields, "field");

            allocate(nrfields);

            for (int i = 0; i < nrfields; i++) {
                Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i);

                outputFields[i] = new SocrataTextFileField();
                outputFields[i].setName("field" + i);
                outputFields[i].setName(XMLHandler.getTagValue(fnode, "name"));
                outputFields[i].setType(XMLHandler.getTagValue(fnode, "type"));
                outputFields[i].setFormat(XMLHandler.getTagValue(fnode, "format"));
                outputFields[i].setCurrencySymbol(XMLHandler.getTagValue(fnode, "currency"));
                outputFields[i].setDecimalSymbol(XMLHandler.getTagValue(fnode, "decimal"));
                outputFields[i].setGroupingSymbol(XMLHandler.getTagValue(fnode, "group"));
                outputFields[i].setTrimType(ValueMeta.getTrimTypeByCode(XMLHandler.getTagValue(fnode, "trim_type")));
                outputFields[i].setNullString(XMLHandler.getTagValue(fnode, "nullif"));
                outputFields[i].setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
                outputFields[i].setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
            }



        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    public void setDefault() {
        domain = "";
        user = "";
        password = "";
        datasetName = "";
        publishDataset = false;
        publicDataset = false;
        writerMode = "";
        useSocrataGeocoding = false;
        proxyHost = "";
        proxyPort = "";
        proxyUsername = "";
        proxyPassword = "";

        int nrfields = 0;

        allocate(nrfields);

        for (int i = 0; i < nrfields; i++) {
            outputFields[i] = new SocrataTextFileField();

            outputFields[i].setName( "field" + i );
            outputFields[i].setType( "Number" );
            outputFields[i].setFormat( " 0,000,000.00;-0,000,000.00" );
            outputFields[i].setCurrencySymbol( "" );
            outputFields[i].setDecimalSymbol( "," );
            outputFields[i].setGroupingSymbol( "." );
            outputFields[i].setNullString( "" );
            outputFields[i].setLength( -1 );
            outputFields[i].setPrecision( -1 );
        }

    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId idStep, List<DatabaseMeta> databases) throws KettleException {
        try
        {
            domain = rep.getStepAttributeString(idStep, "domain");
            user = rep.getStepAttributeString(idStep, "user");
            password = rep.getStepAttributeString(idStep, "password");
            datasetName = rep.getStepAttributeString(idStep, "datasetName");
            publishDataset = rep.getStepAttributeBoolean(idStep, "publishDataset");
            publicDataset = rep.getStepAttributeBoolean(idStep, "publicDataset");
            writerMode = rep.getStepAttributeString(idStep, "writerMode");
            useSocrataGeocoding = rep.getStepAttributeBoolean(idStep, "useSocrataGeocoding");
            proxyHost = rep.getStepAttributeString(idStep, "proxyHost");
            proxyPort = rep.getStepAttributeString(idStep, "proxyPort");
            proxyUsername = rep.getStepAttributeString(idStep, "proxyUsername");
            proxyPassword = rep.getStepAttributeString(idStep, "proxyPassword");

            int nrfields = rep.countNrStepAttributes(idStep, "field_name");

            allocate( nrfields );

            for ( int i = 0; i < nrfields; i++ ) {
                outputFields[i] = new SocrataTextFileField();

                outputFields[i].setName( rep.getStepAttributeString( idStep, i, "field_name" ) );
                outputFields[i].setType( rep.getStepAttributeString( idStep, i, "field_type" ) );
                outputFields[i].setFormat( rep.getStepAttributeString( idStep, i, "field_format" ) );
                outputFields[i].setCurrencySymbol( rep.getStepAttributeString( idStep, i, "field_currency" ) );
                outputFields[i].setDecimalSymbol( rep.getStepAttributeString( idStep, i, "field_decimal" ) );
                outputFields[i].setGroupingSymbol( rep.getStepAttributeString( idStep, i, "field_group" ) );
                outputFields[i].setTrimType( ValueMeta.getTrimTypeByCode( rep.getStepAttributeString( idStep, i,
                        "field_trim_type" ) ) );
                outputFields[i].setNullString( rep.getStepAttributeString( idStep, i, "field_nullif" ) );
                outputFields[i].setLength( (int) rep.getStepAttributeInteger( idStep, i, "field_length" ) );
                outputFields[i].setPrecision( (int) rep.getStepAttributeInteger( idStep, i, "field_precision" ) );
            }
        }
        catch(KettleDatabaseException dbe)
        {
            throw new KettleException("error reading step with id_step=" + idStep + " from the repository", dbe);
        }
        catch(Exception e)
        {
            throw new KettleException("Unexpected error reading step with id_step=" + idStep + " from the repository", e);
        }
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId idTransformation, ObjectId idStep) throws KettleException {
        try
        {
            rep.saveStepAttribute(idTransformation, idStep, "domain", domain);
            rep.saveStepAttribute(idTransformation, idStep, "user", user);
            rep.saveStepAttribute(idTransformation, idStep, "password", password);
            rep.saveStepAttribute(idTransformation, idStep, "datasetName", datasetName);
            rep.saveStepAttribute(idTransformation, idStep, "publishDataset", publishDataset);
            rep.saveStepAttribute(idTransformation, idStep, "publicDataset", publicDataset);
            rep.saveStepAttribute(idTransformation, idStep, "writerMode", writerMode);
            rep.saveStepAttribute(idTransformation, idStep, "useSocrataGeocoding", useSocrataGeocoding);
            rep.saveStepAttribute(idTransformation, idStep, "proxyHost", proxyHost);
            rep.saveStepAttribute(idTransformation, idStep, "proxyPort", proxyPort);
            rep.saveStepAttribute(idTransformation, idStep, "proxyUsername", proxyUsername);
            rep.saveStepAttribute(idTransformation, idStep, "proxyPassword", proxyPassword);

            for ( int i = 0; i < outputFields.length; i++ ) {
                SocrataTextFileField field = outputFields[i];

                rep.saveStepAttribute( idTransformation, idStep, i, "field_name", field.getName() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_type", field.getTypeDesc() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_format", field.getFormat() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_currency", field.getCurrencySymbol() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_decimal", field.getDecimalSymbol() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_group", field.getGroupingSymbol() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_trim_type", field.getTrimTypeCode() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_nullif", field.getNullString() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_length", field.getLength() );
                rep.saveStepAttribute( idTransformation, idStep, i, "field_precision", field.getPrecision() );
            }
        }
        catch(KettleDatabaseException dbe)
        {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[],
                      String output[], RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {
        CheckResult cr;
        if (prev==null || prev.size()==0)
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta);
            remarks.add(cr);
        }
        else
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving "+prev.size()+" fields", stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length>0)
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta);
            remarks.add(cr);
        }
        else
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
            remarks.add(cr);
        }
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new SocrataPluginDialog(shell, meta, transMeta, name);
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
        return new SocrataPlugin(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    public StepDataInterface getStepData() {
        return new SocrataPluginData();
    }

    public boolean supportsErrorHandling() {
        return true;
    }
}
