package com.socrata.kettle.plugin;

import org.apache.commons.httpclient.methods.GetMethod;
import org.codehaus.jackson.JsonNode;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.*;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import java.util.*;
import java.util.List;

/**
 * @author malindac
 * @version 1.0
 * 2015-07-23
 */
public class SocrataPluginDialog extends BaseStepDialog implements StepDialogInterface {

    private SocrataPluginMeta input;

    private Composite wSettingsComp;
    private Composite wFieldsComp;

    private CTabFolder wTabFolder;
    private FormData fdTabFolder;

    private CTabItem wSettingsTab;
    private CTabItem wFieldsTab;
    private FormData fdSettingsComp;
    private FormData fdFieldsComp;

    private Group wConnectionGroup;
    private FormData fdConnectionGroup;

    private FormData fdDomain;
    private FormData fdUserName;
    private FormData fdPassword;
    private LabelTextVar wDomain;
    private LabelTextVar wUserName;
    private LabelTextVar wPassword;

    private Group wParametersGroup;
    private FormData fdParametersGroup;

    private FormData fdlDatasetName;
    private FormData fdDatasetName;
    private FormData fdlPublishDataset;
    private FormData fdPublishDataset;
    private FormData fdlPublicDataset;
    private FormData fdPublicDataset;
    private FormData fdlWriterMode;
    private FormData fdWriterMode;
    private FormData fdlImportConfig;
    private FormData fdImportConfig;
    private FormData fdlNewDatasetName;
    private FormData fdNewDatasetName;
    private FormData fdlUseSocrataGeocoding;
    private FormData fdUseSocrataGeocoding;
    private FormData fdlDeleteTempFile;
    private FormData fdDeleteTempFile;
    private Label wlDatasetName;
    private TextVar wDatasetName;
    private Label wlPublishDataset;
    private Button wPublishDataset;
    private Label wlPublicDataset;
    private Button wPublicDataset;
    private Label wlWriterMode;
    private ComboVar wWriterMode;
    private Label wlImportConfig;
    private ComboVar wImportConfig;
    private Label wlNewDatasetName;
    private TextVar wNewDatasetName;
    private Label wlUseSocrataGeocoding;
    private Button wUseSocrataGeocoding;
    private Label wlDeleteTempFile;
    private Button wDeleteTempFile;

    private Group wProxyGroup;
    private FormData fdProxyGroup;

    private LabelTextVar wProxyHost;
    private FormData fdProxyHost;
    private LabelTextVar wProxyPort;
    private FormData fdProxyPort;
    private LabelTextVar wProxyUsername;
    private FormData fdProxyUsername;
    private LabelTextVar wProxyPassword;
    private FormData fdProxyPassword;

    private Button wMinWidth;
    private Listener lsMinWidth;

    private TableView wFields;
    private FormData fdFields;
    private ColumnInfo[] columnInfos;

    private Map<String, Integer> inputFields;


    public SocrataPluginDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta)in, transMeta, sname);
        input = (SocrataPluginMeta)in;
        inputFields = new HashMap<String, Integer>();
    }

    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                input.setChanged();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(Messages.getString("SocrataPluginDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(Messages.getString("SocrataPluginDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.top = new FormAttachment(0, margin);
        fdlStepname.right = new FormAttachment(middle, -margin);
        wlStepname.setLayoutData(fdlStepname);

        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);

        wTabFolder = new CTabFolder( shell, SWT.BORDER );
        props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
        wTabFolder.setSimple( false );

        // //////////////////////
        // /// Start SETTINGS TAB
        // //////////////////////
        wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
        wSettingsTab.setText(Messages.getString("SocrataPluginDialog.SettingsTab.TabTitle"));

        FormLayout settingsLayout = new FormLayout();
        settingsLayout.marginWidth = Const.FORM_MARGIN;
        settingsLayout.marginHeight = Const.FORM_MARGIN;

        wSettingsComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wSettingsComp);
        wSettingsComp.setLayout(settingsLayout);

        // *** START Connection GROUP *** //
        wConnectionGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
        props.setLook(wConnectionGroup);
        wConnectionGroup.setText(Messages.getString("SocrataPluginDialog.ConnectionGroup.Label"));

        FormLayout connectionGroupLayout = new FormLayout();
        connectionGroupLayout.marginWidth = 10;
        connectionGroupLayout.marginHeight = 10;
        wConnectionGroup.setLayout(connectionGroupLayout);

        // Domain
        wDomain = new LabelTextVar(transMeta, wConnectionGroup,
                Messages.getString("SocrataPluginDialog.Domain.Label"),
                Messages.getString("SocrataPluginDialog.Domain.Tooltip"));
        props.setLook(wDomain);
        wDomain.addModifyListener(lsMod);
        fdDomain = new FormData();
        fdDomain.left = new FormAttachment(0, 0);
        fdDomain.top = new FormAttachment(wStepname, margin);
        fdDomain.right = new FormAttachment(100, 0);
        wDomain.setLayoutData(fdDomain);

        // User
        wUserName = new LabelTextVar(transMeta, wConnectionGroup,
                Messages.getString("SocrataPluginDialog.User.Label"),
                Messages.getString("SocrataPluginDialog.User.Tooltip"));
        props.setLook(wUserName);
        wUserName.addModifyListener(lsMod);
        fdUserName = new FormData();
        fdUserName.left = new FormAttachment(0, 0);
        fdUserName.top = new FormAttachment(wDomain, margin);
        fdUserName.right = new FormAttachment(100, 0);
        wUserName.setLayoutData(fdUserName);

        // Password
        wPassword = new LabelTextVar(transMeta, wConnectionGroup,
                Messages.getString("SocrataPluginDialog.Password.Label"),
                Messages.getString("SocrataPluginDialog.Password.Tooltip"), true);
        props.setLook(wPassword);
        wPassword.addModifyListener(lsMod);
        fdPassword = new FormData();
        fdPassword.left = new FormAttachment(0, 0);
        fdPassword.top = new FormAttachment(wUserName, margin);
        fdPassword.right = new FormAttachment(100, 0);
        wPassword.setLayoutData(fdPassword);

        fdConnectionGroup = new FormData();
        fdConnectionGroup.left = new FormAttachment(0, margin);
        fdConnectionGroup.top = new FormAttachment(wStepname, margin);
        fdConnectionGroup.right = new FormAttachment(100, -margin);
        wConnectionGroup.setLayoutData(fdConnectionGroup);

        // *** END Connection GROUP *** //

        // *** START Parameters GROUP *** //
        wParametersGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
        props.setLook(wParametersGroup);
        wParametersGroup.setText(Messages.getString("SocrataPluginDialog.ParametersGroup.Label"));

        FormLayout parametersGroupLayout = new FormLayout();
        parametersGroupLayout.marginWidth = 10;
        parametersGroupLayout.marginHeight = 10;
        wParametersGroup.setLayout(parametersGroupLayout);

        // Dataset Name
        wlDatasetName = new Label(wParametersGroup, SWT.RIGHT);
        wlDatasetName.setText(Messages.getString("SocrataPluginDialog.DatasetName.Label"));
        props.setLook(wlDatasetName);
        fdlDatasetName = new FormData();
        fdlDatasetName.left = new FormAttachment(0, 0);
        fdlDatasetName.top = new FormAttachment(wParametersGroup, margin);
        fdlDatasetName.right = new FormAttachment(middle, -margin);
        wlDatasetName.setLayoutData(fdlDatasetName);

        wDatasetName = new TextVar(transMeta, wParametersGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wDatasetName);
        wDatasetName.addModifyListener(lsMod);
        fdDatasetName = new FormData();
        fdDatasetName.left = new FormAttachment(middle, 0);
        fdDatasetName.top = new FormAttachment(wParametersGroup, margin);
        fdDatasetName.right = new FormAttachment(100, 0);
        wDatasetName.setLayoutData(fdDatasetName);

        // Writer Mode
        wlWriterMode = new Label(wParametersGroup, SWT.RIGHT);
        wlWriterMode.setText(Messages.getString("SocrataPluginDialog.WriterMode.Label"));
        props.setLook(wlWriterMode);
        fdlWriterMode = new FormData();
        fdlWriterMode.left = new FormAttachment(0, 0);
        fdlWriterMode.top = new FormAttachment(wDatasetName, margin);
        fdlWriterMode.right = new FormAttachment(middle, -margin);
        wlWriterMode.setLayoutData(fdlWriterMode);

        wWriterMode = new ComboVar(transMeta, wParametersGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
        wWriterMode.setEditable(true);

        String upsert = Messages.getString("SocrataPluginDialog.WriterMode.Upsert");
        String replace = Messages.getString("SocrataPluginDialog.WriterMode.Replace");
        String create = Messages.getString("SocrataPluginDialog.WriterMode.Create");
        String delete = Messages.getString("SocrataPluginDialog.WriterMode.Delete");
        //wWriterMode.setItems(new String[]{upsert, replace});
        wWriterMode.setItems(new String[]{upsert, replace, create, delete});
        wWriterMode.setData(upsert, "upsert");
        wWriterMode.setData(replace, "replace");
        wWriterMode.setData(create, "create");
        wWriterMode.setData(delete, "delete");

        props.setLook(wWriterMode);
        wWriterMode.addModifyListener(lsMod);
        wWriterMode.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
                setActive();
            }
        });
        wWriterMode.setToolTipText(Messages.getString("SocrataPluginDialog.WriterMode.Tooltip"));
        fdWriterMode = new FormData();
        fdWriterMode.left = new FormAttachment(middle, 0);
        fdWriterMode.top = new FormAttachment(wDatasetName, margin);
        fdWriterMode.right = new FormAttachment(100, 0);
        wWriterMode.setLayoutData(fdWriterMode);

        // Import Config
        wlImportConfig = new Label(wParametersGroup, SWT.RIGHT);
        wlImportConfig.setText(Messages.getString("SocrataPluginDialog.ImportConfig.Label"));
        props.setLook(wlImportConfig);
        fdlImportConfig = new FormData();
        fdlImportConfig.left = new FormAttachment(0, 0);
        fdlImportConfig.top = new FormAttachment(wWriterMode, margin);
        fdlImportConfig.right = new FormAttachment(middle, -margin);
        wlImportConfig.setLayoutData(fdlImportConfig);

        wImportConfig = new ComboVar(transMeta, wParametersGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
        wImportConfig.setEditable(true);

        String[] importConfigs;
        if (SocrataPublishUtil.hasValue(wDomain.getText()) &&
                SocrataPublishUtil.hasValue(wUserName.getText()) &&
                SocrataPublishUtil.hasValue(wPassword.getText())) {
            importConfigs = getImportConfigs();
        } else {
            importConfigs = new String[] {""};
        }

        wImportConfig.setItems(importConfigs);
        wImportConfig.setData(upsert, "upsert");
        wImportConfig.setData(replace, "replace");
        wImportConfig.setData(create, "create");
        wImportConfig.setData(delete, "delete");

        props.setLook(wImportConfig);
        wImportConfig.addModifyListener(lsMod);
        wImportConfig.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
                setActive();
            }
        });
        wImportConfig.setToolTipText(Messages.getString("SocrataPluginDialog.ImportConfig.Tooltip"));
        fdImportConfig = new FormData();
        fdImportConfig.left = new FormAttachment(middle, 0);
        fdImportConfig.top = new FormAttachment(wWriterMode, margin);
        fdImportConfig.right = new FormAttachment(100, 0);
        wImportConfig.setLayoutData(fdImportConfig);

        // New Dataset Name
        wlNewDatasetName = new Label(wParametersGroup, SWT.RIGHT);
        wlNewDatasetName.setText(Messages.getString("SocrataPluginDialog.NewDatasetName.Label"));
        props.setLook(wlNewDatasetName);
        fdlNewDatasetName = new FormData();
        fdlNewDatasetName.left = new FormAttachment(0, 0);
        fdlNewDatasetName.top = new FormAttachment(wImportConfig, margin);
        fdlNewDatasetName.right = new FormAttachment(middle, -margin);
        wlNewDatasetName.setLayoutData(fdlNewDatasetName);

        wNewDatasetName = new TextVar(transMeta, wParametersGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wNewDatasetName);
        wNewDatasetName.addModifyListener(lsMod);
        fdNewDatasetName = new FormData();
        fdNewDatasetName.left = new FormAttachment(middle, 0);
        fdNewDatasetName.top = new FormAttachment(wImportConfig, margin);
        fdNewDatasetName.right = new FormAttachment(100, 0);
        wNewDatasetName.setLayoutData(fdNewDatasetName);

        // Publish Dataset
        wlPublishDataset = new Label(wParametersGroup, SWT.RIGHT);
        wlPublishDataset.setText(Messages.getString("SocrataPluginDialog.PublishDataset.Label"));
        props.setLook(wlPublishDataset);
        fdlPublishDataset = new FormData();
        fdlPublishDataset.left = new FormAttachment(0, 0);
        fdlPublishDataset.top = new FormAttachment(wNewDatasetName, margin);
        fdlPublishDataset.right = new FormAttachment(middle, -margin);
        wlPublishDataset.setLayoutData(fdlPublishDataset);

        wPublishDataset = new Button(wParametersGroup, SWT.CHECK);
        wPublishDataset.setToolTipText(Messages.getString("SocrataPluginDialog.PublishDataset.Tooltip"));
        props.setLook(wPublishDataset);
        fdPublishDataset = new FormData();
        fdPublishDataset.left = new FormAttachment(middle, 0);
        fdPublishDataset.top = new FormAttachment(wNewDatasetName, margin);
        fdPublishDataset.right = new FormAttachment(100, 0);
        wPublishDataset.setLayoutData(fdPublishDataset);

        // Public Dataset
        wlPublicDataset = new Label(wParametersGroup, SWT.RIGHT);
        wlPublicDataset.setText(Messages.getString("SocrataPluginDialog.PublicDataset.Label"));
        props.setLook(wlPublicDataset);
        fdlPublicDataset = new FormData();
        fdlPublicDataset.left = new FormAttachment(0, 0);
        fdlPublicDataset.top = new FormAttachment(wPublishDataset, margin);
        fdlPublicDataset.right = new FormAttachment(middle, -margin);
        wlPublicDataset.setLayoutData(fdlPublicDataset);

        wPublicDataset = new Button(wParametersGroup, SWT.CHECK);
        wPublicDataset.setToolTipText(Messages.getString("SocrataPluginDialog.PublicDataset.Tooltip"));
        props.setLook(wPublicDataset);
        fdPublicDataset = new FormData();
        fdPublicDataset.left = new FormAttachment(middle, 0);
        fdPublicDataset.top = new FormAttachment(wPublishDataset, margin);
        fdPublicDataset.right = new FormAttachment(100, 0);
        wPublicDataset.setLayoutData(fdPublicDataset);

        // Use Socrata Geocoding?
        wlUseSocrataGeocoding = new Label(wParametersGroup, SWT.RIGHT);
        wlUseSocrataGeocoding.setText(Messages.getString("SocrataPluginDialog.UseSocrataGeocoding.Label"));
        props.setLook(wlUseSocrataGeocoding);
        fdlUseSocrataGeocoding = new FormData();
        fdlUseSocrataGeocoding.left = new FormAttachment(0, 0);
        fdlUseSocrataGeocoding.top = new FormAttachment(wPublicDataset, margin);
        fdlUseSocrataGeocoding.right = new FormAttachment(middle, -margin);
        wlUseSocrataGeocoding.setLayoutData(fdlUseSocrataGeocoding);

        wUseSocrataGeocoding = new Button(wParametersGroup, SWT.CHECK);
        wUseSocrataGeocoding.setToolTipText(Messages.getString("SocrataPluginDialog.UseSocrataGeocoding.Tooltip"));
        props.setLook(wUseSocrataGeocoding);
        fdUseSocrataGeocoding = new FormData();
        fdUseSocrataGeocoding.left = new FormAttachment(middle, 0);
        fdUseSocrataGeocoding.top = new FormAttachment(wPublicDataset, margin);
        fdUseSocrataGeocoding.right = new FormAttachment(100, 0);
        wUseSocrataGeocoding.setLayoutData(fdUseSocrataGeocoding);

        // Delete Temp csv File After Processing?
        wlDeleteTempFile = new Label(wParametersGroup, SWT.RIGHT);
        wlDeleteTempFile.setText(Messages.getString("SocrataPluginDialog.DeleteTempFile.Label"));
        props.setLook(wlDeleteTempFile);
        fdlDeleteTempFile = new FormData();
        fdlDeleteTempFile.left = new FormAttachment(0, 0);
        fdlDeleteTempFile.top = new FormAttachment(wUseSocrataGeocoding, margin);
        fdlDeleteTempFile.right = new FormAttachment(middle, -margin);
        wlDeleteTempFile.setLayoutData(fdlDeleteTempFile);

        wDeleteTempFile = new Button(wParametersGroup, SWT.CHECK);
        wDeleteTempFile.setToolTipText(Messages.getString("SocrataPluginDialog.DeleteTempFile.Tooltip"));
        props.setLook(wDeleteTempFile);
        fdDeleteTempFile = new FormData();
        fdDeleteTempFile.left = new FormAttachment(middle, 0);
        fdDeleteTempFile.top = new FormAttachment(wUseSocrataGeocoding, margin);
        fdDeleteTempFile.right = new FormAttachment(100, 0);
        wDeleteTempFile.setLayoutData(fdDeleteTempFile);

        fdParametersGroup = new FormData();
        fdParametersGroup.left = new FormAttachment(0, margin);
        fdParametersGroup.top = new FormAttachment(wConnectionGroup, margin);
        fdParametersGroup.right = new FormAttachment(100, -margin);
        wParametersGroup.setLayoutData(fdParametersGroup);

        // *** END Parameters GROUP *** //

        // *** START Proxy GROUP *** //
        wProxyGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
        props.setLook(wProxyGroup);
        wProxyGroup.setText(Messages.getString("SocrataPluginDialog.ProxyGroup.Label"));

        FormLayout proxyGroupLayout = new FormLayout();
        proxyGroupLayout.marginWidth = 10;
        proxyGroupLayout.marginHeight = 10;
        wProxyGroup.setLayout(proxyGroupLayout);

        // Proxy Host
        wProxyHost = new LabelTextVar(transMeta, wProxyGroup,
                Messages.getString("SocrataPluginDialog.ProxyHost.Label"),
                Messages.getString("SocrataPluginDialog.ProxyHost.Tooltip"));
        props.setLook(wProxyHost);
        wProxyHost.addModifyListener(lsMod);
        fdProxyHost = new FormData();
        fdProxyHost.left = new FormAttachment(0, 0);
        fdProxyHost.top = new FormAttachment(wProxyGroup, margin);
        fdProxyHost.right = new FormAttachment(100, 0);
        wProxyHost.setLayoutData(fdProxyHost);

        // Proxy Port
        wProxyPort = new LabelTextVar(transMeta, wProxyGroup,
                Messages.getString("SocrataPluginDialog.ProxyPort.Label"),
                Messages.getString("SocrataPluginDialog.ProxyPort.Tooltip"));
        props.setLook(wProxyPort);
        wProxyPort.addModifyListener(lsMod);
        fdProxyPort = new FormData();
        fdProxyPort.left = new FormAttachment(0, 0);
        fdProxyPort.top = new FormAttachment(wProxyHost, margin);
        fdProxyPort.right = new FormAttachment(100, 0);
        wProxyPort.setLayoutData(fdProxyPort);

        // Proxy Username
        wProxyUsername = new LabelTextVar(transMeta, wProxyGroup,
                Messages.getString("SocrataPluginDialog.ProxyUsername.Label"),
                Messages.getString("SocrataPluginDialog.ProxyUsername.Tooltip"));
        props.setLook(wProxyUsername);
        wProxyUsername.addModifyListener(lsMod);
        fdProxyUsername = new FormData();
        fdProxyUsername.left = new FormAttachment(0, 0);
        fdProxyUsername.top = new FormAttachment(wProxyPort, margin);
        fdProxyUsername.right = new FormAttachment(100, 0);
        wProxyUsername.setLayoutData(fdProxyUsername);

        // Proxy Password
        wProxyPassword = new LabelTextVar(transMeta, wProxyGroup,
                Messages.getString("SocrataPluginDialog.ProxyPassword.Label"),
                Messages.getString("SocrataPluginDialog.ProxyPassword.Tooltip"), true);
        props.setLook(wProxyPassword);
        wProxyPassword.addModifyListener(lsMod);
        fdProxyPassword = new FormData();
        fdProxyPassword.left = new FormAttachment(0, 0);
        fdProxyPassword.top = new FormAttachment(wProxyUsername, margin);
        fdProxyPassword.right = new FormAttachment(100, 0);
        wProxyPassword.setLayoutData(fdProxyPassword);

        fdProxyGroup = new FormData();
        fdProxyGroup.left = new FormAttachment(0, margin);
        fdProxyGroup.top = new FormAttachment(wParametersGroup, margin);
        fdProxyGroup.right = new FormAttachment(100, -margin);
        wProxyGroup.setLayoutData(fdProxyGroup);

        // *** END Proxy GROUP *** //

        fdSettingsComp = new FormData();
        fdSettingsComp.left = new FormAttachment(0, 0);
        fdSettingsComp.top = new FormAttachment(wStepname, margin);
        fdSettingsComp.right = new FormAttachment(100, 0);
        fdSettingsComp.bottom = new FormAttachment(100, 0);
        wSettingsComp.setLayoutData(fdSettingsComp);

        wSettingsComp.layout();
        wSettingsTab.setControl(wSettingsComp);

        // //////////////////////
        // /// End SETTINGS TAB
        // //////////////////////

        // //////////////////////
        // /// Start FIELDS TAB
        // //////////////////////

        wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
        wFieldsTab.setText(Messages.getString("SocrataPluginDialog.FieldsTab.TabTitle"));

        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = Const.FORM_MARGIN;
        fieldsLayout.marginWidth = Const.FORM_MARGIN;

        wFieldsComp = new Composite(wTabFolder, SWT.NONE);
        wFieldsComp.setLayout(fieldsLayout);
        props.setLook(wFieldsComp);

        wGet = new Button(wFieldsComp, SWT.PUSH);
        wGet.setText(Messages.getString("System.Button.GetFields"));
        wGet.setToolTipText(Messages.getString("System.Tooltip.GetFields"));

        wMinWidth = new Button(wFieldsComp, SWT.PUSH);
        wMinWidth.setText(Messages.getString("SocrataPluginDialog.MinWidth.Button"));
        wMinWidth.setToolTipText(Messages.getString("SocrataPluginDialog.MinWidth.Tooltip"));
        wMinWidth.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });
        setButtonPositions(new Button[] {wGet, wMinWidth}, margin, null);

        final int fieldsCols = 10;
        final int fieldsRows = input.getOutputFields().length;

        // Prepare a list of possible formats
        java.util.List<String> formats = new ArrayList<String>();
        formats.addAll(Arrays.asList(Const.getDateFormats()));
        formats.addAll(Arrays.asList(Const.getNumberFormats()));

        columnInfos = new ColumnInfo[fieldsCols];
        columnInfos[0] = new ColumnInfo(Messages.getString("SocrataPluginDialog.NameColumn.Column"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false);
        columnInfos[1] = new ColumnInfo(Messages.getString("SocrataPluginDialog.TypeColumn.Column"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes());
        columnInfos[2] = new ColumnInfo(Messages.getString("SocrataPluginDialog.FormatColumn.Column"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, formats.toArray(new String[formats.size()]));
        columnInfos[3] = new ColumnInfo(Messages.getString("SocrataPluginDialog.LengthColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);
        columnInfos[4] = new ColumnInfo(Messages.getString("SocrataPluginDialog.PrecisionColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);
        columnInfos[5] = new ColumnInfo(Messages.getString("SocrataPluginDialog.CurrencyColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);
        columnInfos[6] = new ColumnInfo(Messages.getString("SocrataPluginDialog.DecimalColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);
        columnInfos[7] = new ColumnInfo(Messages.getString("SocrataPluginDialog.GroupColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);
        columnInfos[8] = new ColumnInfo(Messages.getString("SocrataPluginDialog.TrimTypeColumn.Column"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.trimTypeDesc, true);
        columnInfos[9] = new ColumnInfo(Messages.getString("SocrataPluginDialog.NullColumn.Column"),
                ColumnInfo.COLUMN_TYPE_TEXT, false);

        wFields = new TableView(transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnInfos,
                fieldsRows, lsMod, props);

        fdFields = new FormData();
        fdFields.left = new FormAttachment(0, 0);
        fdFields.top = new FormAttachment(0, 0);
        fdFields.right = new FormAttachment(100, 0);
        fdFields.bottom = new FormAttachment(wGet, -margin);
        wFields.setLayoutData(fdFields);

        // Search the fields in the background
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta != null) {
                    try {
                        RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

                        // Remember these fields...
                        for (int i = 0; i < row.size(); i++) {
                            inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }
                        setComboBoxes();
                    } catch (KettleException e) {
                        logError(Messages.getString("System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();

        fdFieldsComp = new FormData();
        fdFieldsComp.left = new FormAttachment(0, 0);
        fdFieldsComp.top = new FormAttachment(wStepname, margin);
        fdFieldsComp.right = new FormAttachment(100, 0);
        fdFieldsComp.bottom = new FormAttachment(100, 0);
        wFieldsComp.setLayoutData(fdFieldsComp);

        wFieldsComp.layout();
        wFieldsTab.setControl(wFieldsComp);


        // //////////////////////
        // /// End FIELDS TAB
        // //////////////////////

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(Messages.getString("System.Button.OK"));

        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(Messages.getString("System.Button.Cancel"));

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, null);

        fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment(0, 0);
        fdTabFolder.top = new FormAttachment(wStepname, margin);
        fdTabFolder.right = new FormAttachment(100, 0);
        fdTabFolder.bottom = new FormAttachment(wOK, -margin);
        wTabFolder.setLayoutData(fdTabFolder);

        lsGet = new Listener() {
            @Override
            public void handleEvent(Event event) {
                get();
            }
        };

        lsMinWidth = new Listener() {
            @Override
            public void handleEvent(Event event) {
                setMinimalWidth();
            }
        };

        lsOK = new Listener() {
            @Override
            public void handleEvent(Event event) {
                ok();
            }
        };

        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event event) {
                cancel();
            }
        };

        wOK.addListener(SWT.Selection, lsOK);
        wCancel.addListener(SWT.Selection, lsCancel);
        wGet.addListener(SWT.Selection, lsGet);
        wMinWidth.addListener(SWT.Selection, lsMinWidth);

        wTabFolder.setSelection(0);

        setSize();

        getData();

        setActive();

        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return stepname;
    }

    private void setActive() {
        String writerMode = wWriterMode.getText();

        boolean enable = writerMode.equalsIgnoreCase("Create");

        wlNewDatasetName.setEnabled(enable);
        wNewDatasetName.setEnabled(enable);

        wlPublicDataset.setEnabled(enable);
        wPublicDataset.setEnabled(enable);

        wlPublishDataset.setEnabled(enable);
        wPublishDataset.setEnabled(enable);
    }

    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    private void ok() {
        if (Const.isEmpty(wStepname.getText())) {
            return;
        }

        saveInfoInMeta(input);

        // Check for dataset id (not create) or dataset title (create only)
        if (input.hasChanged()) {
            if (input.getWriterMode().equalsIgnoreCase("create") &&
                    (input.getNewDatasetName().isEmpty() || input.getNewDatasetName() == null)) {
                MessageBox messageBox = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                messageBox.setMessage(Messages.getString("SocrataPluginDialog.MissingValue.InputDatasetName"));
                messageBox.setText(Messages.getString("SocrataPluginDialog.DialogCaptionError"));
                messageBox.open();
                return;
            } else if (!input.getWriterMode().equalsIgnoreCase("create") && (input.getDatasetName().isEmpty() || input.getDatasetName() == null)) {
                MessageBox messageBox = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                messageBox.setMessage(Messages.getString("SocrataPluginDialog.MissingValue.InputValidId"));
                messageBox.setText(Messages.getString("SocrataPluginDialog.DialogCaptionError"));
                messageBox.open();
                return;
            }
        }

        // Show warning on delete
        if (input.getWriterMode().equalsIgnoreCase("delete")) {
            MessageDialog md = new MessageDialog(shell, Messages.getString("SocrataPluginDialog.DeleteAction.DialogTitle"),
                    null, Messages.getString("SocrataPluginDialog.DeleteAction.DialogMessage"), MessageDialog.WARNING,
                    new String[] {Messages.getString("SocrataPluginDialog.DeleteAction.DialogConfirm")}, 0);
            MessageDialog.setDefaultImage(GUIResource.getInstance().getImageSpoon());
            md.open();
        }

        stepname = wStepname.getText();

        dispose();
    }

    public void getData() {
        wDomain.setText(input.getDomain());
        wUserName.setText(input.getUser());
        wPassword.setText(input.getPassword());
        wDatasetName.setText(input.getDatasetName());
        wPublishDataset.setSelection(input.isPublishDataset());
        wPublicDataset.setSelection(input.isPublicDataset());
        wWriterMode.setText(input.getWriterMode());
        wImportConfig.setText(input.getImportConfig());
        wNewDatasetName.setText(input.getNewDatasetName());
        wUseSocrataGeocoding.setSelection(input.isUseSocrataGeocoding());
        wDeleteTempFile.setSelection(input.isDeleteTempFile());
        wProxyHost.setText(input.getProxyHost());
        wProxyPort.setText(input.getProxyPort());
        wProxyUsername.setText(input.getProxyUsername());
        wProxyPassword.setText(input.getProxyPassword());

        String[] importConfigs;
        if (SocrataPublishUtil.hasValue(wDomain.getText()) &&
                SocrataPublishUtil.hasValue(wUserName.getText()) &&
                SocrataPublishUtil.hasValue(wPassword.getText())) {
            importConfigs = getImportConfigs();
        } else {
            importConfigs = new String[] {""};
        }

        wImportConfig.setItems(importConfigs);

        for (int i = 0; i < input.getOutputFields().length; i++) {
            SocrataTextFileField field = input.getOutputFields()[i];

            TableItem item = wFields.table.getItem(i);
            if (field.getName() != null) {
                item.setText(1, field.getName());
            }
            item.setText(2, field.getTypeDesc());
            if (field.getFormat() != null) {
                item.setText(3, field.getFormat());
            }
            if (field.getLength() >= 0) {
                item.setText(4, "" + field.getLength());
            }
            if (field.getPrecision() >= 0) {
                item.setText(5, "" + field.getPrecision());
            }
            if (field.getCurrencySymbol() != null) {
                item.setText(6, field.getCurrencySymbol() );
            }
            if (field.getDecimalSymbol() != null) {
                item.setText(7, field.getDecimalSymbol() );
            }
            if (field.getGroupingSymbol() != null) {
                item.setText(8, field.getGroupingSymbol());
            }
            String trim = field.getTrimTypeDesc();
            if (trim != null) {
                item.setText(9, trim);
            }
            if (field.getNullString() != null) {
                item.setText(10, field.getNullString());
            }
        }

        wFields.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();
    }

    private void saveInfoInMeta(SocrataPluginMeta spm) {
        spm.setDomain(wDomain.getText());
        spm.setUser(wUserName.getText());
        spm.setPassword(wPassword.getText());
        spm.setDatasetName(wDatasetName.getText());
        spm.setPublishDataset(wPublishDataset.getSelection());
        spm.setPublicDataset(wPublicDataset.getSelection());
        spm.setWriterMode(wWriterMode.getText());
        spm.setImportConfig(wImportConfig.getText());
        spm.setNewDatasetName(wNewDatasetName.getText());
        spm.setUseSocrataGeocoding(wUseSocrataGeocoding.getSelection());
        spm.setDeleteTempFile(wDeleteTempFile.getSelection());
        spm.setProxyHost(wProxyHost.getText());
        spm.setProxyPort(wProxyPort.getText());
        spm.setProxyUsername(wProxyUsername.getText());
        spm.setProxyPassword(wProxyPassword.getText());

        int nrfields = wFields.nrNonEmpty();

        spm.allocate(nrfields);

        for (int i = 0; i < nrfields; i++) {
            SocrataTextFileField field = new SocrataTextFileField();

            TableItem item = wFields.getNonEmpty(i);
            field.setName(item.getText(1));
            field.setType(item.getText(2));
            field.setFormat(item.getText(3));
            field.setLength(Const.toInt(item.getText(4), -1));
            field.setPrecision(Const.toInt(item.getText(5), -1));
            field.setCurrencySymbol(item.getText(6));
            field.setDecimalSymbol(item.getText(7));
            field.setGroupingSymbol(item.getText(8));
            field.setTrimType(ValueMeta.getTrimTypeByDesc(item.getText(9)));
            field.setNullString(item.getText(10));

            spm.getOutputFields()[i] = field;
        }
    }

    private void get() {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields(stepname);
            if (r != null) {
                TableItemInsertListener listener = new TableItemInsertListener() {
                    @Override
                    public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v) {

                        if (v.isNumeric()) {
                            // currency symbol
                            tableItem.setText(6, Const.NVL(v.getCurrencySymbol(), ""));

                            // decimal and grouping
                            tableItem.setText(7, Const.NVL(v.getDecimalSymbol(), ""));
                            tableItem.setText(8, Const.NVL(v.getGroupingSymbol(), ""));
                        }

                        // trim type
                        tableItem.setText(9, Const.NVL(ValueMeta.getTrimTypeDesc(v.getTrimType()), ""));

                        // conversion mask
                        if (!Const.isEmpty(v.getConversionMask())) {
                            tableItem.setText(3, v.getConversionMask());
                        } else {
                            if (v.isNumber()) {
                                if (v.getLength() > 0) {
                                    int le = v.getLength();
                                    int pr = v.getPrecision();

                                    if (v.getPrecision() <= 0) {
                                        pr = 0;
                                    }

                                    String mask = "";
                                    for (int m = 0; m < le - pr; m++) {
                                        mask += "0";
                                    }
                                    if (pr > 0) {
                                        mask += ".";
                                    }
                                    for (int m = 0; m < pr; m++) {
                                        mask += "0";
                                    }
                                    tableItem.setText(3, mask);
                                }
                            }
                        }
                        return true;
                    }
                };
                BaseStepDialog.getFieldsFromPrevious(r, wFields, 1, new int[] {1}, new int[] {2}, 4, 5, listener);
            }
        } catch(KettleException e) {
            new ErrorDialog(shell, Messages.getString("System.Dialog.GetFieldsFailed.Title"),
                    Messages.getString("System.Dialog.GetFieldsFailed.Message"), e);
        }
    }

    private void setMinimalWidth() {
        int nrNonEmptyFields = wFields.nrNonEmpty();

        for (int i = 0; i < nrNonEmptyFields; i++) {
            TableItem item = wFields.getNonEmpty(i);

            item.setText(4, "");
            item.setText(5, "");
            item.setText(9, ValueMeta.getTrimTypeDesc(ValueMetaInterface.TRIM_TYPE_BOTH));

            int type = ValueMeta.getType(item.getText(2));
            switch (type) {
                case ValueMetaInterface.TYPE_STRING:
                    item.setText(3, "");
                    break;
                case ValueMetaInterface.TYPE_INTEGER:
                    item.setText(3, "0");
                    break;
                case ValueMetaInterface.TYPE_NUMBER:
                    item.setText(3, "0.#####");
                    break;
                case ValueMetaInterface.TYPE_DATE:
                    break;
                default:
                    break;
            }
        }

        for (int i = 0; i < input.getOutputFields().length; i++) {
            input.getOutputFields()[i].setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH);
        }
        wFields.optWidth(true);
    }

    protected void setComboBoxes() {
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        fields.putAll(inputFields);

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String[] fieldNames = entries.toArray(new String[entries.size()]);

        Const.sortStrings(fieldNames);
        columnInfos[0].setComboValues(fieldNames);
    }

    private String[] getImportConfigs() {
        List<String> importConfigs = new ArrayList<>();
        String domain = wDomain.getText();
        String host;
        if (domain.startsWith("https://")) {
            host = domain.replace("https://", "");
        } else if (domain.startsWith("http://")) {
            host = domain.replace("http://", "");
        } else {
            host = domain;
            domain = "https://" + domain;
        }

        String url = domain + "/api/publishing/v1/config";
        String credentials = wUserName.getText() + ":" + wPassword.getText();
        String authorize = Base64.getEncoder().encodeToString(credentials.getBytes());

        JsonNode result = SocrataPublishUtil.execute(SocrataPublishUtil.get(url, host, authorize, "application/json"), log);

        for (JsonNode node : result) {
            JsonNode name = node.path("resource").path("name");
            importConfigs.add(name.asText());
        }

        return importConfigs.toArray(new String[importConfigs.size()]);
    }
}
