package com.socrata.pdi.plugin;


import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.io.OutputStream;

/**
 * @author malindac
 * @version 1.0
 * 2015-07-23
 */
public class SocrataPluginData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface outputRowMeta;

    public int[] fieldnrs;

    public OutputStream writer;
    public OutputStream fos;

    public SocrataPluginData() {
        super();
    }
}
