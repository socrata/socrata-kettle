package com.socrata.pdi.plugin;

import org.pentaho.di.trans.steps.textfileoutput.TextFileField;

/**
 * @author malindac
 * @version 1.0
 * 2015-08-11
 */
public class SocrataTextFileField extends TextFileField {

    private String fieldName;

    public SocrataTextFileField() {
        super();
    }

    public SocrataTextFileField(String name, int type, String format, int length, int precision, String currencySymbol, String decimalSymbol, String groupSymbol, String nullString) {
        super(name, type, format, length, precision, currencySymbol, decimalSymbol, groupSymbol, nullString);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
