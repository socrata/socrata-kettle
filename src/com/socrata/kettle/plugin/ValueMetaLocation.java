package com.socrata.kettle.plugin;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaPlugin;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author malindac
 * @version 1.0
 * 2016-05-06
 */

@ValueMetaPlugin(id = "11", name = "Location", description = "A string formatted to represent a location")
public class ValueMetaLocation extends ValueMetaBase implements Cloneable {

    public static final int TYPE_LOCATION = 11;

    public ValueMetaLocation() {
        this(null);
    }

    public ValueMetaLocation(String name) {
        super(name, TYPE_LOCATION);
    }

    @Override
    public String getString(Object object) throws KettleValueException {
        String locationString = object.toString();
        return locationString;
    }

    @Override
    public Double getNumber(Object object) throws KettleValueException {
        throw new KettleValueException(toString() + ": can't be converted to a number");
    }

    @Override
    public Long getInteger(Object object) throws KettleValueException {
        throw new KettleValueException(toString() + ": can't be converted to an integer");
    }

    @Override
    public BigDecimal getBigNumber(Object object) throws KettleValueException {
        throw new KettleValueException(toString() + ": can't be converted to a big number");
    }

    @Override
    public Boolean getBoolean(Object object) throws KettleValueException {
        throw new KettleValueException(toString() + ": can't be converted to a boolean");
    }

    @Override
    public Date getDate(Object object) throws KettleValueException {
        throw new KettleValueException(toString() + ": can't be converted to a date");
    }

    /**
     * Convert the specified data to the data type specified in this object.
     *
     * @param meta2
     *          the metadata of the object to be converted
     * @param data2
     *          the data of the object to be converted
     * @return the object in the data type of this value metadata object
     * @throws KettleValueException
     *           in case there is a data conversion error
     */
    @Override
    public Object convertData(ValueMetaInterface meta2, Object data2) throws KettleValueException {
        switch (meta2.getType()) {
            case TYPE_STRING:
                return data2.toString();
            case TYPE_LOCATION:
                return data2.toString();
            default:
                throw new KettleValueException(meta2.toStringMeta() + " : can't be converted to a location");
        }
    }

    @Override
    public Object getNativeDataType(Object object) throws KettleValueException {
        return object.toString();
    }
}
