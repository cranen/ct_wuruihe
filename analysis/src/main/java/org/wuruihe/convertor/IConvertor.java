package org.wuruihe.convertor;

import org.wuruihe.key.BaseDimension;

import java.sql.SQLException;

public  interface IConvertor {
    int getDimensionID(BaseDimension baseDimension) throws SQLException;

}
