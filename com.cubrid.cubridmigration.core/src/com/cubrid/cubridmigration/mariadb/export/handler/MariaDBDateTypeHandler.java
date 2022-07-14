/**
 * 
 */
package com.cubrid.cubridmigration.mariadb.export.handler;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

/**
 * @author rathana
 *
 */
public class MariaDBDateTypeHandler implements IExportDataHandler{

	
	/**
	 * Retrieves the value object of Date column.
	 * 
	 * @param rs the result set
	 * @param column column description
	 * @return value of column
	 * @throws SQLException e
	 */
	public Object getJdbcObject(ResultSet rs, Column column) throws SQLException {
		try {
			return Date.valueOf(rs.getString(column.getName()));
		} catch (Exception e) {
			String dateValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_DATE);
			return MySQL2CUBRIDMigParas.getReplacedDate(dateValue,
					TimeZone.getDefault());
		}
	}
}
