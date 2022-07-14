/**
 * 
 */
package com.cubrid.cubridmigration.mariadb.export.handler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

/**
 * @author rathana
 *
 */
public class MariaDBTimestampTypeHandler implements IExportDataHandler {

	
	
	/**
	 * Retrieves the value object of Timestamp column.
	 * 
	 * @param rs the result set
	 * @param column column description
	 * @return value of column
	 * @throws SQLException e
	 */
	public Object getJdbcObject(ResultSet rs, Column column) throws SQLException {
		// if there is "0000-00-00 00:00:00" in time field, for example, return null		
		try {
			return Timestamp.valueOf(rs.getString(column.getName()));
		} catch (Exception e) {
			String timestampValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
			return MySQL2CUBRIDMigParas.getReplacedTimestamp(timestampValue,
					TimeZone.getDefault());
		}
	}
}
