/*
 * Copyright (c) 2016 CUBRID Corporation.
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.postgresql.meta;

import java.io.Reader;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;

/**
 * PostgreSQLSchemaFetcher Description
 *
 * @author rathana
 * @version 1.0 
 * @created Feb 5, 2023
 */
public class PostgreSQLSchemaFetcher extends AbstractJDBCSchemaFetcher{

	private final static Logger LOG = LogUtil.getLogger(PostgreSQLSchemaFetcher.class);
	private static final String NEWLINE = "\n";
	final static List<String> CUMSTOME_TYPE = Arrays.asList("USER-DEFINED", "ARRAY");
	final static String TABLE_QUERY = "select pn.nspname, pc.* from pg_class as pc \r\n"
			+ " join pg_namespace as pn on pc.relnamespace = pn.oid\r\n"
			+ " where pn.nspname =? and pc.relkind in ('r', 'p') and relispartition = ?";
	final static String SEQUENCE_QUERY = "select * from information_schema.sequences where sequence_schema = ?";
	final static String VIEW_QUERY = "select pg_get_viewdef(?, true) as view_ddl";
	final static String COLUMN_QUERY = "SELECT * FROM information_schema.columns WHERE table_name = ?";
			
	final static String TRIGGER_QUERY = "select * from information_schema.triggers where trigger_schema = ?";
	final static String ROUTINE_QUERY = "select pg_get_functiondef(p.oid) AS ddl, p.proname as name from pg_proc p "
			+ "JOIN   pg_namespace n ON n.oid = p.pronamespace where p.prokind = ? and n.nspname = ?";
	
	public PostgreSQLSchemaFetcher() {
		factory = new DBObjectFactory();
	}
	
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		
		Catalog catalog = super.buildCatalog(conn, cp, filter);
		catalog.setDatabaseType(DatabaseType.POSTGRESQL);
		
		
		final List<Schema> schemaList = new ArrayList<Schema>(catalog.getSchemas());
		for(Schema schema: schemaList) {
			buildPartitions(conn, catalog, schema);
		}
		return catalog;
		
	}
	
	
	
	/**
	 * build Partitions
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 */
	protected void buildPartitions(final Connection conn, final Catalog catalog, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildPartitions()");
		}
		String GET_PARTITION_INFO = "SELECT c.relname AS child_partition,\r\n"
				+ "       p.relname AS parent_tbl,\r\n"
				+ "       matches[1] as min_rangeval,\r\n"
				+ "       matches[2] as max_rangeval,\r\n"
				+ "       c.relkind as method,\r\n"
				+ "       t.table_schema\r\n"
				+ "FROM pg_class AS p\r\n"
				+ "   JOIN pg_inherits AS i ON p.oid = i.inhparent\r\n"
				+ "   JOIN pg_class AS c ON i.inhrelid = c.oid\r\n"
				+ "   cross join regexp_matches(pg_get_expr(c.relpartbound, c.oid), '\\((.+?)\\).+\\((.+?)\\)') as matches\r\n"
				+ "   join information_schema.tables as t on c.relname  = t.table_name\r\n"
				+ "WHERE p.relkind = 'p' and t.table_schema = ?;";
		
		
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		try {
			stmt = conn.prepareStatement(GET_PARTITION_INFO);
			stmt.setString(1, schema.getName());
			rs = stmt.executeQuery();
			while (rs.next()) {
				
			
			
			String tableName = rs.getString("parent_tbl");
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]tableName=" + tableName);
			}
			Table table = schema.getTableByName(tableName);
			
			if(table == null) {
				continue;
			}
			
			String partitionMethod = rs.getString("method");
			if(partitionMethod.equals("r")) {
				partitionMethod = "RANGE";
			}
			PartitionInfo partitionInfo = factory.createPartitionInfo();
			partitionInfo.setPartitionMethod(partitionMethod);
//			partitionInfo.setDDL(getSourcePartitionDDL(table));
			partitionInfo.setPartitionExp(null);
			partitionInfo.setPartitionFunc(null);
			
			table.setPartitionInfo(partitionInfo);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]partitionInfo=" + partitionInfo);
			}
			
			}
			
		}catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
		getPartitionColumn(conn, schema);
		getPartitionTables(conn, schema);
		
		
	}
	
	
	/**
	 * get partition column information
	 * 
	 * @param conn Connection
	 * @param schema Schema
	 */
	private void getPartitionColumn(Connection conn, Schema schema) {
		// TODO Auto-generated method stub
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getPartitionColumn()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		
		String GET_PART_COLUMN = "select \r\n"
				+ "    par.relnamespace::regnamespace::text as schema, \r\n"
				+ "    par.relname as table_name, \r\n"
				+ "    column_index,\r\n"
				+ "    col.column_name as column_name\r\n"
				+ "from   \r\n"
				+ "    (select\r\n"
				+ "         partrelid,\r\n"
				+ "         unnest(partattrs) column_index\r\n"
				+ "     from\r\n"
				+ "         pg_partitioned_table) pt \r\n"
				+ "join pg_class par on par.oid = pt.partrelid\r\n"
				+ "join information_schema.columns col on  \r\n"
				+ "    col.table_schema = par.relnamespace::regnamespace::text\r\n"
				+ "    and col.table_name = par.relname\r\n"
				+ "    and ordinal_position = pt.column_index\r\n"
				+ "where par.relnamespace::regnamespace::text = ?;";

		try {
			stmt = conn.prepareStatement(GET_PART_COLUMN);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + GET_PART_COLUMN);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("table_name");
				String columnName = rs.getString("column_name");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName + ", columnName=" + columnName);
				}

				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				PartitionInfo partitionInfo = table.getPartitionInfo();
				partitionInfo.addPartitionColumn(table.getColumnByName(columnName));
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partitionInfo=" + partitionInfo);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
		
	}
	
	
	
	/**
	 * get partition table information
	 * 
	 * @param conn Connection
	 * @param schema Schema
	 */
	private void getPartitionTables(final Connection conn, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getPartitionTables()");
		}
		
		String GET_PART = "SELECT c.relname AS child_partition,\r\n"
				+ "       p.relname AS parent_tbl,\r\n"
				+ "       matches[1] as min_val,\r\n"
				+ "       matches[2] as max_val,\r\n"
				+ "       c.relkind,\r\n"
				+ "       t.table_schema,\r\n"
				+ "      row_number() OVER (PARTITION BY p.relname ORDER BY c.oid) AS partition_position\r\n"
				+ "FROM pg_class AS p\r\n"
				+ "   JOIN pg_inherits AS i ON p.oid = i.inhparent\r\n"
				+ "   JOIN pg_class AS c ON i.inhrelid = c.oid\r\n"
				+ "   cross join regexp_matches(pg_get_expr(c.relpartbound, c.oid), '\\((.+?)\\).+\\((.+?)\\)') as matches\r\n"
				+ "   join information_schema.tables as t on c.relname  = t.table_name\r\n"
				+ "WHERE p.relkind = 'p' and t.table_schema = ?;\r\n";
				
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD

		try {
			stmt = conn.prepareStatement(GET_PART);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + GET_PART + ", 1=" + schema.getName() + ", 2="
						+ schema.getName());
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("parent_tbl");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				String partitionName = rs.getString("child_partition");
				Reader reader = rs.getCharacterStream("max_val");
				String partitionDesc = reader == null ? null : DBUtils.reader2String(reader);
				int partitionPosition = rs.getInt("partition_position");

				PartitionInfo partitionInfo = table.getPartitionInfo();
				partitionInfo.setPartitionExp(null);
				partitionInfo.setPartitionFunc(null);

				PartitionTable partition = factory.createPartitionTable();
				partition.setPartitionName(partitionName);
				partition.setPartitionDesc(partitionDesc);
				partition.setPartitionIdx(partitionPosition);
				partitionInfo.addPartition(partition);
				partitionInfo.setDDL(getTablePartitonDDL(table));
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partition=" + partition);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}
	
	
	/**
	 * getTablePartitonDDL
	 * 
	 * @param table Table
	 * @return String
	 */
	public String getTablePartitonDDL(Table table) {
		PartitionInfo partInfo = table.getPartitionInfo();
		if (partInfo == null) {
			return null;
		}
		try {
			String partMethod = partInfo.getPartitionMethod();

			if (PartitionInfo.PARTITION_METHOD_KEY.equals(partMethod)
					|| PartitionInfo.PARTITION_METHOD_LINEARKEY.equals(partMethod)) {
				return partInfo.getDDL();
			}
			StringBuffer buf = new StringBuffer("PARTITION BY ");
			//Expression
			StringBuffer exp = new StringBuffer();
			exp.append("(");
			if (StringUtils.isEmpty(partInfo.getPartitionFunc())) {
				exp.append("\"").append(partInfo.getPartitionColumns().get(0).getName()).append(
						"\"");
			} else {
				exp.append(DBUtils.getCubridPartitionExp(partInfo));
			}
			exp.append(")");

			List<PartitionTable> partitions = partInfo.getPartitions();
			if (PartitionInfo.PARTITION_METHOD_RANGE.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_RANGE).append(" ");
				buf.append(exp).append("(").append(NEWLINE);
				for (int i = 0; i < partitions.size(); i++) {
					PartitionTable partTable = partitions.get(i);
					buf.append("PARTITION \"");
					buf.append(partTable.getPartitionName());
					buf.append("\" VALUES LESS THAN ");

					String partitionDesc = partTable.getPartitionDesc();

					if ("MAXVALUE".equalsIgnoreCase(partitionDesc)) {
						buf.append(partitionDesc);
					} else {
						buf.append("(");
						buf.append(partitionDesc);
						buf.append(")");
					}

					if (i != partitions.size() - 1) {
						buf.append(",");
						buf.append(NEWLINE);
					}
				}
				buf.append(NEWLINE).append(")");
			} else if (PartitionInfo.PARTITION_METHOD_LIST.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_LIST).append(" ");
				buf.append(exp).append(" ").append("( ").append(NEWLINE);
				for (int i = 0; i < partitions.size(); i++) {
					PartitionTable partitionTable = partitions.get(i);

					buf.append("PARTITION ");
					buf.append(partitionTable.getPartitionName());

					buf.append(" VALUES IN (");

					String partitionDesc = partitionTable.getPartitionDesc();

					if (partInfo.getPartitionFunc() == null) {
						String[] splits = partitionDesc.split(",");

						for (int j = 0; j < splits.length; j++) {
							if (j != 0) {
								buf.append(",");
							}
							buf.append(splits[j]);
						}

					} else {
						buf.append(partitionDesc);
					}

					buf.append(")");

					if (i != partitions.size() - 1) {
						buf.append(",");
						buf.append(NEWLINE);
					}

				}
				buf.append(NEWLINE).append(" ) ");
			} else if (PartitionInfo.PARTITION_METHOD_HASH.equals(partMethod)
					|| PartitionInfo.PARTITION_METHOD_LINEARHASH.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_HASH).append(" ");
				buf.append(exp).append(" ").append(NEWLINE);
				buf.append("PARTITIONS ").append(partitions.size());
			}
			return buf.toString();
		} catch (Exception e) {
			LOG.error("", e);
		}
		return "";
	}
	

	/** return schema names
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * 
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getSchemaNames(final Connection conn, ConnParameters cp) throws SQLException {
		List<String> result = new ArrayList<String>();
		List <String> exclude_schema = Arrays.asList("information_schema", "pg_catalog");
		//		if (StringUtils.isNotBlank(cp.getSchema())) {
		//			result.add(cp.getSchema());
		//		} else {
		//			result.add(cp.getDbName());
		//		}
//		result.add(cp.getDbName());
		ResultSet resultSet = conn.getMetaData().getSchemas();
		while (resultSet.next()) {
			String name = resultSet.getString("TABLE_SCHEM");
			if(!exclude_schema.contains(name)) {
				result.add(name);
			}
			
		}
		return result;
	}
	
	/**
	 * 
	 * extract Tables
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildTables(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTables()");
		}
		List<String> tableNameList = getAllTableNames(conn, catalog, schema);
		Set<String> set = new HashSet<String> ();
		for (String tableName : tableNameList) {
			Table table = null;
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				String tableOwnerName = schema.getName();
				String tablePureName = tableName;
				if(set.add(tableName) == false) {
					tablePureName = tableOwnerName + "_" + tablePureName;
				}
				
				
				
				//If names format like xxx.xxx means schema name prefixed
				
				
				table = factory.createTable();
				table.setOwner(tableOwnerName);
				table.setName(tablePureName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + table.getName() + ", owner=" + table.getOwner());
				}
				table.setSchema(schema);

				buildTableColumns(conn, catalog, schema, table);
				buildTablePK(conn, catalog, schema, table);
				buildTableFKs(conn, catalog, schema, table);
				buildTableIndexes(conn, catalog, schema, table);
			} catch (Exception ex) {
				LOG.error("", ex);
			}
			if (table != null) {
				schema.addTable(table);
//				allTables.add(table);
			}
		}
			
	}
	
	
	/**
	 * return a list of table name. for different database, this method may be
	 * needed to override
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getAllTableNames(final Connection conn, final Catalog catalog,
			final Schema schema) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllTableNames()");
		}
		ResultSet rs = null; //NOPMD
		List<String> tableNameList = new ArrayList<String>();
		try {
			PreparedStatement stmt = conn.prepareStatement(TABLE_QUERY);
			stmt.setString(1, schema.getName());
			stmt.setBoolean(2, false);

			rs = stmt.executeQuery();
			while (rs.next()) {
				String table_name = rs.getString("relname");
				tableNameList.add(table_name);
			}

			return tableNameList;
		} finally {
			Closer.close(rs);
		}
	}
	
	
	/**
	 * Extract Table's Indexes
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableIndexes(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		
		super.buildTableIndexes(conn, catalog, schema, table);
		
		List<Index> indexes = new ArrayList<Index>();
		boolean status = false;
		
		for (Index index : table.getIndexes()) {
			for (Index idx_reserve : indexes) {
				List<String> columns = idx_reserve.getColumnNames();
				if(idx_reserve.getColumnNames().equals(index.getColumnNames())) {
					status = true;
				}
			}
			if(!status) {
				indexes.add(index);
			}
			status =false;
		}
		table.setIndexes(indexes);
		
	}
	
	
	/**
	 * extract Table's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableColumns(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		super.buildTableColumns(conn, catalog, schema, table);
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableColumns()");
		}
		
		
		for(Column column: table.getColumns()) {
			if(column.getDataType().contains("serial")) {
				column.setAutoIncrement(true);
				column.setDefaultValue(null);
			}
		}
		
		PreparedStatement stmt = conn.prepareStatement(COLUMN_QUERY);
		stmt.setString(1, table.getName());

		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {

			String data_type = rs.getString("data_type");
			Column column = table.getColumnByName(rs.getString("column_name"));
			column.setDataType(data_type);
			if(data_type.equals("timestamp with time zone")) {
				column.setJdbcIDOfDataType(Types.TIMESTAMP_WITH_TIMEZONE);
			}else if(data_type.equals("USER-DEFINED")) {
				String udt_name = rs.getString("udt_name");
				if(udt_name.equals("geometry")) {
					column.setDataType(udt_name);
				}
			}
			
			
		}
		
		
	}
	
	/**
	 * extract Table's FK
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableFKs(final Connection conn, final Catalog catalog, final Schema schema,
			final Table table) throws SQLException {
		super.buildTableFKs(conn, catalog, schema, table);
		
		List<FK> fks = table.getFks();
		for (FK fk : fks) {
			if(fk.getUpdateRule() == DatabaseMetaData.importedKeyCascade) {
				fk.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
			}
		}
		
	}
	
	
	/**
	 * Fetch all views of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildViews(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildViews()");
		}
		List<String> viewNameList = getAllViewNames(conn, catalog, schema);
		for (String viewName : viewNameList) {
			String viewOwnerName = null;
			String viewPureName = null;
			String querySpec = null;
			if (viewName != null && viewName.indexOf(".") != -1) {
				String[] arr = viewName.split("\\.");
				viewOwnerName = schema.getName();
				viewPureName = arr[1];
			} else {
				viewOwnerName = schema.getName();
				viewPureName = viewName;
			}
			if (filter != null && filter.filter(schema.getName(), viewPureName)) {
				continue;
			}
			if (!isViewNameAccepted(viewName)) {
				continue;
			}
			
			querySpec = buildViewDDL(conn, viewOwnerName, viewName);
			

			final View view = factory.createView();
			view.setQuerySpec(querySpec);
			view.setOwner(viewOwnerName);
			view.setName(viewPureName);
			view.setSchema(schema);
			schema.addView(view);
			buildViewColumns(conn, catalog, schema, view);
		}
	}
	
	
	/**
	 * Fetch all stored Triggers of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	public void buildTriggers(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		
		List<Schema> schemaList = catalog.getSchemas();
		
		for (Schema sc : schemaList) {
			List<Trigger> triggers = getTriggers(conn, sc.getName());
			sc.setTriggers(triggers);
		}
		
	}
	
	/**
	 * Fetch all stored procedures and function of the given schema.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	public void buildProcedures(final Connection conn, final Catalog catalog,
			final Schema schema, IBuildSchemaFilter filter) throws SQLException {
		
		List<Schema> schemaList = catalog.getSchemas();

		for (Schema sc : schemaList) {
			// get procedures
			List<Procedure> procedures = getProcedures(conn, sc.getName());
			sc.setProcedures(procedures);
			
			// get functions
			List<Function> funcList = getFunctions(conn, sc.getName());
			sc.setFunctions(funcList);
		}
		
	}
	
	
	
	public String buildViewDDL(final Connection conn, String schema_name, String view_name) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(VIEW_QUERY);
		stmt.setString(1, view_name);
//		stmt.setString(2, schema_name);
		ResultSet rs = stmt.executeQuery();
		String ddl = "";
		while (rs.next()) {
			ddl = rs.getString("view_ddl");
		}
		if (ddl != "") {
			ddl = ddl.replaceAll("::\\w+|::\"\\w+\"", "");
			Pattern p1 = Pattern.compile("\"\\w+\"+\\(");
			Pattern p2 = Pattern.compile("\\w+");
			
			Matcher m1 = p1.matcher(ddl);
			while(m1.find()) {
				String s = m1.group();
				Matcher m2 = p2.matcher(s);
				while(m2.find()) {
					ddl = ddl.replace(s, m2.group()+"(");
				}
			}
			return ddl;
		}
		return null;
	}
	
	/**
	 * Fetch all sequences of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	public void buildSequence(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		List<Schema> schemaList = catalog.getSchemas();

		for (Schema sc : schemaList) {
			List<Sequence> sequences = getSequences(conn, sc.getName());
			sc.setSequenceList(sequences);
		}
	}
	
	private List<Sequence> getSequences(Connection conn, String schema_name) throws SQLException{
		
		PreparedStatement stmt = conn.prepareStatement(SEQUENCE_QUERY);
		stmt.setString(1, schema_name);
		ResultSet rs = stmt.executeQuery();
		List<Sequence> sequences = new ArrayList<Sequence>();
		
		while(rs.next()) {
			Sequence sequence = factory.createSequence(null, null, null, null, null, false, 0);
			sequence.setOwner(schema_name);
			sequence.setName(rs.getString("sequence_name"));
			sequence.setMaxValue(new BigInteger(rs.getString("maximum_value")));
			sequence.setMinValue(new BigInteger(rs.getString("minimum_value")));
			sequence.setIncrementBy(new BigInteger(rs.getString("increment")));
			sequence.setCurrentValue(new BigInteger(rs.getString("start_value")));
			sequence.setCycleFlag(rs.getString("cycle_option").equals("YES"));
			sequence.setNoMaxValue(false);
			sequence.setNoMinValue(false);
			sequences.add(sequence);
			
			// missing ddl
		}
		Closer.close(rs);
		Closer.close(stmt);
		return sequences;
		
	}
	
	private List<Trigger> getTriggers(Connection conn, String schema_name) throws SQLException {
		
		PreparedStatement stmt = conn.prepareStatement(TRIGGER_QUERY);
		stmt.setString(1, schema_name);
		ResultSet rs = stmt.executeQuery();
		List<Trigger> triggers = new ArrayList<Trigger>();
		
		while(rs.next()) {
			Trigger trigger = factory.createTrigger();
			
			String name = rs.getString("trigger_name");
			String event = rs.getString("event_manipulation");
			String action = rs.getString("action_statement");
			trigger.setName(name);
			String ddl = buildTriggerDDL(action, name, event);
			trigger.setDDL(ddl);
			
			triggers.add(trigger);
		}
		
		Closer.close(rs);
		Closer.close(stmt);
		
		return triggers;
		
	}
	
private List<Function> getFunctions(Connection conn, String schema_name) throws SQLException{
		
		PreparedStatement stmt = conn.prepareStatement(ROUTINE_QUERY);
		stmt.setString(1, "f");
		stmt.setString(2, schema_name);
		ResultSet rs = stmt.executeQuery();
		List<Function> functions = new ArrayList<Function>();
		Function function = factory.createFunction();
		while(rs.next()) {
			function.setName(rs.getString("name"));
			function.setFuncDDL(rs.getString("ddl"));
			functions.add(function);
		}
		
		Closer.close(rs);
		Closer.close(stmt);
		return functions;
		
	}
	
	private List<Procedure> getProcedures(Connection conn, String schema_name) throws SQLException{
		
		PreparedStatement stmt = conn.prepareStatement(ROUTINE_QUERY);
		stmt.setString(1, "p");
		stmt.setString(2, schema_name);
		ResultSet rs = stmt.executeQuery();
		List<Procedure> procedures = new ArrayList<Procedure>();
		Procedure procedure = factory.createProcedure();
		while(rs.next()) {
			procedure.setName(rs.getString("name"));
			procedure.setProcedureDDL(rs.getString("ddl"));
			procedures.add(procedure);
		}
		
		Closer.close(rs);
		Closer.close(stmt);
		return procedures;
		
	}
	
	

	private String buildTriggerDDL(String action, String name, String event) {

		String template = "create trigger %s before %s on %s \n %s";
		String ddlText = String.format(template, name, event, action);
		return ddlText;
		
	}
	
	
	public DatabaseType getDBType() {
		// TODO Auto-generated method stub
		return DatabaseType.POSTGRESQL;
	}

	@Override
	protected DBExportHelper getExportHelper() {
		// TODO Auto-generated method stub
		return null;
	}

}
