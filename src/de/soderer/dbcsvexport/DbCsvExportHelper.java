package de.soderer.dbcsvexport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.Utilities;

public class DbCsvExportHelper {
	public static List<String> getTablesToExport(Connection connection, DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();

			String tableQuery;
			if ("oracle".equals(dbCsvExportDefinition.getDbType())) {
				tableQuery = "SELECT DISTINCT table_name FROM all_tables WHERE owner NOT IN ('CTXSYS', 'DBSNMP', 'MDDATA', 'MDSYS', 'DMSYS', 'OLAPSYS', 'ORDPLUGINS', 'OUTLN', 'SI_INFORMATN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM')";
				for (String tablePattern : dbCsvExportDefinition.getSqlStatementOrTablelist().split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";
			} else if ("mysql".equals(dbCsvExportDefinition.getDbType())) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema')";
				for (String tablePattern : dbCsvExportDefinition.getSqlStatementOrTablelist().split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";
			} else if ("postgres".equals(dbCsvExportDefinition.getDbType()) || "postgressql".equals(dbCsvExportDefinition.getDbType())) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
				for (String tablePattern : dbCsvExportDefinition.getSqlStatementOrTablelist().split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";
			} else {
				throw new Exception("Unknown db vendor");
			}
			resultSet = statement.executeQuery(tableQuery);
			List<String> tableNamesToExport = new ArrayList<String>();
			while (resultSet.next()) {
				tableNamesToExport.add(resultSet.getString("table_name"));
			}
			return tableNamesToExport;
		} catch (Exception e) {
			throw e;
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static String getPrimaryKeyColumn(Connection connection, String tablename, DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		try {
			return DbUtilities.getPrimaryKeyColumn(connection, tablename);
		} catch (Exception e) {
			throw e;
		}
	}

	public static Connection createConnection(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		Class.forName(DbUtilities.getDriverClassName(dbCsvExportDefinition.getDbType()));

		int port;
		String[] hostParts = dbCsvExportDefinition.getHostname().split(":");
		if (hostParts.length == 2) {
			try {
				port = Integer.parseInt(hostParts[1]);
			} catch (Exception e) {
				throw new DbCsvExportException("Invalid port: " + hostParts[1]);
			}
		} else if ("oracle".equalsIgnoreCase(dbCsvExportDefinition.getDbType())) {
			port = 1521;
		} else if ("mysql".equalsIgnoreCase(dbCsvExportDefinition.getDbType())) {
			port = 3306;
		} else if ("postgres".equalsIgnoreCase(dbCsvExportDefinition.getDbType()) || "postgresql".equalsIgnoreCase(dbCsvExportDefinition.getDbType())) {
			port = 5432;
		} else {
			throw new Exception("Unknown db vendor");
		}

		return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbCsvExportDefinition.getDbType(), hostParts[0], port, dbCsvExportDefinition.getDbName()), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword());
	}
}
