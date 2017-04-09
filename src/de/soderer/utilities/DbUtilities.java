package de.soderer.utilities;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.collection.CaseInsensitiveMap;

public class DbUtilities {
	public static final String DOWNLOAD_LOCATION_MYSQL = "https://dev.mysql.com/downloads/connector/j/";
	public static final String DOWNLOAD_LOCATION_ORACLE = "http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html";
	public static final String DOWNLOAD_LOCATION_POSTGRESQL = "https://jdbc.postgresql.org/download.html";
	public static final String DOWNLOAD_LOCATION_FIREBIRD = "http://www.firebirdsql.org/en/jdbc-driver";
	public static final String DOWNLOAD_LOCATION_DERBY = "https://db.apache.org/derby/derby_downloads.html";
	public static final String DOWNLOAD_LOCATION_SQLITE = "https://bitbucket.org/xerial/sqlite-jdbc/downloads";
	public static final String DOWNLOAD_LOCATION_HSQL = "http://hsqldb.org/download";
	
	/**
	 * In an Oracle DB the statement "SELECT CURRENT_TIMESTAMP FROM DUAL" return this special Oracle type "oracle.sql.TIMESTAMPTZ",
	 * which is not listed in java.sql.Types, but can be read via ResultSet.getTimestamp(i) into a normal java.sql.Timestamp object
	 */
	public static final int ORACLE_TIMESTAMPTZ_TYPECODE = -101;
	
	public enum DbVendor {
		Oracle("oracle.jdbc.OracleDriver", 1521),
		MySQL("com.mysql.jdbc.Driver", 3306),
		PostgreSQL("org.postgresql.Driver", 5432),
		Firebird("org.firebirdsql.jdbc.FBDriver", 3050),
		SQLite("org.sqlite.JDBC", 0),
		Derby("org.apache.derby.jdbc.EmbeddedDriver", 0),
		HSQL("org.hsqldb.jdbc.JDBCDriver", 0);
		
		public static DbVendor getDbVendorByName(String dbVendorName) throws Exception {
			if ("oracle".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.Oracle;
			} else if ("mysql".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.MySQL;
			} else if ("postgres".equalsIgnoreCase(dbVendorName) || "postgresql".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.PostgreSQL;
			} else if ("sqlite".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.SQLite;
			} else if ("derby".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.Derby;
			} else if ("hsql".equalsIgnoreCase(dbVendorName) || "hypersql".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.HSQL;
			} else if ("firebird".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.Firebird;
			} else {
				throw new Exception("Unknown db vendor: " + dbVendorName);
			}
		}
		
		private String driverClassName;
		private int defaultPort;
		
		private DbVendor(String driverClassName, int defaultPort) {
			this.driverClassName = driverClassName;
			this.defaultPort = defaultPort;
		}
		
		public String getDriverClassName() {
			return driverClassName;
		}
		
		public int getDefaultPort() {
			return defaultPort;
		}
	}

	public static String generateUrlConnectionString(DbVendor dbVendor, String dbServerHostname, int dbServerPort, String dbName) throws Exception {
		if (DbVendor.Oracle == dbVendor) {
			if (dbName.startsWith("/")) {
				// Some Oracle databases only accept the SID separated by a "/"
				return "jdbc:oracle:thin:@" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + dbName;
			} else {
				return "jdbc:oracle:thin:@" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + dbName;
			}
		} else if (DbVendor.MySQL == dbVendor) {
			return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
		} else if (DbVendor.PostgreSQL == dbVendor) {
			return "jdbc:postgresql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName;
		} else if (DbVendor.SQLite == dbVendor) {
			return "jdbc:sqlite:" + dbName.replace("~", System.getProperty("user.home"));
		} else if (DbVendor.Derby == dbVendor) {
			return "jdbc:derby:" + dbName.replace("~", System.getProperty("user.home"));
		} else if (DbVendor.Firebird == dbVendor) {
			return "jdbc:firebirdsql:" + dbServerHostname + "/" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + dbName.replace("~", System.getProperty("user.home"));
		} else if (DbVendor.HSQL == dbVendor) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (dbName.startsWith("/")) {
				return "jdbc:hsqldb:file:" + dbName + ";shutdown=true";
			} else if (Utilities.isNotBlank(dbServerHostname)) {
				if (!dbServerHostname.toLowerCase().startsWith("http")) {
					dbServerHostname = "http://" + dbServerHostname;
				}
				if (dbServerHostname.toLowerCase().startsWith("https://") && dbServerPort == 443) {
					dbServerPort = -1;
				} else if (dbServerHostname.toLowerCase().startsWith("http://") && dbServerPort == 80) {
					dbServerPort = -1;
				}
				return "jdbc:hsqldb:" + dbServerHostname + (dbServerPort <= 0 ? "" : ":" + dbServerPort) + "/" + dbName;
			} else {
				return "jdbc:hsqldb:mem:" + dbName;
			}
		} else {
			throw new Exception("Unknown db vendor");
		}
	}
	
	public static Connection createNewDatabase(DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
		
		Class.forName(dbVendor.getDriverClassName());
	
		if (dbVendor == DbVendor.SQLite) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (new File(dbPath).exists()) {
				throw new Exception("SQLite db file '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbPath));
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (new File(dbPath).exists()) {
				throw new Exception("Derby db directory '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbPath) + ";create=true");
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (dbPath.startsWith("/")) {
				if (getFilesByPattern(new File(dbPath.substring(0, dbPath.lastIndexOf("/"))), dbPath.substring(dbPath.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() > 0) {
					throw new Exception("HSQL db '" + dbPath + "' already exists");
				}
			}
			
			// Logger must be kept in a local variable for making it work
			Logger dbLogger = Logger.getLogger("hsqldb.db");
			dbLogger.setLevel(Level.WARNING);
			
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbPath));
		} else {
			throw new Exception("Invalid db vendor '" + dbVendor.toString() + "'. Only SQLite, HSQL or Derby db can be created this way.");
		}
	}
	
	public static void deleteDatabase(DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
	
		if (dbVendor == DbVendor.SQLite) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (new File(dbPath).exists()) {
				new File(dbPath).delete();
			}
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (new File(dbPath).exists()) {
				Utilities.delete(new File(dbPath));
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = dbPath.replace("~", System.getProperty("user.home"));
			if (dbPath.startsWith("/")) {
				File baseDirectory = new File(dbPath.substring(0, dbPath.lastIndexOf("/")));
				String basename = dbPath.substring(dbPath.lastIndexOf("/") + 1);
				for (File fileToDelete : baseDirectory.listFiles()) {
					if (fileToDelete.getName().startsWith(basename)) {
						Utilities.delete(fileToDelete);
					}
				}
			}
		} else {
			throw new Exception("Invalid db vendor '" + dbVendor.toString() + "'. Only SQLite, HSQL or Derby db can be deleted this way.");
		}
	}
	
	public static Connection createConnection(DbVendor dbVendor, String hostname, String dbName, String userName, char[] password) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
		
		Class.forName(dbVendor.getDriverClassName());
	
		if (dbVendor == DbVendor.SQLite) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (!new File(dbName).exists()) {
				throw new Exception("SQLite db file '" + dbName + "' is not available");
			} else if (!new File(dbName).isFile()) {
				throw new Exception("SQLite db file '" + dbName + "' is not a file");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbName));
		} else if (dbVendor == DbVendor.Derby) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (!new File(dbName).exists()) {
				throw new Exception("Derby db directory '" + dbName + "' is not available");
			} else if (!new File(dbName).isDirectory()) {
				throw new Exception("Derby db directory '" + dbName + "' is not a directory");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbName));
		} else if (dbVendor == DbVendor.HSQL) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (dbName.startsWith("/")) {
				if (getFilesByPattern(new File(dbName.substring(0, dbName.lastIndexOf("/"))), dbName.substring(dbName.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() <= 0) {
					throw new Exception("HSQL db directory '" + dbName + "' is not a directory");
				}
			}
			int port;
			String[] hostParts = hostname.split(":");
			if (hostParts.length == 2) {
				try {
					port = Integer.parseInt(hostParts[1]);
				} catch (Exception e) {
					throw new Exception("Invalid port: " + hostParts[1]);
				}
			} else {
				port = dbVendor.getDefaultPort();
			}
			
			// Logger must be kept in a local variable for making it work
			Logger dbLogger = Logger.getLogger("hsqldb.db");
			dbLogger.setLevel(Level.WARNING);
			
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), (Utilities.isNotEmpty(userName) ? userName : "SA"), (password != null ? new String(password) : ""));
		} else {
			int port;
			String[] hostParts = hostname.split(":");
			if (hostParts.length == 2) {
				try {
					port = Integer.parseInt(hostParts[1]);
				} catch (Exception e) {
					throw new Exception("Invalid port: " + hostParts[1]);
				}
			} else {
				port = dbVendor.getDefaultPort();
			}
		
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), userName, new String(password));
		}
	}
	
	public static int readoutInOutputStream(DataSource dataSource, String statementString, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutInOutputStream(connection, statementString, outputStream, encoding, separator, stringQuote);
		}
	}
	
	public static int readoutInOutputStream(Connection connection, String statementString, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		try (Statement statement = connection.createStatement()) {
			try (ResultSet resultSet = statement.executeQuery(statementString)) {
				try (CsvWriter csvWriter = new CsvWriter(outputStream, encoding, separator, stringQuote)) {
					ResultSetMetaData metaData = resultSet.getMetaData();

					List<String> headers = new ArrayList<>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						headers.add(metaData.getColumnName(i));
					}
					csvWriter.writeValues(headers);

					while (resultSet.next()) {
						List<String> values = new ArrayList<String>();
						for (int i = 1; i <= metaData.getColumnCount(); i++) {
							if (metaData.getColumnType(i) == Types.BLOB
									|| metaData.getColumnType(i) == Types.BINARY
									|| metaData.getColumnType(i) == Types.VARBINARY
									|| metaData.getColumnType(i) == Types.LONGVARBINARY) {
								Blob blob = resultSet.getBlob(i);
								if (resultSet.wasNull()) {
									values.add("");
								} else {
									try (InputStream input = blob.getBinaryStream()) {
										byte[] data = Utilities.toByteArray(input);
										values.add(Base64.getEncoder().encodeToString(data));
									}
								}
							} else {
								values.add(resultSet.getString(i));
							}
						}
						csvWriter.writeValues(values);
					}

					return csvWriter.getWrittenLines() - 1;
				}
			}
		}
	}

	public static String readout(DataSource dataSource, String statementString, char separator, Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readout(connection, statementString, separator, stringQuote);
		}
	}
    
    public static String readout(Connection connection, String statementString, char separator, Character stringQuote) throws Exception {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		readoutInOutputStream(connection, statementString, outputStream, "UTF-8", separator, stringQuote);
		return new String(outputStream.toByteArray(), "UTF-8");
	}
    
    public static String readoutTable(DataSource dataSource, String tableName, char separator, Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutTable(connection, tableName, separator, stringQuote);
		}
    }

	public static String readoutTable(Connection connection, String tableName, char separator, Character stringQuote) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			List<String> columnNames = getColumnNames(connection, tableName);
			Collections.sort(columnNames);
			List<String> keyColumnNames = getPrimaryKeyColumns(connection, tableName);
			Collections.sort(keyColumnNames);
			List<String> readoutColumns = new ArrayList<String>();
			readoutColumns.addAll(keyColumnNames);
			for (String columnName : columnNames) {
				if (!Utilities.containsIgnoreCase(readoutColumns, columnName)) {
					readoutColumns.add(columnName);
				}
			}
			String orderPart = "";
			if (!keyColumnNames.isEmpty()) {
				orderPart = " ORDER BY " + Utilities.join(keyColumnNames, ", ");
			}
			return readout(connection, "SELECT " + Utilities.join(readoutColumns, ", ") + " FROM " + tableName + orderPart, separator, stringQuote);
		}
	}

	public static DbVendor getDbVendor(DataSource dataSource) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getDbVendor(connection);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot check db vendor: " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	public static DbVendor getDbVendor(Connection connection) throws Exception {
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				String productName = databaseMetaData.getDatabaseProductName();
				if (productName != null && productName.toLowerCase().contains("oracle")) {
					return DbVendor.Oracle;
				} else if (productName != null && productName.toLowerCase().contains("mysql")) {
					return DbVendor.MySQL;
				} else if (productName != null && productName.toLowerCase().contains("postgres")) {
					return DbVendor.PostgreSQL;
				} else if (productName != null && productName.toLowerCase().contains("sqlite")) {
					return DbVendor.SQLite;
				} else if (productName != null && productName.toLowerCase().contains("derby")) {
					return DbVendor.Derby;
				} else if (productName != null && productName.toLowerCase().contains("hsql")) {
					return DbVendor.HSQL;
				} else if (productName != null && productName.toLowerCase().contains("firebird")) {
					return DbVendor.Firebird;
				} else {
					throw new Exception("Unknown db vendor: " + productName);
				}
			} else {
				throw new Exception("Undetectable db vendor");
			}
		} catch (SQLException e) {
			throw new Exception("Error while detecting db vendor: " + e.getMessage(), e);
		}
	}

	public static String getDbUrl(DataSource dataSource) throws SQLException {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getDbUrl(connection);
		} finally {
			closeQuietly(connection);
		}
	}

	public static String getDbUrl(Connection connection) {
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				return databaseMetaData.getURL();
			} else {
				return null;
			}
		} catch (SQLException e) {
			return null;
		}
	}

	public static boolean checkTableAndColumnsExist(Connection connection, String tableName, String... columns) throws Exception {
		return checkTableAndColumnsExist(connection, tableName, false, columns);
	}

	public static boolean checkTableAndColumnsExist(DataSource dataSource, String tableName, boolean throwExceptionOnError, String... columns) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return checkTableAndColumnsExist(connection, tableName, throwExceptionOnError, columns);
		} finally {
			closeQuietly(connection);
		}
	}

	public static boolean checkTableAndColumnsExist(Connection connection, String tableName, boolean throwExceptionOnError, String... columns) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();

			// Check if table exists
			try {
				resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0");
			} catch (Exception e) {
				if (throwExceptionOnError) {
					throw new Exception("Table '" + tableName + "' does not exist");
				} else {
					return false;
				}
			}

			// Check if all needed columns exist
			Set<String> dbTableColumns = new HashSet<String>();
			ResultSetMetaData metaData = resultSet.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				dbTableColumns.add(metaData.getColumnName(i).toUpperCase());
			}
			if (columns != null) {
				for (String column : columns) {
					if (column != null && !dbTableColumns.contains(column.toUpperCase())) {
						if (throwExceptionOnError) {
							throw new Exception("Column '" + column + "' does not exist in table '" + tableName + "'");
						} else {
							return false;
						}
					}
				}
			}
			return true;
		} finally {
			closeQuietly(resultSet);
			resultSet = null;
			closeQuietly(statement);
			statement = null;
		}
	}
	
	public static boolean checkTableExist(Connection connection, String tableName) throws Exception {
		return checkTableExist(connection, tableName, false);
	}

	public static boolean checkTableExist(Connection connection, String tableName, boolean throwExceptionOnError) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();

			// Check if table exists
			try {
				resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0");
			} catch (Exception e) {
				if (throwExceptionOnError) {
					throw new Exception("Table '" + tableName + "' does not exist");
				} else {
					return false;
				}
			}
			return true;
		} finally {
			closeQuietly(resultSet);
			resultSet = null;
			closeQuietly(statement);
			statement = null;
		}
	}

	public static String callStoredProcedureWithDbmsOutput(Connection connection, String procedureName, Object... parameters) throws SQLException {
		CallableStatement callableStatement = null;
		try {
			callableStatement = connection.prepareCall("begin dbms_output.enable(:1); end;");
			callableStatement.setLong(1, 10000);
			callableStatement.executeUpdate();
			callableStatement.close();
			callableStatement = null;

			if (parameters != null) {
				callableStatement = connection.prepareCall("{call " + procedureName + "(" + TextUtilities.repeatString("?", parameters.length, ", ") + ")}");
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].getClass() == Date.class) {
						parameters[i] = new java.sql.Date(((Date) parameters[i]).getTime());
					}
				}
				for (int i = 0; i < parameters.length; i++) {
					callableStatement.setObject(i + 1, parameters[i]);
				}
			} else {
				callableStatement = connection.prepareCall("{call " + procedureName + "()}");
			}
			callableStatement.execute();
			callableStatement.close();
			callableStatement = null;

			callableStatement = connection.prepareCall("declare " + "    l_line varchar2(255); " + "    l_done number; " + "    l_buffer long; " + "begin " + "  loop "
					+ "    exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; " + "    dbms_output.get_line( l_line, l_done ); " + "    l_buffer := l_buffer || l_line || chr(10); "
					+ "  end loop; " + " :done := l_done; " + " :buffer := l_buffer; " + "end;");

			callableStatement.registerOutParameter(2, Types.INTEGER);
			callableStatement.registerOutParameter(3, Types.VARCHAR);
			StringBuffer dbmsOutput = new StringBuffer(1024);
			while (true) {
				callableStatement.setInt(1, 32000);
				callableStatement.executeUpdate();
				dbmsOutput.append(callableStatement.getString(3).trim());
				if (callableStatement.getInt(2) == 1) {
					break;
				}
			}
			callableStatement.close();
			callableStatement = null;

			callableStatement = connection.prepareCall("begin dbms_output.disable; end;");
			callableStatement.executeUpdate();
			callableStatement.close();
			callableStatement = null;

			return dbmsOutput.toString();
		} finally {
			closeQuietly(callableStatement);
		}
	}

	public static void closeQuietly(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static void closeQuietly(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static void closeQuietly(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static String getResultAsTextTable(DataSource datasource, String selectString) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = datasource.getConnection();
			preparedStatement = connection.prepareStatement(selectString);
			resultSet = preparedStatement.executeQuery();
			TextTable textTable = new TextTable();
			for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
				textTable.addColumn(resultSet.getMetaData().getColumnName(columnIndex));
			}
			while (resultSet.next()) {
				textTable.startNewLine();
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getString(columnIndex) != null) {
						textTable.addValueToCurrentLine(resultSet.getString(columnIndex));
					} else {
						textTable.addValueToCurrentLine("<null>");
					}
				}
			}
			return textTable.toString();
		} finally {
			closeQuietly(resultSet);
			closeQuietly(preparedStatement);
			closeQuietly(connection);
		}
	}
	
	public static List<String> getColumnNames(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnNames");
		}
		
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getColumnNames(connection, tableName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot read columns for table " + tableName + ": " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	public static List<String> getColumnNames(Connection connection, String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				List<String> columnNamesList = new ArrayList<String>();
				for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
					columnNamesList.add(resultSet.getMetaData().getColumnName(i));
				}
				return columnNamesList;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
			}
		}
	}

	public static DbColumnType getColumnDataType(Connection connection, String tableName, String columnName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDataType");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataType");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDataType");
		} else {
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				int characterLength;
				int numericPrecision;
				int numericScale;
				boolean isNullable;
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
					String sql = "SELECT NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) as data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE LOWER(table_name) = LOWER(?) AND LOWER(column_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					resultSet = preparedStatement.executeQuery();

					if (resultSet.next()) {
						characterLength = resultSet.getInt("data_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						numericPrecision = resultSet.getInt("data_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						numericScale = resultSet.getInt("data_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						isNullable = resultSet.getString("nullable").equalsIgnoreCase("y");

						return new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable);
					} else {
						return null;
					}
				} else if (DbVendor.MySQL == dbVendor) {
					String sql = "SELECT data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?) AND LOWER(column_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					resultSet = preparedStatement.executeQuery();

					if (resultSet.next()) {
						characterLength = resultSet.getInt("character_maximum_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						numericPrecision = resultSet.getInt("numeric_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						numericScale = resultSet.getInt("numeric_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						return new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable);
					} else {
						return null;
					}
				} else {
					throw new Exception("Unsupported db vendor");
				}
			} catch (Exception e) {
				return null;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
			}
		}
	}

	public static DbColumnType getColumnDataType(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataType");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataType");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDataType");
		} else {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				return getColumnDataType(connection, tableName, columnName);
			} catch (Exception e) {
				return null;
			} finally {
				closeQuietly(connection);
			}
		}
	}
	
	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				return getColumnDataTypes(connection, tableName);
			} catch (Exception e) {
				throw e;
			} finally {
				closeQuietly(connection);
			}
		}
	}

	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(Connection connection, String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				CaseInsensitiveMap<DbColumnType> returnMap = new CaseInsensitiveMap<DbColumnType>();
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
					String sql = "SELECT column_name, NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) as data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE LOWER(table_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						int characterLength = resultSet.getInt("data_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("data_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("data_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("nullable").equalsIgnoreCase("y");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.MySQL == dbVendor) {
					String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						long characterLength = resultSet.getLong("character_maximum_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("numeric_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("numeric_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.HSQL == dbVendor) {
					String sql = "SELECT * FROM information_schema.system_columns WHERE LOWER(table_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						long characterLength = resultSet.getLong("column_size");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("column_size");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("decimal_digits");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("type_name"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.Derby == dbVendor) {
					String sql = "SELECT * FROM sys.systables, sys.syscolumns WHERE tableid = referenceid AND LOWER(tablename) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						String type = resultSet.getString("columndatatype");
						
						long characterLength = -1;
						int numericPrecision = -1;
						int numericScale = -1;
						
						boolean isNullable;
						if (type.toLowerCase().endsWith("not null")) {
							isNullable = false;
							type = type.substring(0, type.length() - 8).trim();
						} else {
							isNullable = true;
						}
						
						if (type.contains("(")) {
							characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
							type = type.substring(0, type.indexOf("("));
						}

						returnMap.put(resultSet.getString("columnname"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.Firebird == dbVendor) {
					String sql = "SELECT rf.rdb$field_name, f.rdb$field_type, f.rdb$field_sub_type, f.rdb$field_length, f.rdb$field_scale, f.rdb$null_flag"
						+ " FROM rdb$fields f JOIN rdb$relation_fields rf ON rf.rdb$field_source = f.rdb$field_name WHERE rf.rdb$relation_name = ?";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName.toUpperCase());
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						long characterLength = resultSet.getLong("rdb$field_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("rdb$field_length");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("rdb$field_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getObject("rdb$null_flag") == null;
						
						String dataType;
						switch (resultSet.getInt("rdb$field_type")) {
							case 7: dataType = "SMALLINT";
								break;
							case 8: dataType = "INTEGER";
								break;
							case 10: dataType = "FLOAT";
								break;
							case 12: dataType = "DATE";
								break;
							case 13: dataType = "TIME";
								break;
							case 14: dataType = "CHAR";
								break;
							case 16: dataType = "BIGINT";
								break;
							case 27: dataType = "DOUBLE PRECISION";
								break;
							case 35: dataType = "TIMESTAMP";
								break;
							case 37: dataType = "VARCHAR";
								break;
							case 261:
								if (resultSet.getInt("rdb$field_sub_type") == 1) {
									dataType = "CLOB";
								} else {
									dataType = "BLOB";
								}
								break;
							default: dataType = getTypeNameById(resultSet.getInt("rdb$field_type"));
						}

						returnMap.put(resultSet.getString("rdb$field_name").trim(), new DbColumnType(dataType, characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.SQLite == dbVendor) {
					String sql = "PRAGMA table_info(" + tableName + ")";
					preparedStatement = connection.prepareStatement(sql);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						long characterLength = -1;
						int numericPrecision = -1;
						int numericScale = -1;
						boolean isNullable = resultSet.getInt("notnull") > 0;
						
						String type = resultSet.getString("type");
						
						if (type.contains("(")) {
							characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
							type = type.substring(0, type.indexOf("("));
						}

						returnMap.put(resultSet.getString("name"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.PostgreSQL == dbVendor) {
					String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA() AND LOWER(table_name) = LOWER(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						long characterLength = resultSet.getLong("character_maximum_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("numeric_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("numeric_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else {
					throw new Exception("Unsupported db vendor");
				}
				return returnMap;
			} catch (Exception e) {
				throw e;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
			}
		}
	}

	public static int getColumnCount(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnCount");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnCount");
		} else {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				return getColumnCount(connection, tableName);
			} finally {
				closeQuietly(connection);
			}
		}
	}

	public static int getColumnCount(Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnCount");
		} else {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				return resultSet.getMetaData().getColumnCount();
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
			}
		}
	}

	public static int getTableEntriesCount(Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getTableEntriesNumber");
		} else {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = connection.createStatement();
				String sql = "SELECT COUNT(*) FROM " + getSQLSafeString(tableName);
				resultSet = statement.executeQuery(sql);
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
			}
		}
	}

	public static boolean containsColumnName(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for containsColumnName");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for containsColumnName");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for containsColumnName");
		} else {
			Connection connection = null;
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getMetaData().getColumnName(columnIndex).equalsIgnoreCase(columnName.trim())) {
						return true;
					}
				}
				return false;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static int getColumnDataTypeLength(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getDefaultValueOf");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getDefaultValueOf");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getDefaultValueOf");
		} else {
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				connection = dataSource.getConnection();
				String sql;
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					sql = "SELECT data_length FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName.toUpperCase());
					preparedStatement.setString(2, columnName.toUpperCase());
				} else {
					sql = "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
				}
				resultSet = preparedStatement.executeQuery();
				if (resultSet.next()) {
					int returnValue = resultSet.getInt(1);
					if (resultSet.next()) {
						throw new Exception("Cannot retrieve column datatypelength");
					} else {
						return returnValue;
					}
				} else {
					throw new Exception("Cannot retrieve column datatypelength");
				}
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
				closeQuietly(connection);
			}
		}
	}

	public static String getColumnDefaultValue(DataSource dataSource, String tableName, String columnName) throws Exception {
		try {
			if (dataSource == null) {
				throw new Exception("Invalid empty dataSource for getDefaultValueOf");
			} else if (Utilities.isBlank(tableName)) {
				throw new Exception("Invalid empty tableName for getDefaultValueOf");
			} else if (Utilities.isBlank(columnName)) {
				throw new Exception("Invalid empty columnName for getDefaultValueOf");
			} else {
				Connection connection = null;
				PreparedStatement preparedStatement = null;
				ResultSet resultSet = null;
				try {
					connection = dataSource.getConnection();
					String sql;
					DbVendor dbVendor = getDbVendor(connection);
					if (DbVendor.Oracle == dbVendor) {
						sql = "SELECT data_default FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
						preparedStatement = connection.prepareStatement(sql);
						preparedStatement.setString(1, tableName.toUpperCase());
						preparedStatement.setString(2, columnName.toUpperCase());
						resultSet = preparedStatement.executeQuery();
						if (resultSet.next()) {
							String defaultvalue = resultSet.getString(1);
							String returnValue;
							if (defaultvalue == null || "null".equalsIgnoreCase(defaultvalue)) {
								returnValue = null;
							} else if (defaultvalue.startsWith("'") && defaultvalue.endsWith("'")) {
								returnValue = defaultvalue.substring(1, defaultvalue.length() - 1);
							} else {
								returnValue = defaultvalue;
							}
							if (resultSet.next()) {
								throw new Exception("Cannot retrieve column datatype");
							} else {
								return returnValue;
							}
						} else {
							throw new Exception("Cannot retrieve column datatype");
						}
					} else {
						sql = "SELECT column_default FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
						preparedStatement = connection.prepareStatement(sql);
						preparedStatement.setString(1, tableName);
						preparedStatement.setString(2, columnName);
						resultSet = preparedStatement.executeQuery();
						if (resultSet.next()) {
							String returnValue = resultSet.getString(1);
							if (resultSet.next()) {
								throw new Exception("Cannot retrieve column datatype");
							} else {
								return returnValue;
							}
						} else {
							throw new Exception("Cannot retrieve column datatype");
						}
					}

				} finally {
					closeQuietly(resultSet);
					closeQuietly(preparedStatement);
					closeQuietly(connection);
				}
			}
		} catch (Exception e) {
			throw e;
		}
	}

	public static String getDateDefaultValue(DataSource dataSource, String fieldDefault) throws Exception {
		DbVendor dbVendor = getDbVendor(dataSource);
		if (fieldDefault.equalsIgnoreCase("sysdate") || fieldDefault.equalsIgnoreCase("sysdate()") || fieldDefault.equalsIgnoreCase("current_timestamp")) {
			if (dbVendor == DbVendor.Oracle) {
				return "current_timestamp";
			} else if (dbVendor == DbVendor.MySQL) {
				return "sysdate()";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		} else {
			if (dbVendor == DbVendor.Oracle) {
				return "to_date('" + fieldDefault + "', 'DD.MM.YYYY')";
			} else if (dbVendor == DbVendor.MySQL) {
				return "'" + fieldDefault + "'";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		}
	}

	public static boolean addColumnToDbTable(DataSource dataSource, String tablename, String fieldname, String fieldType, int length, String fieldDefault, boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String addColumnStatement = "ALTER TABLE " + tablename + " ADD (" + fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				addColumnStatement += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if (fieldType.startsWith("VARCHAR")) {
					addColumnStatement += " DEFAULT '" + fieldDefault + "'";
				} else if (fieldType.equalsIgnoreCase("DATE")) {
					addColumnStatement += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					addColumnStatement += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				addColumnStatement += " NOT NULL";
			}

			addColumnStatement += ")";

			Connection connection = null;
			Statement statement = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				statement.executeUpdate(addColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
			} finally {
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static boolean alterColumnTypeInDbTable(DataSource dataSource, String tablename, String fieldname, String fieldType, int length, String fieldDefault, boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (!containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String changeColumnStatementPart = fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				changeColumnStatementPart += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if (fieldType.equalsIgnoreCase("VARCHAR")) {
					changeColumnStatementPart += " DEFAULT '" + fieldDefault + "'";
				} else if (fieldType.equalsIgnoreCase("DATE")) {
					changeColumnStatementPart += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					changeColumnStatementPart += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				changeColumnStatementPart += " NOT NULL";
			}

			String changeColumnStatement;
			DbVendor dbVendor = getDbVendor(dataSource);
			if (DbVendor.Oracle == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY (" + changeColumnStatementPart + ")";
			} else if (DbVendor.MySQL == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY " + changeColumnStatementPart;
			} else {
				throw new Exception("Unsupported db vendor");
			}

			Connection connection = null;
			Statement statement = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				statement.executeUpdate(changeColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
			} finally {
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	private static String getSQLSafeString(String value) {
		if (value == null) {
			return null;
		} else {
			return value.replace("'", "''");
		}
	}

	public static boolean checkOracleTablespaceExists(DataSource dataSource, String tablespaceName) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return checkOracleTablespaceExists(connection, tablespaceName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	/**
	 * Check if oracle tablespace exists
	 *
	 * @param connection
	 * @param tablespaceName
	 * @return
	 * @throws Exception 
	 */
	public static boolean checkOracleTablespaceExists(Connection connection, String tablespaceName) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		if (dbVendor == DbVendor.Oracle && tablespaceName != null) {
			PreparedStatement statement = null;
			ResultSet resultSet = null;

			try {
				statement = connection.prepareStatement("SELECT COUNT(*) FROM dba_tablespaces WHERE LOWER(tablespace_name) = ?");
				statement.setString(1, tablespaceName.toLowerCase());

				resultSet = statement.executeQuery();

				return resultSet.getInt(1) > 0;
			} catch (Exception e) {
				throw new RuntimeException("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
			} finally {
				closeQuietly(resultSet);
				resultSet = null;
				closeQuietly(statement);
				statement = null;
			}
		} else {
			return false;
		}
	}

	public static List<String> getPrimaryKeyColumns(DataSource dataSource, String tableName) {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getPrimaryKeyColumns(connection, tableName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	public static List<String> getPrimaryKeyColumns(Connection connection, String tableName) {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			ResultSet resultSet = null;
			try {
				if (getDbVendor(connection) == DbVendor.Oracle || getDbVendor(connection) == DbVendor.HSQL || getDbVendor(connection) == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}
				
				DatabaseMetaData metaData = connection.getMetaData();
				resultSet = metaData.getPrimaryKeys(connection.getCatalog(), null, tableName);

				List<String> returnList = new ArrayList<String>();
				while (resultSet.next()) {
					returnList.add(resultSet.getString("COLUMN_NAME"));
				}
				return returnList;
			} catch (Exception e) {
				throw new RuntimeException("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
			} finally {
				closeQuietly(resultSet);
				resultSet = null;
			}
		}
	}

	public static List<List<String>> getForeignKeys(Connection connection, String tableName) {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			ResultSet resultSet = null;
			try {
				if (getDbVendor(connection) == DbVendor.Oracle || getDbVendor(connection) == DbVendor.HSQL || getDbVendor(connection) == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}
				
				DatabaseMetaData metaData = connection.getMetaData();
				resultSet = metaData.getImportedKeys(connection.getCatalog(), null, tableName);

				List<List<String>> returnList = new ArrayList<List<String>>();
				while (resultSet.next()) {
					List<String> nextForeignKey = new ArrayList<String>();
					nextForeignKey.add(resultSet.getString("FKTABLE_NAME"));
					nextForeignKey.add(resultSet.getString("FKCOLUMN_NAME"));
					nextForeignKey.add(resultSet.getString("PKTABLE_NAME"));
					nextForeignKey.add(resultSet.getString("PKCOLUMN_NAME"));
					returnList.add(nextForeignKey);
				}
				return returnList;
			} catch (Exception e) {
				throw new RuntimeException("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
			} finally {
				closeQuietly(resultSet);
				resultSet = null;
			}
		}
	}
	
	/**
	 * tablePatternExpression contains a comma-separated list of tablenames with wildcards *? and !(not, before tablename)
	 * 
	 * @param connection
	 * @param tablePatternExpression
	 * @return
	 * @throws Exception
	 */
	public static List<String> getAvailableTables(Connection connection, String tablePatternExpression) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			DbVendor dbVendor = getDbVendor(connection);
			statement = connection.createStatement();

			String tableQuery;
			if (DbVendor.Oracle == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM all_tables WHERE owner NOT IN ('CTXSYS', 'DBSNMP', 'MDDATA', 'MDSYS', 'DMSYS', 'OLAPSYS', 'ORDPLUGINS', 'OUTLN', 'SI_INFORMATN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
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

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.MySQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
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

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.PostgreSQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.SQLite == dbVendor) {
				tableQuery = "SELECT name FROM sqlite_master WHERE type = 'table'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY name";
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.Derby == dbVendor) {
				tableQuery = "SELECT tablename FROM sys.systables WHERE tabletype = 'T'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND tablename NOT LIKE '" + tablePattern.substring(1) + "' {ESCAPE '\\'}";
						} else {
							tableQuery += " AND tablename LIKE '" + tablePattern + "' {ESCAPE '\\'}";
						}
					}
				}
				tableQuery += " ORDER BY tablename";
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("tablename"));
				}
				return tableNamesToExport;
			} else if (DbVendor.Firebird == dbVendor) {
				tableQuery = "SELECT TRIM(rdb$relation_name) AS table_name FROM rdb$relations WHERE rdb$view_blr IS NULL AND (rdb$system_flag IS NULL OR rdb$system_flag = 0)";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND TRIM(rdb$relation_name) NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND TRIM(rdb$relation_name) LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY rdb$relation_name";
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.HSQL == dbVendor) {
				tableQuery = "SELECT table_name FROM information_schema.system_tables WHERE table_type = 'TABLE' AND table_schem = 'PUBLIC'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
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
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else {
				throw new Exception("Unknown db vendor");
			}
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
	
	private static List<File> getFilesByPattern(File startDirectory, String patternString, boolean traverseCompletely) {
		return getFilesByPattern(startDirectory, Pattern.compile(patternString), traverseCompletely);
	}

	private static List<File> getFilesByPattern(File startDirectory, Pattern pattern, boolean traverseCompletely) {
		List<File> files = new ArrayList<File>();
		if (startDirectory.isDirectory()) {
			for (File file : startDirectory.listFiles()) {
				if (file.isDirectory()) {
					files.add(file);
					if (traverseCompletely) {
						files.addAll(getFilesByPattern(file, pattern, traverseCompletely));
					}
				} else if (file.isFile() && pattern.matcher(file.getName()).matches()) {
					files.add(file);
				}
			}
		}
		return files;
	}

	public static void createTable(Connection connection, String tablename, Map<String, DbColumnType> columnsAndTypes, List<String> keyColumns) throws Exception {
		boolean hasKeyColumns = false;
		if  (keyColumns != null) {
			for (String keyColumn : keyColumns) {
				if (Utilities.isNotBlank(keyColumn)) {
					keyColumn = keyColumn.trim();
					if (Utilities.startsWithCaseinsensitive(keyColumn, "lower(") && keyColumn.endsWith(")")) {
						keyColumn = keyColumn.substring(6, keyColumn.length() - 1).trim();
					}
					
					if (!columnsAndTypes.containsKey(keyColumn)) {
						throw new Exception("Cannot create table. Keycolumn '" + keyColumn + "' is not included in column types");
					} else {
						hasKeyColumns = true;
					}
				}
			}
		}
		
		Statement statement = null;
		try {
			DbVendor dbVendor = DbUtilities.getDbVendor(connection);
			
			String columnPart = "";
			for (Entry<String, DbColumnType> columnAndType : columnsAndTypes.entrySet()) {
				if (columnPart.length() > 0) {
					columnPart += ", ";
				}
				String dataType = DbUtilities.getDataType(dbVendor, columnAndType.getValue().getSimpleDataType());
				int dataLength = dataType.toLowerCase().contains("varchar") ? (int) columnAndType.getValue().getCharacterLength() : 0;
				columnPart += columnAndType.getKey() + " " + dataType + (dataLength > 0 ? "(" + dataLength + ")" : "");
			}
			
			statement = connection.createStatement();
			String primaryKeyPart = "";
			if (Utilities.isNotEmpty(keyColumns) && hasKeyColumns) {
				String keyColumnPart = "";
				for (String keyColumn : keyColumns) {
					if (keyColumnPart.length() > 0) {
						keyColumnPart += ", ";
					}
					keyColumn = keyColumn.trim();
					if (Utilities.startsWithCaseinsensitive(keyColumn, "lower(") && keyColumn.endsWith(")")) {
						keyColumn = keyColumn.substring(6, keyColumn.length() - 1).trim();
					}
					keyColumnPart += keyColumn;
				}
				
				primaryKeyPart = ", PRIMARY KEY (" + keyColumnPart + ")";
			}
			statement.execute("CREATE TABLE " + tablename + " (" + columnPart + primaryKeyPart + ")");
			if (getDbVendor(connection) == DbVendor.Derby) {
				connection.commit();
			}
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	private static String getDataType(DbVendor dbVendor, SimpleDataType simpleDataType) throws Exception {
		if (dbVendor == DbVendor.Oracle) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case Date: return "TIMESTAMP";
				case Integer: return "NUMBER";
				case Double: return "NUMBER";
				case String: return "VARCHAR2";
				default: return "VARCHAR2";
			}
		} else if (dbVendor == DbVendor.MySQL) {
			switch (simpleDataType) {
				case Blob: return "LONGBLOB";
				case Clob: return "LONGTEXT";
				case Date: return "TIMESTAMP NULL";
				case Integer: return "INT";
				case Double: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.HSQL) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case Date: return "TIMESTAMP";
				case Integer: return "INTEGER";
				case Double: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.PostgreSQL) {
			switch (simpleDataType) {
				case Blob: return "BYTEA";
				case Clob: return "TEXT";
				case Date: return "TIMESTAMP";
				case Integer: return "INTEGER";
				case Double: return "REAL";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.SQLite) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case Date: return "TIMESTAMP";
				case Integer: return "INTEGER";
				case Double: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.Derby) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case Date: return "TIMESTAMP";
				case Integer: return "INTEGER";
				case Double: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.Firebird) {
			switch (simpleDataType) {
				case Blob: return "BLOB SUB_TYPE BINARY";
				case Clob: return "BLOB SUB_TYPE TEXT";
				case Date: return "TIMESTAMP";
				case Integer: return "INTEGER";
				case Double: return "DOUBLE PRECISION";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else {
			throw new Exception("Cannot get datatype: " + dbVendor + "/" + simpleDataType);
		}
	}
	
	public static String getTypeNameById(int typeId) throws Exception {
		switch (typeId) {
			case Types.BIT: return "BIT";
			case Types.TINYINT: return "TINYINT";
			case Types.SMALLINT: return "SMALLINT";
			case Types.INTEGER: return "INTEGER";
			case Types.BIGINT: return "BIGINT";
			case Types.FLOAT: return "FLOAT";
			case Types.REAL: return "REAL";
			case Types.DOUBLE: return "DOUBLE";
			case Types.NUMERIC: return "NUMERIC";
			case Types.DECIMAL: return "DECIMAL";
			case Types.CHAR: return "CHAR";
			case Types.VARCHAR: return "VARCHAR";
			case Types.LONGVARCHAR: return "LONGVARCHAR";
			case Types.DATE: return "DATE";
			case Types.TIME: return "TIME";
			case Types.TIMESTAMP: return "TIMESTAMP";
			case Types.BINARY: return "BINARY";
			case Types.VARBINARY: return "VARBINARY";
			case Types.LONGVARBINARY: return "LONGVARBINARY";
			case Types.NULL: return "NULL";
			case Types.OTHER: return "OTHER";
			case Types.JAVA_OBJECT: return "JAVA_OBJECT";
			case Types.DISTINCT: return "DISTINCT";
			case Types.STRUCT: return "STRUCT";
			case Types.ARRAY: return "ARRAY";
			case Types.BLOB: return "BLOB";
			case Types.CLOB: return "CLOB";
			case Types.REF: return "REF";
			case Types.DATALINK: return "DATALINK";
			case Types.BOOLEAN: return "BOOLEAN";
			case Types.ROWID: return "ROWID";
			case Types.NCHAR: return "NCHAR";
			case Types.NVARCHAR: return "NVARCHAR";
			case Types.LONGNVARCHAR: return "LONGNVARCHAR";
			case Types.NCLOB: return "NCLOB";
			case Types.SQLXML: return "SQLXML";
			case Types.REF_CURSOR: return "REF_CURSOR";
			case Types.TIME_WITH_TIMEZONE: return "TIME_WITH_TIMEZONE";
			case Types.TIMESTAMP_WITH_TIMEZONE: return "TIMESTAMP_WITH_TIMEZONE";
			default: throw new Exception("Unknown type id: " + typeId);
		}
	}

	public static String getDownloadUrl(DbVendor dbVendor) throws Exception {
		if (dbVendor == DbVendor.MySQL) {
			return DOWNLOAD_LOCATION_MYSQL;
		} else if (dbVendor == DbVendor.Oracle) {
			return DOWNLOAD_LOCATION_ORACLE;
		} else if (dbVendor == DbVendor.PostgreSQL) {
			return DOWNLOAD_LOCATION_POSTGRESQL;
		} else if (dbVendor == DbVendor.Firebird) {
			return DOWNLOAD_LOCATION_FIREBIRD;
		} else if (dbVendor == DbVendor.Derby) {
			return DOWNLOAD_LOCATION_DERBY;
		} else if (dbVendor == DbVendor.SQLite) {
			return DOWNLOAD_LOCATION_SQLITE;
		} else if (dbVendor == DbVendor.HSQL) {
			return DOWNLOAD_LOCATION_HSQL;
		} else {
			throw new Exception("Invalid db vendor");
		}
	}
}
