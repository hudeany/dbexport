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
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvWriter;

public class DbUtilities {
	public static final String DOWNLOAD_LOCATION_MYSQL = "https://dev.mysql.com/downloads/connector/j";
	public static final String DOWNLOAD_LOCATION_MARIADB = "https://downloads.mariadb.org/connector-java";
	public static final String DOWNLOAD_LOCATION_ORACLE = "http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html";
	public static final String DOWNLOAD_LOCATION_POSTGRESQL = "https://jdbc.postgresql.org/download.html";
	public static final String DOWNLOAD_LOCATION_FIREBIRD = "http://www.firebirdsql.org/en/jdbc-driver";
	public static final String DOWNLOAD_LOCATION_DERBY = "https://db.apache.org/derby/derby_downloads.html";
	public static final String DOWNLOAD_LOCATION_SQLITE = "https://bitbucket.org/xerial/sqlite-jdbc/downloads";
	public static final String DOWNLOAD_LOCATION_HSQL = "http://hsqldb.org/download";
	
	/**
	 * DevNull used to prevent creation of file "derby.log"
	 */
	public static final OutputStream DEV_NULL = new OutputStream() {
        public void write(int b) {}
    };
	
	/**
	 * In an Oracle DB the statement "SELECT CURRENT_TIMESTAMP FROM DUAL" return this special Oracle type "oracle.sql.TIMESTAMPTZ",
	 * which is not listed in java.sql.Types, but can be read via ResultSet.getTimestamp(i) into a normal java.sql.Timestamp object
	 */
	public static final int ORACLE_TIMESTAMPTZ_TYPECODE = -101;
	
	public enum DbVendor {
		Oracle("oracle.jdbc.OracleDriver", 1521, "SELECT 1 FROM DUAL"),
		MySQL("com.mysql.jdbc.Driver", 3306, "SELECT 1"),
		MariaDB("org.mariadb.jdbc.Driver", 3306, "SELECT 1"),
		PostgreSQL("org.postgresql.Driver", 5432, "SELECT 1"),
		Firebird("org.firebirdsql.jdbc.FBDriver", 3050, "SELECT 1 FROM RDB$RELATION_FIELDS ROWS 1"),
		SQLite("org.sqlite.JDBC", 0, "SELECT 1"),
		Derby("org.apache.derby.jdbc.EmbeddedDriver", 0, "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
		HSQL("org.hsqldb.jdbc.JDBCDriver", 0, "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
		Cassandra("org.bigsql.cassandra2.jdbc.CassandraDriver", 7000, "");
		
		public static DbVendor getDbVendorByName(String dbVendorName) throws Exception {
			for (DbVendor dbVendor : DbVendor.values()) {
				if (dbVendor.toString().equalsIgnoreCase(dbVendorName)) {
					return dbVendor;
				}
			}
			if ("postgres".equalsIgnoreCase(dbVendorName)) {
				return DbVendor.PostgreSQL;
			} else if ("hypersql".equalsIgnoreCase(dbVendorName)) {
				return DbVendor.HSQL;
			} else {
				throw new Exception("Invalid db vendor: " + dbVendorName);
			}
		}
		
		private String driverClassName;
		private int defaultPort;
		private String testStatement;
		
		private DbVendor(String driverClassName, int defaultPort, String testStatement) {
			this.driverClassName = driverClassName;
			this.defaultPort = defaultPort;
			this.testStatement = testStatement;
		}
		
		public String getDriverClassName() {
			return driverClassName;
		}
		
		public int getDefaultPort() {
			return defaultPort;
		}
		
		public String getTestStatement() {
			return testStatement;
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
		} else if (DbVendor.MariaDB == dbVendor) {
			return "jdbc:mariadb://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
		} else if (DbVendor.PostgreSQL == dbVendor) {
			return "jdbc:postgresql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName;
		} else if (DbVendor.SQLite == dbVendor) {
			return "jdbc:sqlite:" + Utilities.replaceHomeTilde(dbName);
		} else if (DbVendor.Derby == dbVendor) {
			return "jdbc:derby:" + Utilities.replaceHomeTilde(dbName);
		} else if (DbVendor.Firebird == dbVendor) {
			return "jdbc:firebirdsql:" + dbServerHostname + "/" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + Utilities.replaceHomeTilde(dbName);
		} else if (DbVendor.HSQL == dbVendor) {
			dbName = Utilities.replaceHomeTilde(dbName);
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
		} else if (DbVendor.Cassandra == dbVendor) {
			return "jdbc:cassandra://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?primarydc=DC1&backupdc=DC2&consistency=QUORUM";
		} else {
			throw new Exception("Unknown db vendor");
		}
	}
	
	public static Connection createNewDatabase(DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
		
		if (dbVendor == DbVendor.Derby) {
			// Prevent creation of file "derby.log"
			System.setProperty("derby.stream.error.field", "de.soderer.utilities.DbUtilities.DEV_NULL");
		}
		
		Class.forName(dbVendor.getDriverClassName());
	
		if (dbVendor == DbVendor.SQLite) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
			if (new File(dbPath).exists()) {
				throw new Exception("SQLite db file '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath));
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
			if (new File(dbPath).exists()) {
				throw new Exception("Derby db directory '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath) + ";create=true");
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
			if (dbPath.startsWith("/")) {
				if (FileUtilities.getFilesByPattern(new File(dbPath.substring(0, dbPath.lastIndexOf("/"))), dbPath.substring(dbPath.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() > 0) {
					throw new Exception("HSQL db '" + dbPath + "' already exists");
				}
			}
			
			// Logger must be kept in a local variable for making it work
			Logger dbLogger = Logger.getLogger("hsqldb.db");
			dbLogger.setLevel(Level.WARNING);
			
			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath));
		} else {
			throw new Exception("Invalid db vendor '" + dbVendor.toString() + "'. Only SQLite, HSQL or Derby db can be created this way.");
		}
	}
	
	public static void deleteDatabase(DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
	
		if (dbVendor == DbVendor.SQLite) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
			if (new File(dbPath).exists()) {
				new File(dbPath).delete();
			}
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
			if (new File(dbPath).exists()) {
				Utilities.delete(new File(dbPath));
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = Utilities.replaceHomeTilde(dbPath);
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
		return createConnection(dbVendor, hostname, dbName, userName, password, false);
	}
	
	public static Connection createConnection(DbVendor dbVendor, String hostname, String dbName, String userName, char[] password, boolean retryOnError) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		} else if (Utilities.isEmpty(hostname) && dbVendor != DbVendor.HSQL && dbVendor != DbVendor.SQLite && dbVendor != DbVendor.Derby){
			throw new Exception("Cannot create db connection: Missing hostname");
		} else if (Utilities.isEmpty(dbName)){
			throw new Exception("Cannot create db connection: Missing dbName");
		}
		
		try {
			if (dbVendor == DbVendor.Derby) {
				// Prevent creation of file "derby.log"
				System.setProperty("derby.stream.error.field", "de.soderer.utilities.DbUtilities.DEV_NULL");
			}
			
			Class.forName(dbVendor.getDriverClassName());
		} catch (Exception e) {
			throw new Exception("Cannot create db connection, caused by unknown DriverClassName: " + e.getMessage());
		}
	
		try {
			if (dbVendor == DbVendor.SQLite) {
				dbName = Utilities.replaceHomeTilde(dbName);
				if (!new File(dbName).exists()) {
					throw new DbNotExistsException("SQLite db file '" + dbName + "' is not available");
				} else if (!new File(dbName).isFile()) {
					throw new Exception("SQLite db file '" + dbName + "' is not a file");
				}
				return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbName));
			} else if (dbVendor == DbVendor.Derby) {
				dbName = Utilities.replaceHomeTilde(dbName);
				if (!new File(dbName).exists()) {
					throw new DbNotExistsException("Derby db directory '" + dbName + "' is not available");
				} else if (!new File(dbName).isDirectory()) {
					throw new Exception("Derby db directory '" + dbName + "' is not a directory");
				}
				return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbName));
			} else if (dbVendor == DbVendor.HSQL) {
				dbName = Utilities.replaceHomeTilde(dbName);
				if (dbName.startsWith("/")) {
					if (FileUtilities.getFilesByPattern(new File(dbName.substring(0, dbName.lastIndexOf("/"))), dbName.substring(dbName.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() <= 0) {
						throw new DbNotExistsException("HSQL db '" + dbName + "' is not available");
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
				
				return DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), (Utilities.isNotEmpty(userName) ? userName : "SA"), (password != null ? new String(password) : ""));
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

//				if (dbVendor == DbVendor.Oracle && new File("/dev/urandom").exists()) {
//					// Set the alternative random generator to improve the connection creation speed, which may even cause a "I/O-Fehler: Connection reset"-error on low performance systems
//					System.setProperty("java.security.egd", "file:///dev/./urandom");
//					
//					// Alternatively you can change the file $JAVA_HOME/jre/lib/security/java.security and add following line:
//					// securerandom.source=file:/dev/./urandom
//				}
				
				Connection connection;
				try {
					connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), userName, new String(password));
				} catch (Exception e) {
					if (retryOnError && e.getCause() != null && e.getCause() instanceof SQLRecoverableException) {
						connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), userName, new String(password));
					} else {
						throw e;
					}
				}

//				if (dbVendor == DbVendor.Oracle && new File("/dev/random").exists()) {
//					// Reset the alternative random generator
//					System.clearProperty("java.security.egd");
//				}
				
				return connection;
			}
		} catch (DbNotExistsException e) {
			throw e;
		} catch (Exception e) {
			try {
				int port;
				if (Utilities.isNotEmpty(hostname)) {
					if (hostname.contains(":")) {
						try {
							port = Integer.parseInt(hostname.substring(hostname.indexOf(":") + 1));
						} catch (Exception e1) {
							throw new Exception("Invalid port: " + hostname.substring(hostname.indexOf(":") + 1));
						}
						NetworkUtilities.testConnection(hostname.substring(0, hostname.indexOf(":")), port);
					} else {
						port = dbVendor.getDefaultPort();
						NetworkUtilities.testConnection(hostname, port);
					}
				}
				
				// No Exception from testConnection, so it must be some other problem
				throw new Exception("Cannot create db connection: " + e.getMessage(), e);
			} catch (Exception e1) {
				throw new Exception("Cannot create db connection, caused by Url (" + hostname + "): " + e1.getMessage(), e1);
			}
		}
	}
	
	public static int readoutInOutputStream(DataSource dataSource, String statementString, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutInOutputStream(connection, statementString, outputStream, encoding, separator, stringQuote);
		}
	}
	
	public static int readoutInOutputStream(Connection connection, String statementString, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(statementString);
				CsvWriter csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote))) {
			ResultSetMetaData metaData = resultSet.getMetaData();

			List<String> headers = new ArrayList<>();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				headers.add(metaData.getColumnLabel(i));
			}
			csvWriter.writeValues(headers);

			while (resultSet.next()) {
				List<String> values = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					if (metaData.getColumnType(i) == Types.BLOB
							|| metaData.getColumnType(i) == Types.BINARY
							|| metaData.getColumnType(i) == Types.VARBINARY
							|| metaData.getColumnType(i) == Types.LONGVARBINARY) {
						if (dbVendor == DbVendor.SQLite || dbVendor == DbVendor.PostgreSQL) {
							// SQLite does not allow "resultSet.getBlob(i)"
							InputStream input = null;
							try {
								input = resultSet.getBinaryStream(metaData.getColumnName(i));
								if (input != null) {
									byte[] data = Utilities.toByteArray(input);
									values.add(Base64.getEncoder().encodeToString(data));
								} else {
									values.add("");
								}
							} catch (Exception e) {
								// NULL blobs throw a NullpointerException in SQLite
								values.add("");
							} finally {
								Utilities.closeQuietly(input);
							}
						} else {
							Blob blob = resultSet.getBlob(i);
							if (resultSet.wasNull()) {
								values.add("");
							} else {
								try (InputStream input = blob.getBinaryStream()) {
									byte[] data = Utilities.toByteArray(input);
									values.add(Base64.getEncoder().encodeToString(data));
								}
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
    
    public static String readoutTable(Connection connection, String tableName) throws Exception {
		return readoutTable(connection, tableName, ';', '"');
    }

	public static String readoutTable(Connection connection, String tableName, char separator, Character stringQuote) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			DbVendor dbVendor = getDbVendor(connection);
			List<String> columnNames = new ArrayList<String>(getColumnNames(connection, tableName));
			Collections.sort(columnNames);
			List<String> keyColumnNames = new ArrayList<String>(getPrimaryKeyColumns(connection, tableName));
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
				orderPart = " ORDER BY " + joinColumnVendorEscaped(dbVendor, keyColumnNames);
			}
			return readout(connection, "SELECT " + joinColumnVendorEscaped(dbVendor, readoutColumns) + " FROM " + tableName + orderPart, separator, stringQuote);
		}
	}

	public static DbVendor getDbVendor(DataSource dataSource) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return getDbVendor(connection);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot check db vendor: " + e.getMessage(), e);
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
				} else if (productName != null && productName.toLowerCase().contains("maria")) {
					return DbVendor.MariaDB;
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
				} else if (productName != null && productName.toLowerCase().contains("cassandra")) {
					return DbVendor.Cassandra;
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
		try (Connection connection = dataSource.getConnection()) {
			return getDbUrl(connection);
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
		try (Connection connection = dataSource.getConnection()) {
			return checkTableAndColumnsExist(connection, tableName, throwExceptionOnError, columns);
		}
	}

	public static boolean checkTableAndColumnsExist(Connection connection, String tableName, boolean throwExceptionOnError, String... columns) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		CaseInsensitiveSet dbTableColumns = getColumnNames(connection, tableName);
		if (columns != null) {
			for (String column : columns) {
				if (column != null && !dbTableColumns.contains(unescapeVendorReservedNames(dbVendor, column))) {
					if (throwExceptionOnError) {
						throw new Exception("Column '" + column + "' does not exist in table '" + tableName + "'");
					} else {
						return false;
					}
				}
			}
		}
		return true;
	}
	
	public static boolean checkTableExist(Connection connection, String tableName) throws Exception {
		return checkTableExist(connection, tableName, false);
	}

	public static boolean checkTableExist(Connection connection, String tableName, boolean throwExceptionOnError) throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0")) {
			return true;
		} catch (Exception e) {
			if (throwExceptionOnError) {
				throw new Exception("Table '" + tableName + "' does not exist");
			} else {
				return false;
			}
		}
	}

	public static String callStoredProcedureWithDbmsOutput(Connection connection, String procedureName, Object... parameters) throws SQLException {
		try (CallableStatement callableStatement = connection.prepareCall("begin dbms_output.enable(:1); end;")) {
			callableStatement.setLong(1, 10000);
			callableStatement.executeUpdate();
		}

		if (parameters != null) {
			try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(" + TextUtilities.repeatString("?", parameters.length, ", ") + ")}")) {
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].getClass() == Date.class) {
						parameters[i] = new java.sql.Date(((Date) parameters[i]).getTime());
					}
				}
				for (int i = 0; i < parameters.length; i++) {
					callableStatement.setObject(i + 1, parameters[i]);
				}
				callableStatement.execute();
			}
		} else {
			try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "()}")) {
				callableStatement.execute();
			}
		}

		StringBuffer dbmsOutput;
		try (CallableStatement callableStatement = connection.prepareCall(
	"declare "
				+ "    l_line varchar2(255); "
				+ "    l_done number; "
				+ "    l_buffer long; "
				+ "begin "
				+ "  loop "
				+ "    exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; "
				+ "    dbms_output.get_line( l_line, l_done ); "
				+ "    l_buffer := l_buffer || l_line || chr(10); "
				+ "  end loop; "
				+ " :done := l_done; "
				+ " :buffer := l_buffer; "
				+ "end;")) {
			callableStatement.registerOutParameter(2, Types.INTEGER);
			callableStatement.registerOutParameter(3, Types.VARCHAR);
			dbmsOutput = new StringBuffer(1024);
			while (true) {
				callableStatement.setInt(1, 32000);
				callableStatement.executeUpdate();
				dbmsOutput.append(callableStatement.getString(3).trim());
				if (callableStatement.getInt(2) == 1) {
					break;
				}
			}
		}
	
		try (CallableStatement callableStatement = connection.prepareCall("begin dbms_output.disable; end;")) {
			callableStatement.executeUpdate();
		}
		
		return dbmsOutput == null ? null : dbmsOutput.toString();
	}

	public static String getResultAsTextTable(DataSource datasource, String selectString) throws Exception {
		try (Connection connection = datasource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(selectString);
				ResultSet resultSet = preparedStatement.executeQuery()) {
			TextTable textTable = new TextTable();
			for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
				textTable.addColumn(resultSet.getMetaData().getColumnLabel(columnIndex));
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
		}
	}
	
	public static CaseInsensitiveSet getColumnNames(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnNames");
		}
		
		try (Connection connection = dataSource.getConnection()) {
			return getColumnNames(connection, tableName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot read columns for table " + tableName + ": " + e.getMessage(), e);
		}
	}

	public static CaseInsensitiveSet getColumnNames(Connection connection, String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			try (Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0")) {
				CaseInsensitiveSet columnNamesList = new CaseInsensitiveSet();
				for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
					columnNamesList.add(resultSet.getMetaData().getColumnName(i));
				}
				return columnNamesList;
			}
		}
	}
	
	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			try (Connection connection = dataSource.getConnection()) {
				return getColumnDataTypes(connection, tableName);
			}
		}
	}

	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(Connection connection, String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			CaseInsensitiveMap<DbColumnType> returnMap = new CaseInsensitiveMap<DbColumnType>();
			DbVendor dbVendor = getDbVendor(connection);
			if (DbVendor.Oracle == dbVendor) {
				// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
				String sql = "SELECT column_name, NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) as data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
	
							// TODO AutoIncrements will be introduced with Oracle 12
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, false));
						}
					}
				}
			} else if (DbVendor.MySQL == dbVendor) {
				String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, extra FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery() ) {
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
							boolean isAutoIncrement = resultSet.getString("extra").equalsIgnoreCase("auto_increment");
	
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.MariaDB == dbVendor) {
				String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, extra FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery() ) {
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
							boolean isAutoIncrement = resultSet.getString("extra").equalsIgnoreCase("auto_increment");
	
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.HSQL == dbVendor) {
				String sql = "SELECT column_name, type_name, column_size, decimal_digits, is_nullable, is_autoincrement FROM information_schema.system_columns WHERE LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
							boolean isAutoIncrement = resultSet.getString("is_autoincrement").equalsIgnoreCase("yes");
	
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("type_name"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.Derby == dbVendor) {
				String sql = "SELECT columnname, columndatatype, autoincrementvalue FROM sys.systables, sys.syscolumns WHERE tableid = referenceid AND LOWER(tablename) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
	
							boolean autoincrement = !Utilities.isBlank(resultSet.getString("autoincrementvalue"));
							
							if (type.contains("(")) {
								characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
								type = type.substring(0, type.indexOf("("));
							}
	
							returnMap.put(resultSet.getString("columnname"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable, autoincrement));
						}
					}
				}
			} else if (DbVendor.Firebird == dbVendor) {
				String sql = "SELECT rf.rdb$field_name, f.rdb$field_type, f.rdb$field_sub_type, f.rdb$field_length, f.rdb$field_scale, f.rdb$null_flag"
					+ " FROM rdb$fields f JOIN rdb$relation_fields rf ON rf.rdb$field_source = f.rdb$field_name WHERE rf.rdb$relation_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName.toUpperCase());
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
	
							// TODO check autoincrement
							returnMap.put(resultSet.getString("rdb$field_name").trim(), new DbColumnType(dataType, characterLength, numericPrecision, numericScale, isNullable, false));
						}
					}
				}
			} else if (DbVendor.SQLite == dbVendor) {
				boolean hasAutoIncrement = false;
				if (checkTableExist(connection, "sqlite_sequence")) {
					// sqlite_sequence only exists if there is any table with auto_increment
					try (PreparedStatement preparedStatement2 = connection.prepareStatement("SELECT COUNT(*) FROM sqlite_sequence WHERE LOWER(name) = LOWER(?)")) {
						preparedStatement2.setString(1, tableName);
						try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
							if (resultSet2.next()) {
								hasAutoIncrement = resultSet2.getInt(1) > 0;
							}
						}
					}
				}
				
				String sql = "PRAGMA table_info(" + tableName + ")";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
						ResultSet resultSet = preparedStatement.executeQuery()) {
					while (resultSet.next()) {
						long characterLength = -1;
						int numericPrecision = -1;
						int numericScale = -1;
						boolean isNullable = resultSet.getInt("notnull") == 0;
						// Only the primary key can be auto incremented in SQLite
						boolean isAutoIncrement = hasAutoIncrement && resultSet.getInt("pk") > 0;
						
						String type = resultSet.getString("type");
						
						if (type.contains("(")) {
							characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
							type = type.substring(0, type.indexOf("("));
						}

						returnMap.put(resultSet.getString("name"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
					}
				}
			} else if (DbVendor.PostgreSQL == dbVendor) {
				String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
							
							String defaultValue = resultSet.getString("column_default");
							boolean isAutoIncrement = false;
							if (defaultValue!= null && defaultValue.toLowerCase().startsWith("nextval(")) {
								isAutoIncrement = true;
							}
	
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else {
				throw new Exception("Unsupported db vendor");
			}
			return returnMap;
		}
	}

	public static int getTableEntriesCount(Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getTableEntriesNumber");
		} else {
			try (Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + getSQLSafeString(tableName))) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
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
			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0")) {
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getMetaData().getColumnName(columnIndex).equalsIgnoreCase(columnName.trim())) {
						return true;
					}
				}
				return false;
			}
		}
	}

	public static String getColumnDefaultValue(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getDefaultValueOf");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getDefaultValueOf");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getDefaultValueOf");
		} else {
			try (Connection connection = dataSource.getConnection()) {
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					String sql = "SELECT data_default FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
					try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
						preparedStatement.setString(1, tableName.toUpperCase());
						preparedStatement.setString(2, columnName.toUpperCase());
						try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
						}
					}
				} else {
					String sql = "SELECT column_default FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
					try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
						preparedStatement.setString(1, tableName);
						preparedStatement.setString(2, columnName);
						try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
					}
				}
			}
		}
	}

	public static String getDateDefaultValue(DataSource dataSource, String fieldDefault) throws Exception {
		DbVendor dbVendor = getDbVendor(dataSource);
		if (fieldDefault.equalsIgnoreCase("sysdate") || fieldDefault.equalsIgnoreCase("sysdate()") || fieldDefault.equalsIgnoreCase("current_timestamp")) {
			if (dbVendor == DbVendor.Oracle) {
				return "current_timestamp";
			} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
				return "current_timestamp";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		} else {
			if (dbVendor == DbVendor.Oracle) {
				return "to_date('" + fieldDefault + "', 'DD.MM.YYYY')";
			} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
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

			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement()) {
				statement.executeUpdate(addColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
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
			} else if (DbVendor.MariaDB == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY " + changeColumnStatementPart;
			} else {
				throw new Exception("Unsupported db vendor");
			}

			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement()) {
				statement.executeUpdate(changeColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
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
		try (Connection connection = dataSource.getConnection()) {
			return checkOracleTablespaceExists(connection, tablespaceName);
		} catch (SQLException e) {
			throw new Exception("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
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
			try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM dba_tablespaces WHERE LOWER(tablespace_name) = ?")) {
				statement.setString(1, tablespaceName.toLowerCase());
				try (ResultSet resultSet = statement.executeQuery()) {
					return resultSet.getInt(1) > 0;
				}
			} catch (Exception e) {
				throw new RuntimeException("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
			}
		} else {
			return false;
		}
	}

	public static CaseInsensitiveSet getPrimaryKeyColumns(DataSource dataSource, String tableName) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return getPrimaryKeyColumns(connection, tableName);
		} catch (SQLException e) {
			throw new Exception("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
		}
	}

	public static CaseInsensitiveSet getPrimaryKeyColumns(Connection connection, String tableName) {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			try {
				if (getDbVendor(connection) == DbVendor.Oracle || getDbVendor(connection) == DbVendor.HSQL || getDbVendor(connection) == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}
				
				DatabaseMetaData metaData = connection.getMetaData();
				try (ResultSet resultSet = metaData.getPrimaryKeys(connection.getCatalog(), null, tableName)) {
					CaseInsensitiveSet returnList = new CaseInsensitiveSet();
					while (resultSet.next()) {
						returnList.add(resultSet.getString("COLUMN_NAME"));
					}
					return returnList;
				}
			} catch (Exception e) {
				throw new RuntimeException("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
			}
		}
	}

	public static List<List<String>> getForeignKeys(Connection connection, String tableName) {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			try {
				if (getDbVendor(connection) == DbVendor.Oracle || getDbVendor(connection) == DbVendor.HSQL || getDbVendor(connection) == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}
				
				DatabaseMetaData metaData = connection.getMetaData();
				try (ResultSet resultSet = metaData.getImportedKeys(connection.getCatalog(), null, tableName)) {
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
				}
			} catch (Exception e) {
				throw new RuntimeException("Cannot read foreign key columns for table " + tableName + ": " + e.getMessage(), e);
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
		try (Statement statement = connection.createStatement()) {
			DbVendor dbVendor = getDbVendor(connection);

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

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
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

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.MariaDB == dbVendor) {
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

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
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

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
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
				
				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("name"));
					}
					return tableNamesToExport;
				}
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
				
				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("tablename"));
					}
					return tableNamesToExport;
				}
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
				
				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
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
				
				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					List<String> tableNamesToExport = new ArrayList<String>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else {
				throw new Exception("Unknown db vendor");
			}
		}
	}
	
	public static void createTable(Connection connection, String tablename, Map<String, DbColumnType> columnsAndTypes, Collection<String> keyColumns) throws Exception {
		if  (keyColumns != null) {
			for (String keyColumn : keyColumns) {
				keyColumn = Utilities.trimSimultaneously(Utilities.trimSimultaneously(keyColumn, "\""), "`");
				if (!columnsAndTypes.containsKey(keyColumn)) {
					throw new Exception("Cannot create table. Keycolumn '" + keyColumn + "' is not included in column types");
				}
			}
		}
		
		try (Statement statement = connection.createStatement()) {
			DbVendor dbVendor = getDbVendor(connection);
			
			String columnPart = "";
			for (Entry<String, DbColumnType> columnAndType : columnsAndTypes.entrySet()) {
				if (columnPart.length() > 0) {
					columnPart += ", ";
				}
				DbColumnType columnType = columnAndType.getValue();
				if (columnType != null) {
					String dataType = getDataType(dbVendor, columnType.getSimpleDataType());
					int dataLength = dataType.toLowerCase().contains("varchar") ? (int) columnType.getCharacterLength() : 0;
					columnPart += escapeVendorReservedNames(dbVendor, columnAndType.getKey()) + " " + dataType + (dataLength > 0 ? "(" + dataLength + ")" : "");
				} else {
					String dataType = getDataType(dbVendor, SimpleDataType.String);
					columnPart += escapeVendorReservedNames(dbVendor, columnAndType.getKey()) + " " + dataType + "(1)";
				}
			}
			
			String primaryKeyPart = "";
			if (Utilities.isNotEmpty(keyColumns)) {
				primaryKeyPart = ", PRIMARY KEY (" + joinColumnVendorEscaped(dbVendor, keyColumns) + ")";
			}
			statement.execute("CREATE TABLE " + tablename + " (" + columnPart + primaryKeyPart + ")");
			if (getDbVendor(connection) == DbVendor.Derby) {
				connection.commit();
			}
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
		} else if (dbVendor == DbVendor.MariaDB) {
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
		} else if (dbVendor == DbVendor.MariaDB) {
			return DOWNLOAD_LOCATION_MARIADB;
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

	/**
	 * Update the duplicateIndexColumn column of all entries of a table with the minimum index value from itemIndexColumn of other duplicates.
	 * 
	 * @param connection
	 * @param tableName
	 * @param keyColumns
	 * @param keyColumnsWithFunctions
	 * @param itemIndexColumn
	 * @param duplicateIndexColumn
	 * @throws Exception
	 */
	public static void markDuplicates(Connection connection, String tableName, Collection<String> keyColumnsWithFunctions, String itemIndexColumn, String duplicateIndexColumn) throws Exception {
		if (Utilities.isNotEmpty(keyColumnsWithFunctions)) {
			DbVendor dbVendor = getDbVendor(connection);
			itemIndexColumn = escapeVendorReservedNames(dbVendor, itemIndexColumn);
			duplicateIndexColumn = escapeVendorReservedNames(dbVendor, duplicateIndexColumn);
			
			StringBuilder selectPart = new StringBuilder();
			StringBuilder wherePart = new StringBuilder();
			int columnIndex = 0;
			for (String columnWithFunction : keyColumnsWithFunctions) {
				columnWithFunction = escapeVendorReservedNames(dbVendor, columnWithFunction.trim());
				
				if (selectPart.length() > 0) {
					selectPart.append(", ");
					wherePart.append(", ");
				}
				
				selectPart.append(columnWithFunction + " AS col" + columnIndex);
				
				wherePart.append("subselect.col" + columnIndex);
				
				wherePart.append(" = ");

				if (columnWithFunction.contains("(")) {
					wherePart.append(columnWithFunction.replace("(", "(" + tableName + "."));
				} else {
					wherePart.append(tableName + "." + columnWithFunction);
				}
				
				columnIndex++;
			}
			
			try (Statement statement = connection.createStatement()) {
				// The indirection with a subselect is needed for MySQL (You can't specify target table for update in FROM clause)
				String setDuplicateReferences = "UPDATE " + tableName + " SET " + duplicateIndexColumn + " = (SELECT subselect." + itemIndexColumn + " FROM"
					+ " (SELECT " + selectPart.toString() + ", MIN(" + itemIndexColumn + ") AS " + itemIndexColumn + " FROM " + tableName + " GROUP BY " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + ") subselect"
					+ " WHERE " + wherePart.toString() + ")";
				statement.executeUpdate(setDuplicateReferences);
				connection.commit();
			} catch (Exception e) {
				connection.rollback();
				throw new Exception("Cannot markTrailingDuplicates: " + e.getMessage(), e);
			}
		} else {
			throw new Exception("Cannot markDuplicates: Missing keycolumns");
		}
	}
	
	public static String getKeyColumnEquationList(DbVendor dbVendor, Collection<String> columnNamesWithFunctions, String tableAlias1, String tableAlias2) {
		StringBuilder returnValue = new StringBuilder();
		for (String columnName : columnNamesWithFunctions) {
			columnName = escapeVendorReservedNames(dbVendor, columnName.trim());
			
			if (returnValue.length() > 0) {
				returnValue.append(", ");
			}
			
			if (Utilities.isNotBlank(tableAlias1)) {
				if (columnName.contains("(")) {
					returnValue.append(columnName.replace("(", "(" + tableAlias1 + "."));
				} else {
					returnValue.append(tableAlias1 + "." + columnName);
				}
			} else {
				returnValue.append(columnName);
			}
			
			returnValue.append(" = ");

			if (Utilities.isNotBlank(tableAlias2)) {
				if (columnName.contains("(")) {
					returnValue.append(columnName.replace("(", "(" + tableAlias2 + "."));
				} else {
					returnValue.append(tableAlias2 + "." + columnName);
				}
			} else {
				returnValue.append(columnName);
			}
		}
		return returnValue.toString();
	}

	public static int detectDuplicates(Connection connection, String tableName, Collection<String> keyColumnsWithFunctions) throws Exception {
		try (Statement statement = connection.createStatement()) {
			String countDuplicatesStatement = "SELECT COUNT(*) FROM (SELECT COUNT(*) FROM " + tableName + " GROUP BY " + joinColumnVendorEscaped(getDbVendor(connection), keyColumnsWithFunctions) + " HAVING COUNT(*) > 1) subsel";
			try (ResultSet resultSet = statement.executeQuery(countDuplicatesStatement)) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
			}
		} catch (Exception e) {
			throw new Exception("Cannot detectDuplicates: " + e.getMessage(), e);
		}
	}

	public static int detectDuplicatesCrossTables(Connection connection, String detectTableName, String fromTableName, List<String> keyColumnsWithFunctions) throws Exception {
		try (Statement statement = connection.createStatement()) {
			String selectDuplicatesNumber = "SELECT COUNT(*) FROM " + detectTableName + " a WHERE EXISTS (SELECT 1 FROM " + fromTableName + " b WHERE " + getKeyColumnEquationList(getDbVendor(connection), keyColumnsWithFunctions, "a", "b") + ")";
			try (ResultSet resultSet = statement.executeQuery(selectDuplicatesNumber)) {
				resultSet.next();
				return resultSet.getInt(1);
			}
		} catch (Exception e) {
			throw new Exception("Cannot detectDuplicatesCrossTables: " + e.getMessage(), e);
		}
	}

	public static String addIndexedIntegerColumn(Connection connection, String tableName, String columnBaseName) throws Exception {
		String columnName = columnBaseName;
		int i = 0;
		while (checkTableAndColumnsExist(connection, tableName, columnName) && i < 10) {
			i++;
			columnName = columnBaseName + "_" + i;
		}
		if (i >= 10) {
			throw new Exception("Cannot create columnBaseName " + columnBaseName + " in table " + tableName);
		}
		
		try (Statement statement = connection.createStatement()) {
			statement.execute("ALTER TABLE " + tableName + " ADD " + columnName + " INTEGER");
			String dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSSSSS).format(new Date());
			try {
				statement.execute("CREATE INDEX tmp" + dateSuffix + "_idx ON " + tableName + " (" + columnName + ")");
			} catch (Exception e) {
				e.printStackTrace();
				// Work without index. Maybe it already exists or it is a not allowed function based index
			}
		}
		
		return columnName;
	}

	public static int dropDuplicatesCrossTable(Connection connection, String keepInTableName, String deleteInTableName, List<String> keyColumnsWithFunctions) throws Exception {
		if (Utilities.isEmpty(keyColumnsWithFunctions)) {
			DbVendor dbVendor = getDbVendor(connection);
			try (Statement statement = connection.createStatement()) {
				String deleteDuplicates = "DELETE FROM " + deleteInTableName + " WHERE " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + " IN (SELECT " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + " FROM " + keepInTableName + ")";
				int numberOfDeletedDuplicates = statement.executeUpdate(deleteDuplicates);
				connection.commit();
				return numberOfDeletedDuplicates;
			} catch (Exception e) {
				connection.rollback();
				throw new Exception("Cannot deleteTableCrossDuplicates: " + e.getMessage(), e);
			}
		} else {
			return 0;
		}
	}

	public static int dropDuplicates(Connection connection, String tableName, Collection<String> keyColumns) throws Exception {
		if (detectDuplicates(connection, tableName, keyColumns) > 0) {
			String originalItemIndexColumn = null;
			String originalDuplicateIndexColumn = null;
			try (Statement statement = connection.createStatement()) {
				originalItemIndexColumn = createLineNumberIndexColumn(connection, tableName, "drop_idx");
				originalDuplicateIndexColumn = addIndexedIntegerColumn(connection, tableName, "drop_dpl");
				
				// Try to create an additional index columns
				if (Utilities.isNotEmpty(keyColumns)) {
					try {
						String dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(new Date());
						statement.execute("CREATE INDEX tmp" + dateSuffix + "_idx ON " + tableName + " (" + joinColumnVendorEscaped(getDbVendor(connection), keyColumns) + ")");
					} catch (Exception e) {
						// Work without index. Maybe it already exists or it is a not allowed function based index
					}
				}
				
				markDuplicates(connection, tableName, keyColumns, originalItemIndexColumn, originalDuplicateIndexColumn);
				
				int numberOfDeletedDuplicates = statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				connection.commit();
				return numberOfDeletedDuplicates;
			} catch (Exception e) {
				connection.rollback();
				throw new Exception("Cannot dropDuplicates: " + e.getMessage(), e);
			} finally {
				dropColumnIfExists(connection, tableName, originalItemIndexColumn);
				dropColumnIfExists(connection, tableName, originalDuplicateIndexColumn);
			}
		} else {
			return 0;
		}
	}
	
	public static int joinDuplicates(Connection connection, String tableName, Collection<String> keyColumnsWithFunctions, boolean updateWithNullValues) throws Exception {
		if (detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0) {
			DbVendor dbVendor = getDbVendor(connection);
			String dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(new Date());
			String interimTableName = "tmp_join_" + dateSuffix;
			String originalItemIndexColumn = null;
			String originalDuplicateIndexColumn = null;
			
			// Join all duplicates in destination table
			try (Statement statement = connection.createStatement()) {
				// Create additional index columns
				if (Utilities.isNotEmpty(keyColumnsWithFunctions)) {
					try {
						statement.execute("CREATE INDEX tmp" + dateSuffix + "_idx ON " + tableName + " (" + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + ")");
						connection.commit();
					} catch (Exception e) {
						// Work without index. Maybe it already exists or it is a not allowed function based index
					}
				}
				
				originalItemIndexColumn = createLineNumberIndexColumn(connection, tableName, "join_idx");
				originalDuplicateIndexColumn = addIndexedIntegerColumn(connection, tableName, "join_dpl");
				
				markDuplicates(connection, tableName, keyColumnsWithFunctions, originalItemIndexColumn, originalDuplicateIndexColumn);
				connection.commit();
				
				// Create temp table
				if (dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
					statement.execute("CREATE TABLE " + interimTableName + " AS (SELECT * FROM " + tableName + ") WITH NO DATA");
					statement.executeUpdate("INSERT INTO " + interimTableName + " (SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn + ")");
				} else if (dbVendor == DbVendor.PostgreSQL) {
					// Close a maybe open transaction to allow DDL-statement
					connection.rollback();
					statement.execute("CREATE TABLE " + interimTableName + " AS SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				} else if (dbVendor == DbVendor.Firebird) {
					// There is no "create table as select"-statmenet in firebird
					createTable(connection, interimTableName, getColumnDataTypes(connection, tableName), null);
				} else {
					statement.execute("CREATE TABLE " + interimTableName + " AS SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				}
				connection.commit();
				
				int deletedDuplicatesInDB = dropDuplicates(connection, tableName, keyColumnsWithFunctions);

				List<String> columnsWithoutAutoIncrement = new ArrayList<String>();
				for (Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					}
				}
				
				updateAllExistingItems(connection, interimTableName, tableName, columnsWithoutAutoIncrement, keyColumnsWithFunctions, originalItemIndexColumn, updateWithNullValues, null);
				connection.commit();
				return deletedDuplicatesInDB;
			} catch (Exception e) {
				throw new Exception("Cannot joinDuplicates: " + e.getMessage(), e);
			} finally {
				dropTableIfExists(connection, interimTableName);
				dropColumnIfExists(connection, tableName, originalItemIndexColumn);
				dropColumnIfExists(connection, tableName, originalDuplicateIndexColumn);
			}
		} else {
			return 0;
		}
	}

	private static String createLineNumberIndexColumn(Connection connection, String tableName, String indexColumnNameBaseName) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		String indexColumnName = addIndexedIntegerColumn(connection, tableName, indexColumnNameBaseName);

		try (Statement statement = connection.createStatement()) {
			if (dbVendor == DbVendor.MySQL) {
				statement.execute("SELECT @n := 0");
				statement.execute("UPDATE " + tableName + " SET " + indexColumnName + " = @n := @n + 1");
			} else if (dbVendor == DbVendor.MariaDB) {
				statement.execute("SELECT @n := 0");
				statement.execute("UPDATE " + tableName + " SET " + indexColumnName + " = @n := @n + 1");
			} else if (dbVendor == DbVendor.Oracle) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWNUM");
			} else if (dbVendor == DbVendor.SQLite) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWID");
			} else if (dbVendor == DbVendor.HSQL) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWNUM()");
			} else if (dbVendor == DbVendor.Derby) {
				String autoIncrementColumn = null;
				List<String> columnsWithoutAutoIncrement = new ArrayList<String>();
				for (Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					} else {
						autoIncrementColumn = column.getKey();
					}
				}
				columnsWithoutAutoIncrement.remove(indexColumnName);
				if (autoIncrementColumn != null) {
					statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = " + autoIncrementColumn);
				} else {
					statement.executeUpdate("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", " + indexColumnName + ") (SELECT " + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", ROW_NUMBER() OVER() FROM " + tableName + ")");
					statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + indexColumnName + " IS NULL");
				}
			} else if (dbVendor == DbVendor.PostgreSQL) {
				String autoIncrementColumn = null;
				List<String> columnsWithoutAutoIncrement = new ArrayList<String>();
				for (Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					} else {
						autoIncrementColumn = column.getKey();
					}
				}
				columnsWithoutAutoIncrement.remove(indexColumnName);
				if (autoIncrementColumn != null) {
					statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = " + autoIncrementColumn);
				} else {
					statement.executeUpdate("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", " + indexColumnName + ") (SELECT " + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", ROW_NUMBER() OVER() FROM " + tableName + ")");
					statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + indexColumnName + " IS NULL");
				}
			} else {
				throw new Exception("Unsupported db vendor");
			}
			connection.commit();
		} catch (Exception e) {
			throw new Exception("Cannot create lineNumberIndexColumn: " + e.getMessage(), e);
		}
		return indexColumnName;
	}

	public static boolean dropColumnIfExists(Connection connection, String tableName, String columnName) throws Exception {
		if (connection != null && tableName != null && columnName != null && checkTableAndColumnsExist(connection, tableName, columnName)) {
			DbVendor dbVendor = getDbVendor(connection);
			if (dbVendor == DbVendor.SQLite) {
				// SQLite cannot drop colmns
				try (Statement statement = connection.createStatement()) {
					String dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(new Date());
					
					String createTableStatement = null;
					try (ResultSet resultSet = statement.executeQuery("SELECT sql FROM sqlite_master WHERE type = 'table' AND LOWER(name) = LOWER('" + tableName + "')")) {
						if (resultSet.next()) {
							createTableStatement = resultSet.getString("sql");
						}
					}
					
					createTableStatement = createTableStatement.replaceAll(", " + columnName + " [a-zA-Z0-9()]+\\)", ")").replaceAll(", " + columnName + " [a-zA-Z0-9()]+,", ",");
					
					statement.execute("ALTER TABLE " + tableName + " RENAME TO tmp" + dateSuffix + "_old");
					statement.execute(createTableStatement);
					Set<String> columns = getColumnNames(connection, tableName);
					columns.remove(columnName);
					statement.execute("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columns) + ") SELECT " + joinColumnVendorEscaped(dbVendor, columns) + " FROM tmp" + dateSuffix + "_old");
					statement.execute("DROP TABLE tmp" + dateSuffix + "_old");
					connection.commit();
					return true;
				} catch (Exception e) {
					connection.rollback();
					throw e;
				}
			} else {
				try (Statement statement = connection.createStatement()) {
					statement.execute("ALTER TABLE " + tableName + " DROP COLUMN " + columnName);
					connection.commit();
					return true;
				} catch (Exception e) {
					connection.rollback();
					throw e;
				}
			}
		}
		return false;
	}
	
	public static boolean dropTableIfExists(Connection connection, String tableName) throws Exception {
		if (connection != null && tableName != null && checkTableExist(connection, tableName)) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("DROP TABLE " + tableName);
				connection.commit();
				return true;
			}
		}
		return false;
	}
	
	public static void copyTableStructure(Connection connection, String sourceTableName, List<String> columnNames, List<String> keyColumns, String destinationTableName) throws Exception {
		try (Statement statement = connection.createStatement()) {
			DbVendor dbVendor = getDbVendor(connection);
			if (dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
				statement.execute("CREATE TABLE " + destinationTableName + " AS (SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + ") WITH NO DATA");
			} else if (dbVendor == DbVendor.PostgreSQL) {
				// Close a maybe open transaction to allow DDL-statement
				connection.rollback();
				statement.execute("CREATE TABLE " + destinationTableName + " AS SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + " WHERE 1 = 0");
			} else if (dbVendor == DbVendor.Firebird) {
				// There is no "create table as select"-statmenet in firebird
				createTable(connection, destinationTableName, getColumnDataTypes(connection, sourceTableName), null);
			} else {
				statement.execute("CREATE TABLE " + destinationTableName + " AS SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + " WHERE 1 = 0");
			}
			
			// Make all columns nullable
			CaseInsensitiveMap<DbColumnType> columnDataTypes = getColumnDataTypes(connection, destinationTableName);
			for (Entry<String, DbColumnType> columnDataType : columnDataTypes.entrySet()) {
				if (!columnDataType.getValue().isNullable() && (keyColumns == null || !keyColumns.contains(columnDataType.getKey()))) {
					String typeString = columnDataType.getValue().getTypeName();
					if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.String) {
						typeString += "(" + (Long.toString(columnDataType.getValue().getCharacterLength())) + ")";
					} else if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.Integer) {
						typeString += "(" + (Integer.toString(columnDataType.getValue().getNumericPrecision())) + ")";
					} else if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.Double) {
						typeString += "(" + (Integer.toString(columnDataType.getValue().getNumericPrecision())) + ")";
					}
					statement.execute("ALTER TABLE " + destinationTableName + " MODIFY " + columnDataType.getKey() + " " + typeString + " NULL");
				}
			}
			
			if (dbVendor == DbVendor.PostgreSQL || dbVendor == DbVendor.Firebird) {
				connection.commit();
			}
		}
	}
	
	public static String createIndex(Connection connection, String tableName, List<String> columns) throws Exception {
		try (Statement statement = connection.createStatement()) {
			String indexNameSuffix = "_" + new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(new Date()) + "_ix";
			String indexName = Utilities.shortenStringToMaxLengthCutRight(tableName, 30 - indexNameSuffix.length(), "") + indexNameSuffix;
			statement.execute("CREATE INDEX " + indexName + " ON " + tableName + " (" + joinColumnVendorEscaped(getDbVendor(connection), columns) + ")");
			return indexName;
		} catch (Exception e) {
			throw new Exception("Cannot create index: " + e.getMessage(), e);
		}
	}
	
	public static int clearTable(Connection connection, String tableName) throws Exception {
		try (Statement statement = connection.createStatement()) {
			return statement.executeUpdate("DELETE FROM " + tableName);
		} catch (Exception e) {
			throw new Exception("Cannot clear table: " + e.getMessage(), e);
		}
	}
	
	public static int insertNotExistingItems(Connection connection, String sourceTableName, String destinationTableName, List<String> insertColumns, List<String> keyColumnsWithFunctions, String additionalInsertValues) throws Exception {
		try (Statement statement = connection.createStatement()) {
			String additionalInsertValuesSqlColumns = "";
			String additionalInsertValuesSqlValues = "";
			if (Utilities.isNotBlank(additionalInsertValues)) {
				for (String line : Utilities.splitAndTrimListQuoted(additionalInsertValues, '\n', '\r', ';')) {
					String columnName = line.substring(0, line.indexOf("=")).trim();
					String columnvalue = line.substring(line.indexOf("=") + 1).trim();
					additionalInsertValuesSqlColumns += columnName + ", ";
					additionalInsertValuesSqlValues += columnvalue + ", ";
				}
			}
			
			String insertDataStatement = "INSERT INTO " + destinationTableName + " (" + additionalInsertValuesSqlColumns + joinColumnVendorEscaped(getDbVendor(connection), insertColumns) + ") SELECT " + additionalInsertValuesSqlValues + joinColumnVendorEscaped(getDbVendor(connection), insertColumns) + " FROM " + sourceTableName + " a";
			if (Utilities.isNotEmpty(keyColumnsWithFunctions)) {
				insertDataStatement += " WHERE NOT EXISTS (SELECT 1 FROM " + destinationTableName + " b WHERE " + getKeyColumnEquationList(getDbVendor(connection), keyColumnsWithFunctions, "a", "b") + ")";
			}
			int numberOfInserts = statement.executeUpdate(insertDataStatement);
			connection.commit();
			return numberOfInserts;
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot insert: " + e.getMessage(), e);
		}
	}

	public static int insertAllItems(Connection connection, String sourceTableName, String destinationTableName, List<String> insertColumns, String additionalInsertValues) throws Exception {
		return insertNotExistingItems(connection, sourceTableName, destinationTableName, insertColumns, null, additionalInsertValues);
	}

	public static int updateAllExistingItems(Connection connection, String sourceTableName, String destinationTableName, Collection<String> updateColumns, Collection<String> keyColumns, String itemIndexColumn, boolean updateWithNullValues, String additionalUpdateValues) throws Exception {
		if (keyColumns == null || keyColumns.isEmpty()) {
			throw new Exception("Missing keycolumns");
		}
		
		// Do not update the keycolumns
		updateColumns = new ArrayList<String>(updateColumns);
		updateColumns.removeAll(keyColumns);

		int updatedItems = 0;
		
		if (!updateColumns.isEmpty()) {
			try (Statement statement = connection.createStatement()) {
				String additionalUpdateValuesSql = "";
				if (Utilities.isNotBlank(additionalUpdateValues)) {
					for (String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
						String columnName = line.substring(0, line.indexOf("=")).trim();
						String columnvalue = line.substring(line.indexOf("=") + 1).trim();
						additionalUpdateValuesSql += columnName + " = " + columnvalue + ", ";
					}
				}
				
				updateColumns = new ArrayList<String>(updateColumns);
				updateColumns.removeAll(keyColumns);
				if (updateColumns.size() > 0 || additionalUpdateValuesSql.length() > 0) {
					DbVendor dbVendor = getDbVendor(connection);
					itemIndexColumn = escapeVendorReservedNames(dbVendor, itemIndexColumn);
					String updatedIndexColumn = null;
					try {
						if (updateWithNullValues) {
							String updateSetPart = "";
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								if (updateSetPart.length() > 0) {
									updateSetPart += ", ";
								}
								updateSetPart += updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
									+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))";
							}
							String updateAllAtOnce = "UPDATE " + destinationTableName + " SET " + additionalUpdateValuesSql + updateSetPart
								+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "b") + ")";
							updatedItems = statement.executeUpdate(updateAllAtOnce);
						} else {
							updatedIndexColumn = addIndexedIntegerColumn(connection, destinationTableName, "updatedindex");
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								String updateSingleColumn = "UPDATE " + destinationTableName
									+ " SET " + additionalUpdateValuesSql + updatedIndexColumn + " = 1, " + updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))"
									+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "b") + ")";
								statement.executeUpdate(updateSingleColumn);
							}

							try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + destinationTableName + " WHERE " + updatedIndexColumn + " = 1")) {
								resultSet.next();
								updatedItems = resultSet.getInt(1);
							}
						}
						connection.commit();
					} catch (Exception e) {
						connection.rollback();
						throw e;
					} finally {
						dropColumnIfExists(connection, destinationTableName, updatedIndexColumn);
					}
				}
			} catch (Exception e) {
				throw new Exception("Cannot update: " + e.getMessage(), e);
			}
		}
		
		return updatedItems;
	}
	
	public static int updateFirstExistingItems(Connection connection, String sourceTableName, String destinationTableName, Collection<String> updateColumns, Collection<String> keyColumns, String itemIndexColumn, boolean updateWithNullValues, String additionalUpdateValues) throws Exception {
		if (keyColumns == null || keyColumns.isEmpty()) {
			throw new Exception("Missing keycolumns");
		}
		
		// Do not update the keycolumns
		updateColumns = new ArrayList<String>(updateColumns);
		updateColumns.removeAll(keyColumns);
		
		int updatedItems = 0;
		
		if (!updateColumns.isEmpty()) {
			try (Statement statement = connection.createStatement()) {
				String additionalUpdateValuesSql = "";
				if (Utilities.isNotBlank(additionalUpdateValues)) {
					for (String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
						String columnName = line.substring(0, line.indexOf("=")).trim();
						String columnvalue = line.substring(line.indexOf("=") + 1).trim();
						additionalUpdateValuesSql += columnName + " = " + columnvalue + ", ";
					}
				}
				
				updateColumns = new ArrayList<String>(updateColumns);
				updateColumns.removeAll(keyColumns);
				if (updateColumns.size() > 0 || additionalUpdateValuesSql.length() > 0) {
					String originalItemIndexColumn = null;
					String updatedIndexColumn = null;
					try {
						DbVendor dbVendor = getDbVendor(connection);
						originalItemIndexColumn = createLineNumberIndexColumn(connection, destinationTableName, "itemindex");
						
						// Mark duplicates in temp table
						List<String> keycolumnParts = new ArrayList<String>();
						for (String keyColumn : keyColumns) {
							keycolumnParts.add("src." + escapeVendorReservedNames(dbVendor, keyColumn) + " = " + sourceTableName + "." + escapeVendorReservedNames(dbVendor, keyColumn) + " AND src." + escapeVendorReservedNames(dbVendor, keyColumn) + " IS NOT NULL");
						}
						String updateStatement = "UPDATE " + sourceTableName + " SET " + itemIndexColumn + " = COALESCE((SELECT MIN(src." + originalItemIndexColumn + ") FROM " + destinationTableName + " src WHERE " + Utilities.join(keycolumnParts, " AND ") + "), 0)";
						statement.executeUpdate(updateStatement);
						connection.commit();
						
						// Update with marked items
						if (updateWithNullValues) {
							String updateSetPart = "";
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								if (updateSetPart.length() > 0) {
									updateSetPart += ", ";
								}
								updateSetPart += updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
									+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))";
							}
							String updateAllAtOnce = "UPDATE " + destinationTableName + " SET " + additionalUpdateValuesSql + updateSetPart
								+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + originalItemIndexColumn + " = b." + itemIndexColumn + ")";
							updatedItems = statement.executeUpdate(updateAllAtOnce);
						} else {
							updatedIndexColumn = addIndexedIntegerColumn(connection, destinationTableName, "updatedindex");
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								String updateSingleColumn = "UPDATE " + destinationTableName
									+ " SET " + additionalUpdateValuesSql + updatedIndexColumn + " = 1, " + updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))"
									+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + originalItemIndexColumn + " = b." + itemIndexColumn + ")";
								statement.executeUpdate(updateSingleColumn);
							}
							
							try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + destinationTableName + " WHERE " + updatedIndexColumn + " = 1")) {
								resultSet.next();
								updatedItems = resultSet.getInt(1);
							}
						}
						connection.commit();
					} catch (Exception e) {
						connection.rollback();
						throw e;
					} finally {
						dropColumnIfExists(connection, destinationTableName, originalItemIndexColumn);
						dropColumnIfExists(connection, destinationTableName, updatedIndexColumn);
					}
				}
			} catch (Exception e) {
				throw new Exception("Cannot update first entries: " + e.getMessage(), e);
			}
		}
		
		return updatedItems;
	}

	/**
	 * Check for existing index
	 * Returns null, if check cannot be executed (happens on some db vendors)
	 * 
	 * @param connection
	 * @param tableName
	 * @param keyColumns
	 * @return
	 * @throws Exception 
	 */
	public static Boolean checkForIndex(Connection connection, String tableName, List<String> keyColumns) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		if (dbVendor == DbVendor.Oracle) {
			try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM user_ind_columns WHERE LOWER(table_name) = ? AND LOWER(column_name) = ?")) {
	        	for (String keyColumn : keyColumns) {
	        		keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
	        		statement.setString(1, tableName.toLowerCase());
	        		statement.setString(2, keyColumn.toLowerCase());
	        		try (ResultSet resultSet = statement.executeQuery()) {
			            if (!resultSet.next() || resultSet.getInt(1) <= 0) {
			            	return false;
			            }
	        		}
	        	}
	            return true;
			}
        } else if (dbVendor == DbVendor.MySQL) {
			try (PreparedStatement statement = connection.prepareStatement("SHOW INDEX FROM " + tableName.toLowerCase() + " WHERE column_name = ?")) {
	        	for (String keyColumn : keyColumns) {
	        		keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
	        		statement.setString(1, keyColumn.toLowerCase());
	        		try (ResultSet resultSet = statement.executeQuery()) {
			            if (!resultSet.next()) {
			            	return false;
			            }
	        		}
	        	}
	            return true;
			}
        } else if (dbVendor == DbVendor.MariaDB) {
			try (PreparedStatement statement = connection.prepareStatement("SHOW INDEX FROM " + tableName.toLowerCase() + " WHERE column_name = ?")) {
	        	for (String keyColumn : keyColumns) {
	        		keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
	        		statement.setString(1, keyColumn.toLowerCase());
	        		try (ResultSet resultSet = statement.executeQuery()) {
			            if (!resultSet.next()) {
			            	return false;
			            }
	        		}
	        	}
	            return true;
			}
        } else {
        	return null;
        }
	}
	
	public static String unescapeVendorReservedNames(DbVendor dbVendor, String value) {
		if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
			return Utilities.trimSimultaneously(value, "`");
		} else if (dbVendor == DbVendor.Oracle) {
			return Utilities.trimSimultaneously(value, "\"");
		} else if (dbVendor == DbVendor.Derby) {
			return Utilities.trimSimultaneously(value, "\"");
		} else {
			return value;
		}
	}
	
	public static String escapeVendorReservedNames(DbVendor dbVendor, String value) {
		if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
			switch (value.toLowerCase()) {
				case "accessible":
				case "account":
				case "action":
				case "add":
				case "after":
				case "against":
				case "aggregate":
				case "algorithm":
				case "all":
				case "alter":
				case "always":
				case "analyse":
				case "analyze":
				case "and":
				case "any":
				case "as":
				case "asc":
				case "ascii":
				case "asensitive":
				case "at":
				case "autoextend_size":
				case "auto_increment":
				case "avg":
				case "avg_row_length":
				case "backup":
				case "before":
				case "begin":
				case "between":
				case "bigint":
				case "binary":
				case "binlog":
				case "bit":
				case "blob":
				case "block":
				case "bool":
				case "boolean":
				case "both":
				case "btree":
				case "by":
				case "byte":
				case "cache":
				case "call":
				case "cascade":
				case "cascaded":
				case "case":
				case "catalog_name":
				case "chain":
				case "change":
				case "changed":
				case "channel":
				case "char":
				case "character":
				case "charset":
				case "check":
				case "checksum":
				case "cipher":
				case "class_origin":
				case "client":
				case "close":
				case "coalesce":
				case "code":
				case "collate":
				case "collation":
				case "column":
				case "columns":
				case "column_format":
				case "column_name":
				case "comment":
				case "commit":
				case "committed":
				case "compact":
				case "completion":
				case "compressed":
				case "compression":
				case "concurrent":
				case "condition":
				case "connection":
				case "consistent":
				case "constraint":
				case "constraint_catalog":
				case "constraint_name":
				case "constraint_schema":
				case "contains":
				case "context":
				case "continue":
				case "convert":
				case "cpu":
				case "create":
				case "cross":
				case "cube":
				case "current":
				case "current_date":
				case "current_time":
				case "current_timestamp":
				case "current_user":
				case "cursor":
				case "cursor_name":
				case "data":
				case "database":
				case "databases":
				case "datafile":
				case "date":
				case "datetime":
				case "day":
				case "day_hour":
				case "day_microsecond":
				case "day_minute":
				case "day_second":
				case "deallocate":
				case "dec":
				case "decimal":
				case "declare":
				case "default":
				case "default_auth":
				case "definer":
				case "delayed":
				case "delay_key_write":
				case "delete":
				case "desc":
				case "describe":
				case "des_key_file":
				case "deterministic":
				case "diagnostics":
				case "directory":
				case "disable":
				case "discard":
				case "disk":
				case "distinct":
				case "distinctrow":
				case "div":
				case "do":
				case "double":
				case "drop":
				case "dual":
				case "dumpfile":
				case "duplicate":
				case "dynamic":
				case "each":
				case "else":
				case "elseif":
				case "enable":
				case "enclosed":
				case "encryption":
				case "end":
				case "ends":
				case "engine":
				case "engines":
				case "enum":
				case "error":
				case "errors":
				case "escape":
				case "escaped":
				case "event":
				case "events":
				case "every":
				case "exchange":
				case "execute":
				case "exists":
				case "exit":
				case "expansion":
				case "expire":
				case "explain":
				case "export":
				case "extended":
				case "extent_size":
				case "false":
				case "fast":
				case "faults":
				case "fetch":
				case "fields":
				case "file":
				case "file_block_size":
				case "filter":
				case "first":
				case "fixed":
				case "float":
				case "float4":
				case "float8":
				case "flush":
				case "follows":
				case "for":
				case "force":
				case "foreign":
				case "format":
				case "found":
				case "from":
				case "full":
				case "fulltext":
				case "function":
				case "general":
				case "generated":
				case "geometry":
				case "geometrycollection":
				case "get":
				case "get_format":
				case "global":
				case "grant":
				case "grants":
				case "group":
				case "group_replication":
				case "handler":
				case "hash":
				case "having":
				case "help":
				case "high_priority":
				case "host":
				case "hosts":
				case "hour":
				case "hour_microsecond":
				case "hour_minute":
				case "hour_second":
				case "identified":
				case "if":
				case "ignore":
				case "ignore_server_ids":
				case "import":
				case "in":
				case "index":
				case "indexes":
				case "infile":
				case "initial_size":
				case "inner":
				case "inout":
				case "insensitive":
				case "insert":
				case "insert_method":
				case "install":
				case "instance":
				case "int":
				case "int1":
				case "int2":
				case "int3":
				case "int4":
				case "int8":
				case "integer":
				case "interval":
				case "into":
				case "invoker":
				case "io":
				case "io_after_gtids":
				case "io_before_gtids":
				case "io_thread":
				case "ipc":
				case "is":
				case "isolation":
				case "issuer":
				case "iterate":
				case "join":
				case "json":
				case "key":
				case "keys":
				case "key_block_size":
				case "kill":
				case "language":
				case "last":
				case "leading":
				case "leave":
				case "leaves":
				case "left":
				case "less":
				case "level":
				case "like":
				case "limit":
				case "linear":
				case "lines":
				case "linestring":
				case "list":
				case "load":
				case "local":
				case "localtime":
				case "localtimestamp":
				case "lock":
				case "locks":
				case "logfile":
				case "logs":
				case "long":
				case "longblob":
				case "longtext":
				case "loop":
				case "low_priority":
				case "master":
				case "master_auto_position":
				case "master_bind":
				case "master_connect_retry":
				case "master_delay":
				case "master_heartbeat_period":
				case "master_host":
				case "master_log_file":
				case "master_log_pos":
				case "master_password":
				case "master_port":
				case "master_retry_count":
				case "master_server_id":
				case "master_ssl":
				case "master_ssl_ca":
				case "master_ssl_capath":
				case "master_ssl_cert":
				case "master_ssl_cipher":
				case "master_ssl_crl":
				case "master_ssl_crlpath":
				case "master_ssl_key":
				case "master_ssl_verify_server_cert":
				case "master_tls_version":
				case "master_user":
				case "match":
				case "maxvalue":
				case "max_connections_per_hour":
				case "max_queries_per_hour":
				case "max_rows":
				case "max_size":
				case "max_statement_time":
				case "max_updates_per_hour":
				case "max_user_connections":
				case "medium":
				case "mediumblob":
				case "mediumint":
				case "mediumtext":
				case "memory":
				case "merge":
				case "message_text":
				case "microsecond":
				case "middleint":
				case "migrate":
				case "minute":
				case "minute_microsecond":
				case "minute_second":
				case "min_rows":
				case "mod":
				case "mode":
				case "modifies":
				case "modify":
				case "month":
				case "multilinestring":
				case "multipoint":
				case "multipolygon":
				case "mutex":
				case "mysql_errno":
				case "name":
				case "names":
				case "national":
				case "natural":
				case "nchar":
				case "ndb":
				case "ndbcluster":
				case "never":
				case "new":
				case "next":
				case "no":
				case "nodegroup":
				case "nonblocking":
				case "none":
				case "not":
				case "no_wait":
				case "no_write_to_binlog":
				case "null":
				case "number":
				case "numeric":
				case "nvarchar":
				case "offset":
				case "old_password":
				case "on":
				case "one":
				case "only":
				case "open":
				case "optimize":
				case "optimizer_costs":
				case "option":
				case "optionally":
				case "options":
				case "or":
				case "order":
				case "out":
				case "outer":
				case "outfile":
				case "owner":
				case "pack_keys":
				case "page":
				case "parser":
				case "parse_gcol_expr":
				case "partial":
				case "partition":
				case "partitioning":
				case "partitions":
				case "password":
				case "phase":
				case "plugin":
				case "plugins":
				case "plugin_dir":
				case "point":
				case "polygon":
				case "port":
				case "precedes":
				case "precision":
				case "prepare":
				case "preserve":
				case "prev":
				case "primary":
				case "privileges":
				case "procedure":
				case "processlist":
				case "profile":
				case "profiles":
				case "proxy":
				case "purge":
				case "quarter":
				case "query":
				case "quick":
				case "range":
				case "read":
				case "reads":
				case "read_only":
				case "read_write":
				case "real":
				case "rebuild":
				case "recover":
				case "redofile":
				case "redo_buffer_size":
				case "redundant":
				case "references":
				case "regexp":
				case "relay":
				case "relaylog":
				case "relay_log_file":
				case "relay_log_pos":
				case "relay_thread":
				case "release":
				case "reload":
				case "remove":
				case "rename":
				case "reorganize":
				case "repair":
				case "repeat":
				case "repeatable":
				case "replace":
				case "replicate_do_db":
				case "replicate_do_table":
				case "replicate_ignore_db":
				case "replicate_ignore_table":
				case "replicate_rewrite_db":
				case "replicate_wild_do_table":
				case "replicate_wild_ignore_table":
				case "replication":
				case "require":
				case "reset":
				case "resignal":
				case "restore":
				case "restrict":
				case "resume":
				case "return":
				case "returned_sqlstate":
				case "returns":
				case "reverse":
				case "revoke":
				case "right":
				case "rlike":
				case "rollback":
				case "rollup":
				case "rotate":
				case "routine":
				case "row":
				case "rows":
				case "row_count":
				case "row_format":
				case "rtree":
				case "savepoint":
				case "schedule":
				case "schema":
				case "schemas":
				case "schema_name":
				case "second":
				case "second_microsecond":
				case "security":
				case "select":
				case "sensitive":
				case "separator":
				case "serial":
				case "serializable":
				case "server":
				case "session":
				case "set":
				case "share":
				case "show":
				case "shutdown":
				case "signal":
				case "signed":
				case "simple":
				case "slave":
				case "slow":
				case "smallint":
				case "snapshot":
				case "socket":
				case "some":
				case "soname":
				case "sounds":
				case "source":
				case "spatial":
				case "specific":
				case "sql":
				case "sqlexception":
				case "sqlstate":
				case "sqlwarning":
				case "sql_after_gtids":
				case "sql_after_mts_gaps":
				case "sql_before_gtids":
				case "sql_big_result":
				case "sql_buffer_result":
				case "sql_cache":
				case "sql_calc_found_rows":
				case "sql_no_cache":
				case "sql_small_result":
				case "sql_thread":
				case "sql_tsi_day":
				case "sql_tsi_hour":
				case "sql_tsi_minute":
				case "sql_tsi_month":
				case "sql_tsi_quarter":
				case "sql_tsi_second":
				case "sql_tsi_week":
				case "sql_tsi_year":
				case "ssl":
				case "stacked":
				case "start":
				case "starting":
				case "starts":
				case "stats_auto_recalc":
				case "stats_persistent":
				case "stats_sample_pages":
				case "status":
				case "stop":
				case "storage":
				case "stored":
				case "straight_join":
				case "string":
				case "subclass_origin":
				case "subject":
				case "subpartition":
				case "subpartitions":
				case "super":
				case "suspend":
				case "swaps":
				case "switches":
				case "table":
				case "tables":
				case "tablespace":
				case "table_checksum":
				case "table_name":
				case "temporary":
				case "temptable":
				case "terminated":
				case "text":
				case "than":
				case "then":
				case "time":
				case "timestamp":
				case "timestampadd":
				case "timestampdiff":
				case "tinyblob":
				case "tinyint":
				case "tinytext":
				case "to":
				case "trailing":
				case "transaction":
				case "trigger":
				case "triggers":
				case "true":
				case "truncate":
				case "type":
				case "types":
				case "uncommitted":
				case "undefined":
				case "undo":
				case "undofile":
				case "undo_buffer_size":
				case "unicode":
				case "uninstall":
				case "union":
				case "unique":
				case "unknown":
				case "unlock":
				case "unsigned":
				case "until":
				case "update":
				case "upgrade":
				case "usage":
				case "use":
				case "user":
				case "user_resources":
				case "use_frm":
				case "using":
				case "utc_date":
				case "utc_time":
				case "utc_timestamp":
				case "validation":
				case "value":
				case "values":
				case "varbinary":
				case "varchar":
				case "varcharacter":
				case "variables":
				case "varying":
				case "view":
				case "virtual":
				case "wait":
				case "warnings":
				case "week":
				case "weight_string":
				case "when":
				case "where":
				case "while":
				case "with":
				case "without":
				case "work":
				case "wrapper":
				case "write":
				case "x509":
				case "xa":
				case "xid":
				case "xml":
				case "xor":
				case "year":
				case "year_month":
				case "zerofill":
					return "`" + value + "`";
				default:
					return value;
			}
		} else if (dbVendor == DbVendor.Oracle) {
			switch (value.toLowerCase()) {
				case "access":
				case "account":
				case "activate":
				case "add":
				case "admin":
				case "advise":
				case "after":
				case "all":
				case "all_rows":
				case "allocate":
				case "alter":
				case "analyze":
				case "and":
				case "any":
				case "archive":
				case "archivelog":
				case "array":
				case "as":
				case "asc":
				case "at":
				case "audit":
				case "authenticated":
				case "authorization":
				case "autoextend":
				case "automatic":
				case "backup":
				case "become":
				case "before":
				case "begin":
				case "between":
				case "bfile":
				case "bitmap":
				case "blob":
				case "block":
				case "body":
				case "by":
				case "cache":
				case "cache_instances":
				case "cancel":
				case "cascade":
				case "cast":
				case "cfile":
				case "chained":
				case "change":
				case "char":
				case "char_cs":
				case "character":
				case "check":
				case "checkpoint":
				case "choose":
				case "chunk":
				case "clear":
				case "clob":
				case "clone":
				case "close":
				case "close_cached_open_cursors":
				case "cluster":
				case "coalesce":
				case "column":
				case "columns":
				case "comment":
				case "commit":
				case "committed":
				case "compatibility":
				case "compile":
				case "complete":
				case "composite_limit":
				case "compress":
				case "compute":
				case "connect":
				case "connect_time":
				case "constraint":
				case "constraints":
				case "contents":
				case "continue":
				case "controlfile":
				case "convert":
				case "cost":
				case "cpu_per_call":
				case "cpu_per_session":
				case "create":
				case "current":
				case "current_schema":
				case "curren_user":
				case "cursor":
				case "cycle":
				case "dangling":
				case "database":
				case "datafile":
				case "datafiles":
				case "dataobjno":
				case "date":
				case "dba":
				case "dbhigh":
				case "dblow":
				case "dbmac":
				case "deallocate":
				case "debug":
				case "dec":
				case "decimal":
				case "declare":
				case "default":
				case "deferrable":
				case "deferred":
				case "degree":
				case "delete":
				case "deref":
				case "desc":
				case "directory":
				case "disable":
				case "disconnect":
				case "dismount":
				case "distinct":
				case "distributed":
				case "dml":
				case "double":
				case "drop":
				case "dump":
				case "each":
				case "else":
				case "enable":
				case "end":
				case "enforce":
				case "entry":
				case "escape":
				case "except":
				case "exceptions":
				case "exchange":
				case "excluding":
				case "exclusive":
				case "execute":
				case "exists":
				case "expire":
				case "explain":
				case "extent":
				case "extents":
				case "externally":
				case "failed_login_attempts":
				case "false":
				case "fast":
				case "file":
				case "first_rows":
				case "flagger":
				case "float":
				case "flob":
				case "flush":
				case "for":
				case "force":
				case "foreign":
				case "freelist":
				case "freelists":
				case "from":
				case "full":
				case "function":
				case "global":
				case "globally":
				case "global_name":
				case "grant":
				case "group":
				case "groups":
				case "hash":
				case "hashkeys":
				case "having":
				case "header":
				case "heap":
				case "identified":
				case "idgenerators":
				case "idle_time":
				case "if":
				case "immediate":
				case "in":
				case "including":
				case "increment":
				case "index":
				case "indexed":
				case "indexes":
				case "indicator":
				case "ind_partition":
				case "initial":
				case "initially":
				case "initrans":
				case "insert":
				case "instance":
				case "instances":
				case "instead":
				case "int":
				case "integer":
				case "intermediate":
				case "intersect":
				case "into":
				case "is":
				case "isolation":
				case "isolation_level":
				case "keep":
				case "key":
				case "kill":
				case "label":
				case "layer":
				case "less":
				case "level":
				case "library":
				case "like":
				case "limit":
				case "link":
				case "list":
				case "lob":
				case "local":
				case "lock":
				case "locked":
				case "log":
				case "logfile":
				case "logging":
				case "logical_reads_per_call":
				case "logical_reads_per_session":
				case "long":
				case "manage":
				case "master":
				case "max":
				case "maxarchlogs":
				case "maxdatafiles":
				case "maxextents":
				case "maxinstances":
				case "maxlogfiles":
				case "maxloghistory":
				case "maxlogmembers":
				case "maxsize":
				case "maxtrans":
				case "maxvalue":
				case "min":
				case "member":
				case "minimum":
				case "minextents":
				case "minus":
				case "minvalue":
				case "mlslabel":
				case "mls_label_format":
				case "mode":
				case "modify":
				case "mount":
				case "move":
				case "mts_dispatchers":
				case "multiset":
				case "national":
				case "nchar":
				case "nchar_cs":
				case "nclob":
				case "needed":
				case "nested":
				case "network":
				case "new":
				case "next":
				case "noarchivelog":
				case "noaudit":
				case "nocache":
				case "nocompress":
				case "nocycle":
				case "noforce":
				case "nologging":
				case "nomaxvalue":
				case "nominvalue":
				case "none":
				case "noorder":
				case "nooverride":
				case "noparallel":
				case "noreverse":
				case "normal":
				case "nosort":
				case "not":
				case "nothing":
				case "nowait":
				case "null":
				case "number":
				case "numeric":
				case "nvarchar2":
				case "object":
				case "objno":
				case "objno_reuse":
				case "of":
				case "off":
				case "offline":
				case "oid":
				case "oidindex":
				case "old":
				case "on":
				case "online":
				case "only":
				case "opcode":
				case "open":
				case "optimal":
				case "optimizer_goal":
				case "option":
				case "or":
				case "order":
				case "organization":
				case "oslabel":
				case "overflow":
				case "own":
				case "package":
				case "parallel":
				case "partition":
				case "password":
				case "password_grace_time":
				case "password_life_time":
				case "password_lock_time":
				case "password_reuse_max":
				case "password_reuse_time":
				case "password_verify_function":
				case "pctfree":
				case "pctincrease":
				case "pctthreshold":
				case "pctused":
				case "pctversion":
				case "percent":
				case "permanent":
				case "plan":
				case "plsql_debug":
				case "post_transaction":
				case "precision":
				case "preserve":
				case "primary":
				case "prior":
				case "private":
				case "private_sga":
				case "privilege":
				case "privileges":
				case "procedure":
				case "profile":
				case "public":
				case "purge":
				case "queue":
				case "quota":
				case "range":
				case "raw":
				case "rba":
				case "read":
				case "readup":
				case "real":
				case "rebuild":
				case "recover":
				case "recoverable":
				case "recovery":
				case "ref":
				case "references":
				case "referencing":
				case "refresh":
				case "rename":
				case "replace":
				case "reset":
				case "resetlogs":
				case "resize":
				case "resource":
				case "restricted":
				case "return":
				case "returning":
				case "reuse":
				case "reverse":
				case "revoke":
				case "role":
				case "roles":
				case "rollback":
				case "row":
				case "rowid":
				case "rownum":
				case "rows":
				case "rule":
				case "sample":
				case "savepoint":
				case "sb4":
				case "scan_instances":
				case "schema":
				case "scn":
				case "scope":
				case "sd_all":
				case "sd_inhibit":
				case "sd_show":
				case "segment":
				case "seg_block":
				case "seg_file":
				case "select":
				case "sequence":
				case "serializable":
				case "session":
				case "session_cached_cursors":
				case "sessions_per_user":
				case "set":
				case "share":
				case "shared":
				case "shared_pool":
				case "shrink":
				case "size":
				case "skip":
				case "skip_unusable_indexes":
				case "smallint":
				case "snapshot":
				case "some":
				case "sort":
				case "specification":
				case "split":
				case "sql_trace":
				case "standby":
				case "start":
				case "statement_id":
				case "statistics":
				case "stop":
				case "storage":
				case "store":
				case "structure":
				case "successful":
				case "switch":
				case "sys_op_enforce_not_null$":
				case "sys_op_ntcimg$":
				case "synonym":
				case "sysdate":
				case "sysdba":
				case "sysoper":
				case "system":
				case "table":
				case "tables":
				case "tablespace":
				case "tablespace_no":
				case "tabno":
				case "temporary":
				case "than":
				case "the":
				case "then":
				case "thread":
				case "timestamp":
				case "time":
				case "to":
				case "toplevel":
				case "trace":
				case "tracing":
				case "transaction":
				case "transitional":
				case "trigger":
				case "triggers":
				case "true":
				case "truncate":
				case "tx":
				case "type":
				case "ub2":
				case "uba":
				case "uid":
				case "unarchived":
				case "undo":
				case "union":
				case "unique":
				case "unlimited":
				case "unlock":
				case "unrecoverable":
				case "until":
				case "unusable":
				case "unused":
				case "updatable":
				case "update":
				case "usage":
				case "use":
				case "user":
				case "using":
				case "validate":
				case "validation":
				case "value":
				case "values":
				case "varchar":
				case "varchar2":
				case "varying":
				case "view":
				case "when":
				case "whenever":
				case "where":
				case "with":
				case "without":
				case "work":
				case "write":
				case "writedown":
				case "writeup":
				case "xid":
				case "year":
				case "zone":
					return "\"" + value + "\"";
				default:
					return value;
			}
		} else if (dbVendor == DbVendor.Derby) {
			switch (value.toLowerCase()) {
				case "add":
				case "all":
				case "allocate":
				case "alter":
				case "and":
				case "any":
				case "are":
				case "as":
				case "asc":
				case "assertion":
				case "at":
				case "authorization":
				case "avg":
				case "begin":
				case "between":
				case "bit":
				case "boolean":
				case "both":
				case "by":
				case "call":
				case "cascade":
				case "cascaded":
				case "case":
				case "cast":
				case "char":
				case "character":
				case "check":
				case "close":
				case "collate":
				case "collation":
				case "column":
				case "commit":
				case "connect":
				case "connection":
				case "constraint":
				case "constraints":
				case "continue":
				case "convert":
				case "corresponding":
				case "count":
				case "create":
				case "current":
				case "current_date":
				case "current_time":
				case "current_timestamp":
				case "current_user":
				case "cursor":
				case "deallocate":
				case "dec":
				case "decimal":
				case "declare":
				case "deferrable":
				case "deferred":
				case "delete":
				case "desc":
				case "describe":
				case "diagnostics":
				case "disconnect":
				case "distinct":
				case "double":
				case "drop":
				case "else":
				case "end":
				case "endexec":
				case "escape":
				case "except":
				case "exception":
				case "exec":
				case "execute":
				case "exists":
				case "explain":
				case "external":
				case "false":
				case "fetch":
				case "first":
				case "float":
				case "for":
				case "foreign":
				case "found":
				case "from":
				case "full":
				case "function":
				case "get":
				case "get_current_connection":
				case "global":
				case "go":
				case "goto":
				case "grant":
				case "group":
				case "having":
				case "hour":
				case "identity":
				case "immediate":
				case "in":
				case "indicator":
				case "initially":
				case "inner":
				case "inout":
				case "input":
				case "insensitive":
				case "insert":
				case "int":
				case "integer":
				case "intersect":
				case "into":
				case "is":
				case "isolation":
				case "join":
				case "key":
				case "last":
				case "left":
				case "like":
				case "longint":
				case "lower":
				case "ltrim":
				case "match":
				case "max":
				case "min":
				case "minute":
				case "national":
				case "natural":
				case "nchar":
				case "nvarchar":
				case "next":
				case "no":
				case "not":
				case "null":
				case "nullif":
				case "numeric":
				case "of":
				case "on":
				case "only":
				case "open":
				case "option":
				case "or":
				case "order":
				case "out":
				case "outer":
				case "output":
				case "overlaps":
				case "pad":
				case "partial":
				case "prepare":
				case "preserve":
				case "primary":
				case "prior":
				case "privileges":
				case "procedure":
				case "public":
				case "read":
				case "real":
				case "references":
				case "relative":
				case "restrict":
				case "revoke":
				case "right":
				case "rollback":
				case "rows":
				case "rtrim":
				case "schema":
				case "scroll":
				case "second":
				case "select":
				case "session_user":
				case "set":
				case "smallint":
				case "some":
				case "space":
				case "sql":
				case "sqlcode":
				case "sqlerror":
				case "sqlstate":
				case "substr":
				case "substring":
				case "sum":
				case "system_user":
				case "table":
				case "temporary":
				case "timezone_hour":
				case "timezone_minute":
				case "to":
				case "trailing":
				case "transaction":
				case "translate":
				case "translation":
				case "true":
				case "union":
				case "unique":
				case "unknown":
				case "update":
				case "upper":
				case "user":
				case "using":
				case "values":
				case "varchar":
				case "varying":
				case "view":
				case "whenever":
				case "where":
				case "with":
				case "work":
				case "write":
				case "xml":
				case "xmlexists":
				case "xmlparse":
				case "xmlserialize":
				case "year":
					return "\"" + value + "\"";
				default:
					return value;
			}
		} else {
			return value;
		}
	}
	
	public static String joinColumnVendorEscaped(DbVendor dbVendor, Collection<String> columnNames) {
		StringBuilder returnValue = new StringBuilder();
		for (String columnName : columnNames) {
			if (returnValue.length() > 0) {
				returnValue.append(", ");
			}
			returnValue.append(escapeVendorReservedNames(dbVendor, columnName));
		}
		return returnValue.toString();
	}

	/**
	 * Special shutdown command to free single user derby db for next connection maybe of another thread
	 * 
	 * @param dbName
	 * @throws Exception
	 */
	public static void shutDownDerbyDb(String dbName) throws Exception {
		dbName = Utilities.replaceHomeTilde(dbName);
		if (!new File(dbName).exists()) {
			throw new DbNotExistsException("Derby db directory '" + dbName + "' is not available");
		} else if (!new File(dbName).isDirectory()) {
			throw new Exception("Derby db directory '" + dbName + "' is not a directory");
		}
		try {
			DriverManager.getConnection("jdbc:derby:" + Utilities.replaceHomeTilde(dbName) + ";shutdown=true");
		} catch (SQLNonTransientConnectionException e) {
			// Derby shutdown ALWAYS throws a SQLNonTransientConnectionException by intention (see also: http://db.apache.org/derby/docs/10.3/devguide/tdevdvlp20349.html)
		}
	}
}
