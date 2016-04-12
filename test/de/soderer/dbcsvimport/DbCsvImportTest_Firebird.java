package de.soderer.dbcsvimport;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;

public class DbCsvImportTest_Firebird {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_FIREBIRD_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_FIREBIRD_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_FIREBIRD_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_FIREBIRD_TEST");
	
	public static final Date TEST_DATE = new Date(new Date().getTime() / 1000 * 1000);
	public static final Date TEST_DATE_WITHOUT_TIME = DateUtilities.getDayWithoutTime(new GregorianCalendar()).getTime();
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "DOUBLE PRECISION", "VARCHAR(32765)", "BLOB SUB_TYPE BINARY", "BLOB SUB_TYPE TEXT", "TIMESTAMP" };
	
	public static File INPUTFILE_CSV = new File("~/temp/test_tbl.csv".replace("~", System.getProperty("user.home")));
	
	@Before
	public void setup() throws Exception {
		INPUTFILE_CSV.delete();
		
		Connection connection = null;
		Statement statement = null;
		try {
			connection = DbUtilities.createConnection(DbVendor.Firebird, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray());
			
			statement = connection.createStatement();
			
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(connection);
		}
	}
	
	@After
	public void tearDown() throws Exception {
		INPUTFILE_CSV.delete();
		
		Connection connection = null;
		Statement statement = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = DbUtilities.createConnection(DbVendor.Firebird, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray());
			
			statement = connection.createStatement();
			
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			Utilities.closeQuietly(preparedStatement);
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(connection);
		}
	}
	
	private void createEmptyTestTable() throws Exception {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = DbUtilities.createConnection(DbVendor.Firebird, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray());
			
			statement = connection.createStatement();
			
			String dataColumnsPart = "";
			String dataColumnsPartForInsert = "";
			for (String dataType : DATA_TYPES) {
				String columnName = "column_" + dataType.toLowerCase();
				if (columnName.contains("(")) {
					columnName = columnName.substring(0, columnName.indexOf("("));
				} else if (dataType.equalsIgnoreCase("BLOB SUB_TYPE BINARY")) {
					columnName = "column_blob";
				} else if (dataType.equalsIgnoreCase("BLOB SUB_TYPE TEXT")) {
					columnName = "column_clob";
				} else if (dataType.equalsIgnoreCase("DOUBLE PRECISION")) {
					columnName = "column_double";
				}
				
				if (dataColumnsPart.length() > 0) {
					dataColumnsPart += ", ";
				}
				dataColumnsPart += columnName + " " + dataType;
				if (dataColumnsPartForInsert.length() > 0) {
					dataColumnsPartForInsert += ", ";
				}
				dataColumnsPartForInsert += columnName;
			}
			statement.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, " + dataColumnsPart + ")");
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(connection);
		}
	}
	
	private void prefillTestTable() throws Exception {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = DbUtilities.createConnection(DbVendor.Firebird, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray());
			statement = connection.createStatement();
			statement.executeUpdate("INSERT INTO test_tbl (id, column_integer, column_varchar) VALUES (COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1), 1, '<test_text>_1')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (id, column_integer, column_varchar) VALUES (COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1), 3, '<test_text>_3')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (id, column_integer, column_varchar) VALUES (COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1), 999, '<test_text>_999')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(connection);
		}
	}
	
	private String exportTestTable() throws Exception {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = DbUtilities.createConnection(DbVendor.Firebird, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray());
			return DbUtilities.readoutTable(connection, "test_tbl", ';', '\"').replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>");
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(connection);
		}
	}
	
	@Test
	public void testCsvImportNoMapping() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column_integer; column_double; column_varchar; column_clob\n"
				+ "123; 123.456; aBcDeF123; aBcDeF123\n").getBytes("UTF-8"));
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF123;123.456;123;; aBcDeF123\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImport() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
				+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1234;123.456;123;2003-02-01 11:12:13.0; aBcDeF123\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportBig() {
		try {
			createEmptyTestTable();
			
			int numberOfLines = 1200;
			
			StringBuilder dataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				dataPart.append(i + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			}
			FileUtilities.write(INPUTFILE_CSV, ("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n" + dataPart.toString()).getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			
			StringBuilder expectedDataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				expectedDataPart.append((i + 1) + ";; aBcDeF1234;123.456;" + i + ";2003-02-01 11:12:13.0; aBcDeF123\n");
			}
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ expectedDataPart,
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportTrimmed() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
				+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-t", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;;aBcDeF1234;123.456;123;2003-02-01 11:12:13.0;aBcDeF123\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportBase64Blob() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_blob; column_clob; column_timestamp; column_date\n"
				+ "123; 123.456; aBcDeF123; " + Utilities.encodeBase64("abc".getBytes("UTF-8")) + "; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob='column_blob'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-t", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;YWJj;aBcDeF1234;123.456;123;2003-02-01 11:12:13.0;aBcDeF123\n",
			exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportErrorDataType() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_clob; column_timestamp\n"
				+ "121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13\n"
				+ "122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13\n"
				+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1234;123.456;121;2003-02-01 11:12:13.0; aBcDeF123\n"
				+ "2;; aBcDeF1234;123.456;123;2003-02-01 11:12:13.0; aBcDeF123\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportErrorStructure() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
				+ "121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
				+ "122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23"
				+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportErrorDataTypeRollback() {
		try {
			createEmptyTestTable();
			
			FileUtilities.write(INPUTFILE_CSV,
				("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
				+ "121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
				+ "122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
				+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes("UTF-8"));
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-c", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportInsertWithKeycolumns() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			int i = 1;
			StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append((i) + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i++) + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i) + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i++) + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i) + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i++) + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append((i++) + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "INSERT", "-k", "column_integer", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;;;;1;;\"<test_text>_1\"\n"
				+ "2;;;;3;;\"<test_text>_3\"\n"
				+ "3;;;;999;;\"<test_text>_999\"\n"
				+ "4;; aBcDeF1234;123.456;2;2003-02-01 11:12:13.0; aBcDeF123\n"
				+ "5;; aBcDeF1235;123.456;4;2003-02-01 11:12:13.0;\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportUpdateWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "UPDATE", "-k", "column_integer", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1235_1;123.456;1;2003-02-01 11:12:13.0;\n"
				+ "2;; aBcDeF1235_3;123.456;3;2003-02-01 11:12:13.0;\n"
				+ "3;;;;999;;\"<test_text>_999\"\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportUpdateWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_11; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_21; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_31; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_41; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "UPDATE", "-k", "column_integer", "-u", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1235_11;123.456;1;2003-02-01 11:12:13.0; aBcDeF123_1\n"
				+ "2;; aBcDeF1235_31;123.456;3;2003-02-01 11:12:13.0; aBcDeF123_3\n"
				+ "3;;;;999;;\"<test_text>_999\"\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportUpsertWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1235_1;123.456;1;2003-02-01 11:12:13.0;\n"
				+ "2;; aBcDeF1235_3;123.456;3;2003-02-01 11:12:13.0;\n"
				+ "3;;;;999;;\"<test_text>_999\"\n"
				+ "4;; aBcDeF1234;123.456;2;2003-02-01 11:12:13.0; aBcDeF123_2\n"
				+ "5;; aBcDeF1235_4;123.456;4;2003-02-01 11:12:13.0;\n"
				+ "6;; aBcDeF1234;123.456;5;2003-02-01 11:12:13.0; aBcDeF123_5\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportUpsertWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u", "-insvalues", "id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;; aBcDeF1235_1;123.456;1;2003-02-01 11:12:13.0; aBcDeF123_1\n"
				+ "2;; aBcDeF1235_3;123.456;3;2003-02-01 11:12:13.0; aBcDeF123_3\n"
				+ "3;;;;999;;\"<test_text>_999\"\n"
				+ "4;; aBcDeF1234;123.456;2;2003-02-01 11:12:13.0; aBcDeF123_2\n"
				+ "5;; aBcDeF1235_4;123.456;4;2003-02-01 11:12:13.0;\n"
				+ "6;; aBcDeF1234;123.456;5;2003-02-01 11:12:13.0; aBcDeF123_5\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCsvImportUpsertAdditionalValues() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			
			StringBuilder data = new StringBuilder();
			data.append("column integer;column_clob\n");
			data.append("1;Original1\n");
			data.append("1;Original2\n");
			data.append("2;Original1\n");
			data.append("2;Original2\n");
			data.append("3;Original1\n");
			data.append("3;Original2\n");
			data.append("4;Original\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes("UTF-8"));
			
			String mapping = "column_integer='column integer';column_clob='column_clob'";
			Assert.assertEquals(0, DbCsvImport._main(new String[] { "firebird", HOSTNAME, USERNAME, DBNAME, "test_tbl", "~/temp/test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u", "-insvalues", "column_varchar='Insert';id=COALESCE((SELECT MAX(id) + 1 FROM test_tbl), 1)", "-updvalues", "column_varchar='Update'", PASSWORD }));
			Assert.assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
				+ "1;;Original2;;1;;Update\n"
				+ "2;;Original2;;3;;Update\n"
				+ "3;;;;999;;\"<test_text>_999\"\n"
				+ "4;;Original1;;2;;Insert\n"
				+ "5;;Original;;4;;Insert\n",
				exportTestTable());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
