package de.soderer.dbimport;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Element;

import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.xml.XmlUtilities;
import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonWriter;

public class DbImportTest_SQLite {
	public static final String SQLITE_DB_FILE = System.getProperty("user.home") + File.separator + "temp" + File.separator + "test.sqlite";

	public static final String[] DATA_TYPES = new String[] { "INTEGER", "DOUBLE", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File INPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File INPUTFILE2_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test2_tbl.csv"));
	public static File INPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File INPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File BLOB_DATA_FILE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test.blob"));

	@BeforeClass
	public static void setupTestClass() throws Exception {
		try {
			DbUtilities.deleteDatabase(DbVendor.SQLite, SQLITE_DB_FILE);
		} catch (final Exception e) {
			// Do nothing
		}

		try (Connection connection = DbUtilities.createNewDatabase(DbVendor.SQLite, SQLITE_DB_FILE)) {
			// Just close the connection
		}
	}

	@Before
	public void setup() throws Exception {
		INPUTFILE_CSV.delete();
		if (INPUTFILE2_CSV.exists()) {
			INPUTFILE2_CSV.delete();
		}
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false)) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				try (Statement statement = connection.createStatement()) {
					statement.execute("DROP TABLE test_tbl");
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@After
	public void tearDown() throws Exception {
		INPUTFILE_CSV.delete();
		if (INPUTFILE2_CSV.exists()) {
			INPUTFILE2_CSV.delete();
		}
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false);
				Statement statement = connection.createStatement()) {

			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@AfterClass
	public static void tearDownTestClass() throws Exception {
		try {
			DbUtilities.deleteDatabase(DbVendor.SQLite, SQLITE_DB_FILE);
		} catch (final Exception e) {
			// Do nothing
		}
	}

	private void createEmptyTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false);
				Statement statement = connection.createStatement()) {

			String dataColumnsPart = "";
			String dataColumnsPartForInsert = "";
			for (final String dataType : DATA_TYPES) {
				String columnName = "column_" + dataType.toLowerCase();
				if (columnName.contains("(")) {
					columnName = columnName.substring(0, columnName.indexOf("("));
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
			statement.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, " + dataColumnsPart + ")");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void prefillTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false);
				Statement statement = connection.createStatement()) {
			statement.executeUpdate("INSERT INTO test_tbl (column_integer, column_varchar) VALUES (1, '<test_text>_1')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (column_integer, column_varchar) VALUES (3, '<test_text>_3')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (column_integer, column_varchar) VALUES (999, '<test_text>_999')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String exportTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false)) {
			return DbUtilities.readoutTable(connection, "test_tbl", ';', '\"').replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String export(final String tablename) throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.SQLite, "", SQLITE_DB_FILE, "", null), false)) {
			return DbUtilities.readout(connection, tablename, ';', '\"').replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void testCsvImportNoMapping() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("column_integer; column_double; column_varchar; column_clob\n"
							+ "123; 123.456; aBcDeF123; aBcDeF123\n").getBytes(StandardCharsets.UTF_8));
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF123;;123.456;123;; aBcDeF123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImport() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
							+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1234;2003-02-01;123.456;123;2003-02-01T11:12:13; aBcDeF123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportBig() {
		try {
			createEmptyTestTable();

			final int numberOfLines = 1200;

			final StringBuilder dataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				dataPart.append(i + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			}
			FileUtilities.write(INPUTFILE_CSV, ("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n" + dataPart.toString()).getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";

			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping }));

			final StringBuilder expectedDataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				expectedDataPart.append((i + 1) + ";; aBcDeF1234;2003-03-01;123.456;" + i + ";2003-02-01T11:12:13; aBcDeF123\n");
			}
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ expectedDataPart,
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportTrimmed() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
							+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-t" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;;aBcDeF1234;2003-02-01;123.456;123;2003-02-01T11:12:13;aBcDeF123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportBase64Blob() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("column integer; column_double; column_varchar; column_blob; column_clob; column_timestamp; column_date\n"
							+ "123; 123.456; aBcDeF123; " + Utilities.encodeBase64("abc".getBytes(StandardCharsets.UTF_8)) + "; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob='column_blob'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-t" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;" + Utilities.encodeBase64("abc".getBytes(StandardCharsets.UTF_8)) + ";aBcDeF1234;2003-02-01;123.456;123;2003-02-01T11:12:13;aBcDeF123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportErrorDataType() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n"
							+ "121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1234;2003-02-01;123.456;121;2003-02-01T11:12:13; aBcDeF123\n"
							+ "2;; aBcDeF1234;2003-02-01;123.456;123;2003-02-01T11:12:13; aBcDeF123\n",
							exportTestTable());
		} catch (final Exception e) {
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
							+ "122; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23"
							+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n",
					exportTestTable());
		} catch (final Exception e) {
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
							+ "123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-c" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n",
					exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertWithKeycolumns() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append(1 + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(1 + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(2 + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(2 + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(3 + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(3 + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append(4 + "; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "INSERT", "-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;;;;;1;;\"<test_text>_1\"\n"
							+ "2;;;;;3;;\"<test_text>_3\"\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "5;; aBcDeF1235;2003-03-01;123.456;4;2003-02-01T11:12:13;\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpdateWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPDATE", "-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpdateWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_11; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_21; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_31; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_41; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPDATE", "-k", "column_integer", "-u" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_11;2003-03-01;123.456;1;2003-02-01T11:12:13; aBcDeF123_1\n"
							+ "2;; aBcDeF1235_31;2003-03-01;123.456;3;2003-02-01T11:12:13; aBcDeF123_3\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13; aBcDeF123_1\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13; aBcDeF123_3\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13; aBcDeF123_2\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertAdditionalValues() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer;column_clob\n");
			data.append("1;Original1\n");
			data.append("1;Original2\n");
			data.append("2;Original1\n");
			data.append("2;Original2\n");
			data.append("3;Original1\n");
			data.append("3;Original2\n");
			data.append("4;Original\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer';column_clob='column_clob'";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u", "-insvalues", "column_varchar='Insert'", "-updvalues", "column_varchar='Update'" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;;Original2;;;1;;Update\n"
							+ "2;;Original2;;;3;;Update\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;;Original2;;;2;;Insert\n"
							+ "5;;Original;;;4;;Insert\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertClobFile() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("column integer;column_clob\n");
			data.append("1;" + BLOB_DATA_FILE.getAbsolutePath() + "\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer';column_clob='column_clob' file";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u", "-insvalues", "column_varchar='Insert'", "-updvalues", "column_varchar='Update'" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;;\"<test_text>\";;;1;;Update\n"
							+ "2;;;;;3;;\"<test_text>_3\"\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertBlobFile() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("column integer;column_blob\n");
			data.append("1;" + BLOB_DATA_FILE.getAbsolutePath() + "\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer';column_blob='column_blob' file";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u", "-insvalues", "column_varchar='Insert'", "-updvalues", "column_varchar='Update'" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXpBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWjAxMjM0NTY3ODkgw6TDtsO8w5/DhMOWw5zCtSE/wqdA4oKsJCUmL1w8Pigpe31bXSciwrRgXsKwwrnCssKzKiMuLDs6PSstfl98wr3CvMKs;;;;1;;Update\n"
							+ "2;;;;;3;;\"<test_text>_3\"\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTable() {

		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("column integer;column_varchar;column_double;not%included\n");
			data.append("001;AbcÄ123;1.2300;not Included\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer';column_varchar='column_varchar';column_double='column_double'";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-create", "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u" }));
			Assert.assertEquals(
					"column_integer;column_double;column_varchar\n"
							+ "1;1.23;AbcÄ123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateDatabase() {
		try {
			DbUtilities.deleteDatabase(DbVendor.SQLite, SQLITE_DB_FILE);

			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("column integer;column_varchar;column_double;not%included\n");
			data.append("001;AbcÄ123;1.2300;not Included\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer';column_varchar='column_varchar';column_double='column_double'";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-create", "-table", "test_tbl", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", "-m", mapping, "-i", "UPSERT", "-k", "column_integer", "-u" }));
			Assert.assertEquals(
					"column_integer;column_double;column_varchar\n"
							+ "1;1.23;AbcÄ123\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonImportUpsert() {
		JsonWriter jsonWriter = null;
		try {
			createEmptyTestTable();
			prefillTestTable();

			final JsonArray jsonArray = new JsonArray();

			JsonObject jsonObject = new JsonObject();
			jsonObject.add("column integer", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_1");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_clob", " aBcDeF1235_1");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_2");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_clob", " aBcDeF1235_2");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_3");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_clob", " aBcDeF1235_3");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 4);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_clob", " aBcDeF1235_4");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column integer", 5);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_5");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonWriter = new JsonWriter(new FileOutputStream(INPUTFILE_JSON), StandardCharsets.UTF_8);
			jsonWriter.add(jsonArray);
			Utilities.closeQuietly(jsonWriter);
			jsonWriter = null;

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-x", "JSON", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.json", "-m", mapping, "-i", "UPSERT", "-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			Utilities.closeQuietly(jsonWriter);
		}
	}

	@Test
	public void testXmlImportUpsert() {
		final JsonWriter jsonWriter = null;
		try {
			createEmptyTestTable();
			prefillTestTable();

			final Element xmlRootTag = XmlUtilities.createRootTagNode(XmlUtilities.getEmptyDocument(), "Entries");
			Element entryNode;

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_1");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1235_1");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_2");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1235_2");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_3");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1235_3");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 4);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1235_4");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "column_integer", 5);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_5");
			XmlUtilities.appendTextValueNode(entryNode, "column_clob", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			FileUtilities.write(INPUTFILE_XML, XmlUtilities.convertXML2ByteArray(xmlRootTag, StandardCharsets.UTF_8));

			final String mapping = "column_integer='column_integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-table", "test_tbl", "-x", "XML", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.xml", "-m", mapping, "-i", "UPSERT", "-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			Utilities.closeQuietly(jsonWriter);
		}
	}

	@Test
	public void testCsvImportUpsertWithNullMakeUnique() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"sqlite",
					SQLITE_DB_FILE,
					"-table", "test_tbl",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-d", DuplicateMode.MAKE_UNIQUE_JOIN.toString(),
					"-k", "column_integer" }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithNullWithFirstOnly() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"sqlite",
					SQLITE_DB_FILE,
					"-table", "test_tbl",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "column_integer",
					"-d", DuplicateMode.UPDATE_FIRST_JOIN.toString() }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;;;;;1;;\"<test_text>_1\"\n"
							+ "5;;;;;3;;\"<test_text>_3\"\n"
							+ "6;;;;;999;;\"<test_text>_999\"\n"
							+ "7;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13;\n"
							+ "8;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "9;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithoutNullWithFirstOnly() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"sqlite",
					SQLITE_DB_FILE,
					"-table", "test_tbl",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-u",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "column_integer",
					"-d", DuplicateMode.UPDATE_FIRST_JOIN.toString() }));
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T11:12:13; aBcDeF123_1\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T11:12:13; aBcDeF123_3\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;;;;;1;;\"<test_text>_1\"\n"
							+ "5;;;;;3;;\"<test_text>_3\"\n"
							+ "6;;;;;999;;\"<test_text>_999\"\n"
							+ "7;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T11:12:13; aBcDeF123_2\n"
							+ "8;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T11:12:13;\n"
							+ "9;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T11:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithTimezone() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("column integer; column_double; column_varchar; column_clob; column_timestamp; column_date\n");
			data.append("1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "column_integer='column integer'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-table", "test_tbl",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "column_integer",
					"-u",
					"-dbtz", "Europe/Sofia", // UTC+02:00
					"-idtz", "Europe/Dublin" })); // UTC+00:00
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01T13:12:13; aBcDeF123_1\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01T13:12:13; aBcDeF123_3\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01T13:12:13; aBcDeF123_2\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01T13:12:13;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01T13:12:13; aBcDeF123_5\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithWildCard() {
		try {
			final StringBuilder data = new StringBuilder();
			data.append("column_integer;column_double;column_varchar;column_clob;column_timestamp;column_date\n");
			data.append("1;123.456;aBcDeF123_1;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data.append("1;123.456;;aBcDeF1235_1;01.02.2003 11:12:13;01.03.2003\n");
			data.append("2;123.456;aBcDeF123_2;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data.append("2;123.456;;aBcDeF1235_2;01.02.2003 11:12:13;01.03.2003\n");
			data.append("3;123.456;aBcDeF123_3;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data.append("3;123.456;;aBcDeF1235_3;01.02.2003 11:12:13;01.03.2003\n");
			data.append("4;123.456;;aBcDeF1235_4;01.02.2003 11:12:13;01.03.2003\n");
			data.append("5;123.456;aBcDeF123_5;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final StringBuilder data2 = new StringBuilder();
			data2.append("column_integer;column_double;column_varchar;column_clob;column_timestamp;column_date\n");
			data2.append("1;123.456;aBcDeF123_1;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("1;123.456;;aBcDeF1235_1;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("2;123.456;aBcDeF123_2;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("2;123.456;;aBcDeF1235_2;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("3;123.456;aBcDeF123_3;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("3;123.456;;aBcDeF1235_3;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("4;123.456;;aBcDeF1235_4;01.02.2003 11:12:13;01.03.2003\n");
			data2.append("5;123.456;aBcDeF123_5;aBcDeF1234;01.02.2003 11:12:13;01.03.2003\n");
			FileUtilities.write(INPUTFILE2_CSV, data2.toString().getBytes(StandardCharsets.UTF_8));

			Assert.assertEquals(0, DbImport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-create",
					"-table", "*",
					"-import", "~" + File.separator + "temp" + File.separator + "test*_tbl.csv",
					"-i", "INSERT" }));
			Assert.assertEquals(
					"column_integer;column_double;column_varchar;column_clob;column_timestamp;column_date\n"
							+ "1;123.456;;aBcDeF1235_1;2003-02-01T11:12:13;2003-03-01\n"
							+ "1;123.456;aBcDeF123_1;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "2;123.456;;aBcDeF1235_2;2003-02-01T11:12:13;2003-03-01\n"
							+ "2;123.456;aBcDeF123_2;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "3;123.456;;aBcDeF1235_3;2003-02-01T11:12:13;2003-03-01\n"
							+ "3;123.456;aBcDeF123_3;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "4;123.456;;aBcDeF1235_4;2003-02-01T11:12:13;2003-03-01\n"
							+ "5;123.456;aBcDeF123_5;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n",
							export("SELECT column_integer, column_double, column_varchar, column_clob, column_timestamp, column_date FROM test_tbl ORDER BY column_integer, column_double, column_varchar"));
			Assert.assertEquals(
					"column_integer;column_double;column_varchar;column_clob;column_timestamp;column_date\n"
							+ "1;123.456;;aBcDeF1235_1;2003-02-01T11:12:13;2003-03-01\n"
							+ "1;123.456;aBcDeF123_1;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "2;123.456;;aBcDeF1235_2;2003-02-01T11:12:13;2003-03-01\n"
							+ "2;123.456;aBcDeF123_2;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "3;123.456;;aBcDeF1235_3;2003-02-01T11:12:13;2003-03-01\n"
							+ "3;123.456;aBcDeF123_3;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n"
							+ "4;123.456;;aBcDeF1235_4;2003-02-01T11:12:13;2003-03-01\n"
							+ "5;123.456;aBcDeF123_5;aBcDeF1234;2003-02-01T11:12:13;2003-03-01\n",
							export("SELECT column_integer, column_double, column_varchar, column_clob, column_timestamp, column_date FROM test2_tbl ORDER BY column_integer, column_double, column_varchar"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
