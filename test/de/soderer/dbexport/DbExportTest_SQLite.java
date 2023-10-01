package de.soderer.dbexport;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.zip.ZipUtilities;

public class DbExportTest_SQLite {
	public static final String SQLITE_DB_FILE = System.getProperty("user.home") + "" + File.separator + "temp" + File.separator + "test.sqlite";

	public static LocalDateTime TEST_DATETIME;
	public static LocalDate TEST_DATE;
	public static Object[][] DATA_VALUES;
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "REAL", "TEXT", "BLOB", "TIMESTAMP", "DATE" };

	public static File OUTPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File OUTPUTFILE_CSV_SEQUENCE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "sqlite_sequence.csv"));
	public static File OUTPUTFILE_CSV_ZIPPED = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.zip"));
	public static File OUTPUTFILE_CSV_ZIPPED_SEQUENCE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "sqlite_sequence.csv.zip"));
	public static File OUTPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File OUTPUTFILE_XML_SEQUENCE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "sqlite_sequence.xml"));
	public static File OUTPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File OUTPUTFILE_JSON_SEQUENCE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "sqlite_sequence.json"));
	public static File OUTPUTFILE_SQL = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.sql"));
	public static File OUTPUTFILE_SQL_SEQUENCE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "sqlite_sequence.sql"));

	@BeforeClass
	public static void setupTestClass() throws Exception {
		TEST_DATETIME = DateUtilities.parseLocalDateTime(DateUtilities.DD_MM_YYYY_HH_MM_SS, "01.02.2003 04:05:06");
		TEST_DATE = DateUtilities.parseLocalDate(DateUtilities.DD_MM_YYYY, "01.02.2003");
		DATA_VALUES =
				new Object[][]{
			new Object[]{ 1, 1.1237, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TEST_DATETIME, TEST_DATE},
			new Object[]{ 2, 2.1237, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TEST_DATETIME, TEST_DATE},
			new Object[]{ null, null, null, null, null, null}
		};

		new File(SQLITE_DB_FILE).delete();

		try (Connection connection = DbUtilities.createNewDatabase(DbVendor.SQLite, SQLITE_DB_FILE)) {
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

			try (Statement statement = connection.createStatement()) {
				statement.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, " + dataColumnsPart + ")");
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_tbl (" + dataColumnsPartForInsert + ") VALUES (" + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
				for (final Object[] itemData : DATA_VALUES) {
					preparedStatement.clearParameters();
					int paramIndex = 1;
					for (final Object itemValue : itemData) {
						preparedStatement.setObject(paramIndex++, itemValue);
					}
					preparedStatement.executeUpdate();
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Before
	public void setup() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_SEQUENCE.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_CSV_ZIPPED_SEQUENCE.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_XML_SEQUENCE.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_JSON_SEQUENCE.delete();
		OUTPUTFILE_SQL.delete();
		OUTPUTFILE_SQL_SEQUENCE.delete();
	}

	@After
	public void tearDown() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_SEQUENCE.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_CSV_ZIPPED_SEQUENCE.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_XML_SEQUENCE.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_JSON_SEQUENCE.delete();
		OUTPUTFILE_SQL.delete();
		OUTPUTFILE_SQL_SEQUENCE.delete();
	}

	@AfterClass
	public static void tearDownTestClass() {
		new File(SQLITE_DB_FILE).delete();
	}

	@Test
	public void testCsvSelect() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "SELECT column_text FROM test_tbl WHERE id < 3", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"column_text\n"
							+ "\"<test_text>\"\n"
							+ "\"<test_text>\"\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsv() {
		try {
			DbExport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-export", "*",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-dateFormat", "dd.MM.YYYY",
					"-dateTimeFormat", "dd.MM.YYYY HH:mm:ss"});

			Assert.assertTrue(OUTPUTFILE_CSV.exists());

			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;01.02.2003;1;1,124;\"<test_text>\";01.02.2003 04:05:06\n"
							+ "2;<test_text_base64>;01.02.2003;2;2,124;\"<test_text>\";01.02.2003 04:05:06\n"
							+ "3;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithNullString() {
		try {
			DbExport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-export", "*",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-n", "NULL" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;2003-02-01;1;1,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "2;<test_text_base64>;2003-02-01;2;2,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "3;NULL;NULL;NULL;NULL;NULL;NULL\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithTimeZone() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-n", "NULL", "-dbtz", "Europe/Sofia", "-edtz", "Europe/Dublin" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;2003-02-01;1;1,124;\"<test_text>\";2003-02-01T02:05:06\n"
							+ "2;<test_text_base64>;2003-02-01;2;2,124;\"<test_text>\";2003-02-01T02:05:06\n"
							+ "3;NULL;NULL;NULL;NULL;NULL;NULL\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvBeautified() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-beautify" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob                                                                                                                                                                     ;column_date;column_integer;column_real;column_text                                                                                                         ;column_timestamp   \n"
							+ " 1;<test_text_base64>;2003-02-01 ;             1;      1,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ " 2;<test_text_base64>;2003-02-01 ;             2;      2,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ " 3;                                                                                                                                                                                ;           ;              ;           ;                                                                                                                    ;                   \n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvZipped() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-z" });

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;2003-02-01;1;1,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "2;<test_text_base64>;2003-02-01;2;2,124;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "3;;;;;;\n",
							new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonBeautified() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", "-beautify" });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"[\n"
							+ "	{\n"
							+ "		\"id\": 1,\n"
							+ "		\"column_blob\": \"<test_text_base64>\",\n"
							+ "		\"column_date\": \"2003-02-01\",\n"
							+ "		\"column_integer\": 1,\n"
							+ "		\"column_real\": 1.1237,\n"
							+ "		\"column_text\": \"<test_text>\",\n"
							+ "		\"column_timestamp\": \"2003-02-01T04:05:06\"\n"
							+ "	},\n"
							+ "	{\n"
							+ "		\"id\": 2,\n"
							+ "		\"column_blob\": \"<test_text_base64>\",\n"
							+ "		\"column_date\": \"2003-02-01\",\n"
							+ "		\"column_integer\": 2,\n"
							+ "		\"column_real\": 2.1237,\n"
							+ "		\"column_text\": \"<test_text>\",\n"
							+ "		\"column_timestamp\": \"2003-02-01T04:05:06\"\n"
							+ "	},\n"
							+ "	{\n"
							+ "		\"id\": 3,\n"
							+ "		\"column_blob\": null,\n"
							+ "		\"column_date\": null,\n"
							+ "		\"column_integer\": null,\n"
							+ "		\"column_real\": null,\n"
							+ "		\"column_text\": null,\n"
							+ "		\"column_timestamp\": null\n"
							+ "	}\n"
							+ "]",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\"").replace("/", "\\/"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)).replace("/", "\\/"), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJson() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json" });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"["
							+ "{\"id\":1,\"column_blob\":\"<test_text_base64>\",\"column_date\":\"2003-02-01\",\"column_integer\":1,\"column_real\":1.1237,\"column_text\":\"<test_text>\",\"column_timestamp\":\"2003-02-01T04:05:06\"},"
							+ "{\"id\":2,\"column_blob\":\"<test_text_base64>\",\"column_date\":\"2003-02-01\",\"column_integer\":2,\"column_real\":2.1237,\"column_text\":\"<test_text>\",\"column_timestamp\":\"2003-02-01T04:05:06\"},"
							+ "{\"id\":3,\"column_blob\":null,\"column_date\":null,\"column_integer\":null,\"column_real\":null,\"column_text\":null,\"column_timestamp\":null}"
							+ "]",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\"").replace("/", "\\/"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)).replace("/", "\\/"), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlBeautified() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-beautify" });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
							+ "<table statement=\"SELECT id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp FROM test_tbl ORDER BY id\">\n"
							+ "	<line>\n"
							+ "		<id>1</id>\n"
							+ "		<column_blob><test_text_base64></column_blob>\n"
							+ "		<column_date>2003-02-01</column_date>\n"
							+ "		<column_integer>1</column_integer>\n"
							+ "		<column_real>1,124</column_real>\n"
							+ "		<column_text><test_text></column_text>\n"
							+ "		<column_timestamp>2003-02-01T04:05:06</column_timestamp>\n"
							+ "	</line>\n"
							+ "	<line>\n"
							+ "		<id>2</id>\n"
							+ "		<column_blob><test_text_base64></column_blob>\n"
							+ "		<column_date>2003-02-01</column_date>\n"
							+ "		<column_integer>2</column_integer>\n"
							+ "		<column_real>2,124</column_real>\n"
							+ "		<column_text><test_text></column_text>\n"
							+ "		<column_timestamp>2003-02-01T04:05:06</column_timestamp>\n"
							+ "	</line>\n"
							+ "	<line>\n"
							+ "		<id>3</id>\n"
							+ "		<column_blob></column_blob>\n"
							+ "		<column_date></column_date>\n"
							+ "		<column_integer></column_integer>\n"
							+ "		<column_real></column_real>\n"
							+ "		<column_text></column_text>\n"
							+ "		<column_timestamp></column_timestamp>\n"
							+ "	</line>\n"
							+ "</table>\n",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlWithNullString() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-n", "NULL" });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp FROM test_tbl ORDER BY id\">"
							+ "<line><id>1</id><column_blob><test_text_base64></column_blob><column_date>2003-02-01</column_date><column_integer>1</column_integer><column_real>1,124</column_real><column_text><test_text></column_text><column_timestamp>2003-02-01T04:05:06</column_timestamp></line>"
							+ "<line><id>2</id><column_blob><test_text_base64></column_blob><column_date>2003-02-01</column_date><column_integer>2</column_integer><column_real>2,124</column_real><column_text><test_text></column_text><column_timestamp>2003-02-01T04:05:06</column_timestamp></line>"
							+ "<line><id>3</id><column_blob>NULL</column_blob><column_date>NULL</column_date><column_integer>NULL</column_integer><column_real>NULL</column_real><column_text>NULL</column_text><column_timestamp>NULL</column_timestamp></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXml() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml" });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp FROM test_tbl ORDER BY id\"><line>"
							+ "<id>1</id><column_blob><test_text_base64></column_blob><column_date>2003-02-01</column_date><column_integer>1</column_integer><column_real>1,124</column_real><column_text><test_text></column_text><column_timestamp>2003-02-01T04:05:06</column_timestamp></line>"
							+ "<line><id>2</id><column_blob><test_text_base64></column_blob><column_date>2003-02-01</column_date><column_integer>2</column_integer><column_real>2,124</column_real><column_text><test_text></column_text><column_timestamp>2003-02-01T04:05:06</column_timestamp></line>"
							+ "<line><id>3</id><column_blob></column_blob><column_date></column_date><column_integer></column_integer><column_real></column_real><column_text></column_text><column_timestamp></column_timestamp></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSql() {
		try {
			DbExport._main(new String[] {
					"sqlite",
					SQLITE_DB_FILE,
					"-export", "*",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-x", "sql"
			});

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp FROM test_tbl ORDER BY id\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp) VALUES (1, '<test_text_base64>', '2003-02-01', 1, 1.1237, '<test_text>', '2003-02-01 04:05:06');\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp) VALUES (2, '<test_text_base64>', '2003-02-01', 2, 2.1237, '<test_text>', '2003-02-01 04:05:06');\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_date, column_integer, column_real, column_text, column_timestamp) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlSelect() {
		try {
			DbExport._main(new String[] { "sqlite", SQLITE_DB_FILE, "-export", "SELECT column_text FROM test_tbl WHERE 1 = 1", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.sql", "-x", "sql" });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT column_text FROM test_tbl WHERE 1 = 1\n"
							+ "INSERT INTO export_tbl (column_text) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (column_text) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (column_text) VALUES (NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlWithFormats() {
		try {
			DbExport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-export",
					"*",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-dateFormat", "dd.MM.yyyy",
					"-dateTimeFormat", "dd.MM.yyyy hh:mm:ss",
					"-f", "en",
					"-decimalSeparator", ",",
					"-x", "csv" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;01.02.2003;1;1,1237;\"<test_text>\";01.02.2003 04:05:06\n"
							+ "2;<test_text_base64>;01.02.2003;2;2,1237;\"<test_text>\";01.02.2003 04:05:06\n"
							+ "3;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlWithDefaultFormats() {
		try {
			DbExport._main(new String[] {
					"sqlite", SQLITE_DB_FILE,
					"-export",
					"*",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-decimalSeparator", ",",
					"-x", "csv" });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_date;column_integer;column_real;column_text;column_timestamp\n"
							+ "1;<test_text_base64>;2003-02-01;1;1,1237;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "2;<test_text_base64>;2003-02-01;2;2,1237;\"<test_text>\";2003-02-01T04:05:06\n"
							+ "3;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
