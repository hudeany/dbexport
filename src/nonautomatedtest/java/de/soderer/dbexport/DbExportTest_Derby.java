package de.soderer.dbexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.zip.ZipUtilities;

public class DbExportTest_Derby {
	public static final String DERBY_DB_PATH = System.getProperty("user.home") + File.separator + "temp" + File.separator + "test.derby";

	public static LocalDateTime TEST_DATETIME;
	public static LocalDate TEST_DATE;
	public static Object[][] DATA_VALUES;
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "DOUBLE", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File OUTPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File OUTPUTFILE_CSV_ZIPPED = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.zip"));
	public static File OUTPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File OUTPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File OUTPUTFILE_SQL = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.sql"));

	@BeforeAll
	public static void setupTestClass() throws Exception {
		TEST_DATETIME = DateUtilities.parseLocalDateTime(DateUtilities.DD_MM_YYYY_HH_MM_SS, "01.02.2003 04:05:06");
		TEST_DATE = DateUtilities.parseLocalDate(DateUtilities.DD_MM_YYYY, "01.02.2003");
		DATA_VALUES =
				new Object[][]{
			new Object[]{ 1, 1.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATE},
			new Object[]{ 2, 2.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATE},
			new Object[]{ null, null, null, null, null, null, null}
		};

		if (new File(DERBY_DB_PATH).exists()) {
			Utilities.delete(new File(DERBY_DB_PATH));
		}

		try (Connection connection = DbUtilities.createNewDatabase(DbVendor.Derby, DERBY_DB_PATH)) {
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
				statement.execute("CREATE TABLE test_tbl (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " + dataColumnsPart + ", PRIMARY KEY (id))");
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_tbl (" + dataColumnsPartForInsert + ") VALUES (" + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
				for (final Object[] itemData : DATA_VALUES) {
					preparedStatement.clearParameters();
					int paramIndex = 1;
					for (final Object itemValue : itemData) {
						if (itemValue instanceof LocalDate) {
							preparedStatement.setDate(paramIndex++, DateUtilities.getSqlDateForLocalDate((LocalDate) itemValue));
						} else if (itemValue instanceof LocalDateTime) {
							preparedStatement.setTimestamp(paramIndex++, DateUtilities.getSqlTimestampForLocalDateTime((LocalDateTime) itemValue));
						} else{
							preparedStatement.setObject(paramIndex++, itemValue);
						}
					}
					preparedStatement.executeUpdate();
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@BeforeEach
	public void setup() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();
	}

	@AfterEach
	public void tearDown() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();
	}

	@AfterAll
	public static void tearDownTestClass() throws Exception {
		try {
			if (!DbUtilities.deleteDatabase(DbVendor.Derby, DERBY_DB_PATH)) {
				System.out.println("Cannot clean up derby database after tests");
			}
		} catch (final Exception e) {
			System.out.println("Cannot clean up derby database after tests: " + e.getMessage());
			throw e;
		}
	}

	@Test
	public void testCsvSelect() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "SELECT column_varchar FROM test_tbl WHERE id < 3", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv" }));

		assertTrue(OUTPUTFILE_CSV.exists());
		assertEquals(
				"COLUMN_VARCHAR\n"
						+ "\"<test_text>\"\n"
						+ "\"<test_text>\"\n",
						FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testCsv() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "" }));

		assertTrue(OUTPUTFILE_CSV.exists());
		assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
						+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "3;;;;;;;\n",
						FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testCsvWithNullString() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-n", "NULL" }));

		assertTrue(OUTPUTFILE_CSV.exists());
		assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
						+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "3;NULL;NULL;NULL;NULL;NULL;NULL;NULL\n",
						FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testCsvBeautified() throws Exception {
		assertEquals(0, DbExport._main(new String[] {
				"derby", DERBY_DB_PATH,
				"-export", "*",
				"-output", "~" + File.separator + "temp" + File.separator,
		"-beautify" }));

		assertTrue(OUTPUTFILE_CSV.exists());
		assertEquals(
				"ID;COLUMN_BLOB                                                                                                                                                                     ;COLUMN_CLOB                                                                                                          ;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP   ;COLUMN_VARCHAR                                                                                                       \n"
						+ " 1;<test_text_base64>;\"<test_text>\";2003-02-01 ;        1,123;             1;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ " 2;<test_text_base64>;\"<test_text>\";2003-02-01 ;        2,123;             2;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ " 3;                                                                                                                                                                                ;                                                                                                                     ;           ;             ;              ;                   ;                                                                                                                     \n",
						FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8)
						.replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\"\""), "<test_text>")
						.replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testCsvZipped() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-z" }));

		assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
		assertEquals(
				"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
						+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
						+ "3;;;;;;;\n",
						new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testJsonBeautified() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", "-beautify" }));

		assertTrue(OUTPUTFILE_JSON.exists());
		assertEquals(
				"[\n"
						+ "	{\n"
						+ "		\"ID\": 1,\n"
						+ "		\"COLUMN_BLOB\": \"<test_text_base64>\",\n"
						+ "		\"COLUMN_CLOB\": \"<test_text>\",\n"
						+ "		\"COLUMN_DATE\": \"2003-02-01\",\n"
						+ "		\"COLUMN_DOUBLE\": 1.123,\n"
						+ "		\"COLUMN_INTEGER\": 1,\n"
						+ "		\"COLUMN_TIMESTAMP\": \"2003-02-01T04:05:06\",\n"
						+ "		\"COLUMN_VARCHAR\": \"<test_text>\"\n"
						+ "	},\n"
						+ "	{\n"
						+ "		\"ID\": 2,\n"
						+ "		\"COLUMN_BLOB\": \"<test_text_base64>\",\n"
						+ "		\"COLUMN_CLOB\": \"<test_text>\",\n"
						+ "		\"COLUMN_DATE\": \"2003-02-01\",\n"
						+ "		\"COLUMN_DOUBLE\": 2.123,\n"
						+ "		\"COLUMN_INTEGER\": 2,\n"
						+ "		\"COLUMN_TIMESTAMP\": \"2003-02-01T04:05:06\",\n"
						+ "		\"COLUMN_VARCHAR\": \"<test_text>\"\n"
						+ "	},\n"
						+ "	{\n"
						+ "		\"ID\": 3,\n"
						+ "		\"COLUMN_BLOB\": null,\n"
						+ "		\"COLUMN_CLOB\": null,\n"
						+ "		\"COLUMN_DATE\": null,\n"
						+ "		\"COLUMN_DOUBLE\": null,\n"
						+ "		\"COLUMN_INTEGER\": null,\n"
						+ "		\"COLUMN_TIMESTAMP\": null,\n"
						+ "		\"COLUMN_VARCHAR\": null\n"
						+ "	}\n"
						+ "]",
						FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8)
						.replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\""), "<test_text>")
						.replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testJson() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json" }));

		assertTrue(OUTPUTFILE_JSON.exists());
		assertEquals(
				"["
						+ "{\"ID\":1,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01\",\"COLUMN_DOUBLE\":1.123,\"COLUMN_INTEGER\":1,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
						+ "{\"ID\":2,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01\",\"COLUMN_DOUBLE\":2.123,\"COLUMN_INTEGER\":2,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
						+ "{\"ID\":3,\"COLUMN_BLOB\":null,\"COLUMN_CLOB\":null,\"COLUMN_DATE\":null,\"COLUMN_DOUBLE\":null,\"COLUMN_INTEGER\":null,\"COLUMN_TIMESTAMP\":null,\"COLUMN_VARCHAR\":null}"
						+ "]",
						FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8)
						.replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\""), "<test_text>")
						.replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testXmlBeautified() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-beautify" }));

		assertTrue(OUTPUTFILE_XML.exists());
		assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
						+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">\n"
						+ "	<line>\n"
						+ "		<ID>1</ID>\n"
						+ "		<COLUMN_BLOB><test_text_base64></COLUMN_BLOB>\n"
						+ "		<COLUMN_CLOB><test_text></COLUMN_CLOB>\n"
						+ "		<COLUMN_DATE>2003-02-01</COLUMN_DATE>\n"
						+ "		<COLUMN_DOUBLE>1,123</COLUMN_DOUBLE>\n"
						+ "		<COLUMN_INTEGER>1</COLUMN_INTEGER>\n"
						+ "		<COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP>\n"
						+ "		<COLUMN_VARCHAR><test_text></COLUMN_VARCHAR>\n"
						+ "	</line>\n"
						+ "	<line>\n"
						+ "		<ID>2</ID>\n"
						+ "		<COLUMN_BLOB><test_text_base64></COLUMN_BLOB>\n"
						+ "		<COLUMN_CLOB><test_text></COLUMN_CLOB>\n"
						+ "		<COLUMN_DATE>2003-02-01</COLUMN_DATE>\n"
						+ "		<COLUMN_DOUBLE>2,123</COLUMN_DOUBLE>\n"
						+ "		<COLUMN_INTEGER>2</COLUMN_INTEGER>\n"
						+ "		<COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP>\n"
						+ "		<COLUMN_VARCHAR><test_text></COLUMN_VARCHAR>\n"
						+ "	</line>\n"
						+ "	<line>\n"
						+ "		<ID>3</ID>\n"
						+ "		<COLUMN_BLOB></COLUMN_BLOB>\n"
						+ "		<COLUMN_CLOB></COLUMN_CLOB>\n"
						+ "		<COLUMN_DATE></COLUMN_DATE>\n"
						+ "		<COLUMN_DOUBLE></COLUMN_DOUBLE>\n"
						+ "		<COLUMN_INTEGER></COLUMN_INTEGER>\n"
						+ "		<COLUMN_TIMESTAMP></COLUMN_TIMESTAMP>\n"
						+ "		<COLUMN_VARCHAR></COLUMN_VARCHAR>\n"
						+ "	</line>\n"
						+ "</table>\n",
						FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testXmlWithNullString() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-n", "NULL" }));

		assertTrue(OUTPUTFILE_XML.exists());
		assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
						+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
						+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
						+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
						+ "<line><ID>3</ID><COLUMN_BLOB>NULL</COLUMN_BLOB><COLUMN_CLOB>NULL</COLUMN_CLOB><COLUMN_DATE>NULL</COLUMN_DATE><COLUMN_DOUBLE>NULL</COLUMN_DOUBLE><COLUMN_INTEGER>NULL</COLUMN_INTEGER><COLUMN_TIMESTAMP>NULL</COLUMN_TIMESTAMP><COLUMN_VARCHAR>NULL</COLUMN_VARCHAR></line>"
						+ "</table>",
						FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testXml() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml" }));

		assertTrue(OUTPUTFILE_XML.exists());
		assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
						+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
						+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
						+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
						+ "<line><ID>3</ID><COLUMN_BLOB></COLUMN_BLOB><COLUMN_CLOB></COLUMN_CLOB><COLUMN_DATE></COLUMN_DATE><COLUMN_DOUBLE></COLUMN_DOUBLE><COLUMN_INTEGER></COLUMN_INTEGER><COLUMN_TIMESTAMP></COLUMN_TIMESTAMP><COLUMN_VARCHAR></COLUMN_VARCHAR></line>"
						+ "</table>",
						FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testSql() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "*", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "sql" }));

		assertTrue(OUTPUTFILE_SQL.exists());
		assertEquals(
				"--SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\n"
						+ "INSERT INTO test_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (1, '<test_text_base64>', '<test_text>', '2003-02-01', 1.123, 1, '2003-02-01 04:05:06', '<test_text>');\n"
						+ "INSERT INTO test_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (2, '<test_text_base64>', '<test_text>', '2003-02-01', 2.123, 2, '2003-02-01 04:05:06', '<test_text>');\n"
						+ "INSERT INTO test_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);\n",
						FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}

	@Test
	public void testSqlSelect() throws Exception {
		assertEquals(0, DbExport._main(new String[] { "derby", DERBY_DB_PATH, "-export", "SELECT column_varchar FROM test_tbl WHERE 1 = 1", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.sql", "-x", "sql" }));

		assertTrue(OUTPUTFILE_SQL.exists());
		assertEquals(
				"--SELECT column_varchar FROM test_tbl WHERE 1 = 1\n"
						+ "INSERT INTO test_tbl (COLUMN_VARCHAR) VALUES ('<test_text>');\n"
						+ "INSERT INTO test_tbl (COLUMN_VARCHAR) VALUES ('<test_text>');\n"
						+ "INSERT INTO test_tbl (COLUMN_VARCHAR) VALUES (NULL);\n",
						FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
	}
}
