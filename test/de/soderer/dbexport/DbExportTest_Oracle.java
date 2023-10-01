package de.soderer.dbexport;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

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
import de.soderer.utilities.WildcardFilenameFilter;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.zip.ZipUtilities;

public class DbExportTest_Oracle {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_ORACLE_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_ORACLE_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_ORACLE_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_ORACLE_TEST");

	public static LocalDateTime TEST_DATETIME;
	public static LocalDate TEST_DATE;
	public static Object[][] DATA_VALUES;
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "NUMBER", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File OUTPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File OUTPUTFILE_CSV_ZIPPED = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.zip"));
	public static File OUTPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File OUTPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File OUTPUTFILE_SQL = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.sql"));

	@BeforeClass
	public static void setupTestClass() throws Exception {
		TEST_DATETIME = DateUtilities.parseLocalDateTime(DateUtilities.DD_MM_YYYY_HH_MM_SS, "01.02.2003 04:05:06");
		TEST_DATE = DateUtilities.parseLocalDate(DateUtilities.DD_MM_YYYY, "01.02.2003");
		DATA_VALUES =
				new Object[][]{
			new Object[]{ 1, 1.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATETIME},
			new Object[]{ 2, 2.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATETIME},
			new Object[]{ null, null, null, null, null, null, null}
		};

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Oracle, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray()), false)) {
			String dataColumnsPart = "";
			String dataColumnsPartForInsert = "";
			for (final String dataType : DATA_TYPES) {
				String columnName = "column_" + dataType.toLowerCase();
				if (columnName.contains("(")) {
					columnName = columnName.substring(0, columnName.indexOf("("));
				}
				columnName = columnName.replace("number", "double");

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
				if (DbUtilities.checkTableExist(connection, "test_tbl")) {
					statement.execute("DROP TABLE test_tbl");
				}
				statement.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, " + dataColumnsPart + ")");
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_tbl (id, " + dataColumnsPartForInsert + ") VALUES (NVL((SELECT MAX(id) + 1 FROM test_tbl), 1), " + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
				for (final Object[] itemData : DATA_VALUES) {
					preparedStatement.clearParameters();
					int paramIndex = 1;
					ByteArrayInputStream inputStream = null;
					try {
						for (final Object itemValue : itemData) {
							if (itemValue != null && itemValue instanceof byte[]) {
								final byte[] arrayValue = (byte[]) itemValue;
								inputStream = new ByteArrayInputStream(arrayValue);
								// cast to int is necessary because with JDBC 4 there is also a version of this method with a (int, long), but that is not implemented by Oracle
								preparedStatement.setBinaryStream(paramIndex++, inputStream, arrayValue.length);
							} else if (itemValue != null && itemValue instanceof Date) {
								preparedStatement.setTimestamp(paramIndex++, new Timestamp(((Date) itemValue).getTime()));
							} else {
								preparedStatement.setObject(paramIndex++, itemValue);
							}
						}
						preparedStatement.executeUpdate();
					} finally {
						Utilities.closeQuietly(inputStream);
					}
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
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();

		deleteLogFiles();
	}

	@After
	public void tearDown() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();
	}

	@AfterClass
	public static void tearDownTestClass() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Oracle, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray()), false);
				Statement statement = connection.createStatement()) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void deleteLogFiles() {
		final File dir = new File(System.getProperty("user.home") + File.separator + "temp");
		final FileFilter fileFilter = new WildcardFilenameFilter("test_tbl*.log");
		final File[] files = dir.listFiles(fileFilter);
		for (final File logFile : files) {
			logFile.delete();
		}
	}

	private String getLogFileData() throws Exception {
		final File dir = new File(System.getProperty("user.home") + File.separator + "temp");
		final FileFilter fileFilter = new WildcardFilenameFilter("test_tbl*.log");
		final File[] files = dir.listFiles(fileFilter);
		return FileUtilities.readFileToString(files[0], StandardCharsets.UTF_8);
	}

	private void assertLogContains(final String logData, final String valueName, final String value) {
		boolean valueFound = false;
		for (final String line : logData.split("\r\n|\r|\n")) {
			if (line.startsWith(valueName + ":") && line.endsWith(" " + value)) {
				valueFound = true;
			}
		}
		Assert.assertTrue(logData, valueFound);
	}

	@Test
	public void testCsvSelect() {
		try {
			DbExport._main(new String[] {
					"oracle",
					HOSTNAME,
					USERNAME,
					DBNAME,
					"-l",
					"-export", "SELECT COLUMN_VARCHAR FROM test_tbl WHERE id < 3",
					"-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"COLUMN_VARCHAR\n"
							+ "\"<test_text>\"\n"
							+ "\"<test_text>\"\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));

			final String logData = getLogFileData();
			assertLogContains(logData, "Lines to export", "2");
			assertLogContains(logData, "Exported lines", "2");
			assertLogContains(logData, "Exported data amount", "287 B");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsv() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithNullString() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-n", "NULL", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;NULL;NULL;NULL;NULL;NULL;NULL;NULL\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvBeautified() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB                                                                                                                                                                     ;COLUMN_CLOB                                                                                                         ;COLUMN_DATE        ;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP   ;COLUMN_VARCHAR                                                                                                      \n"
							+ " 1;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;        1,123;             1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ " 2;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;        2,123;             2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ " 3;                                                                                                                                                                                ;                                                                                                                    ;                   ;             ;              ;                   ;                                                                                                                    \n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvZipped() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-z", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 04:05:06;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonBeautified() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"[\n"
							+ "	{\n"
							+ "		\"ID\": 1,\n"
							+ "		\"COLUMN_BLOB\": \"<test_text_base64>\",\n"
							+ "		\"COLUMN_CLOB\": \"<test_text>\",\n"
							+ "		\"COLUMN_DATE\": \"2003-02-01T04:05:06+01\",\n"
							+ "		\"COLUMN_DOUBLE\": 1.123,\n"
							+ "		\"COLUMN_INTEGER\": 1,\n"
							+ "		\"COLUMN_TIMESTAMP\": \"2003-02-01T04:05:06+01\",\n"
							+ "		\"COLUMN_VARCHAR\": \"<test_text>\"\n"
							+ "	},\n"
							+ "	{\n"
							+ "		\"ID\": 2,\n"
							+ "		\"COLUMN_BLOB\": \"<test_text_base64>\",\n"
							+ "		\"COLUMN_CLOB\": \"<test_text>\",\n"
							+ "		\"COLUMN_DATE\": \"2003-02-01T04:05:06+01\",\n"
							+ "		\"COLUMN_DOUBLE\": 2.123,\n"
							+ "		\"COLUMN_INTEGER\": 2,\n"
							+ "		\"COLUMN_TIMESTAMP\": \"2003-02-01T04:05:06+01\",\n"
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
							+ "]\n",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\\\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJson() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"["
							+ "{\"ID\":1,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01T04:05:06+01\",\"COLUMN_DOUBLE\":1.123,\"COLUMN_INTEGER\":1,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06+01\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
							+ "{\"ID\":2,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01T04:05:06+01\",\"COLUMN_DOUBLE\":2.123,\"COLUMN_INTEGER\":2,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06+01\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
							+ "{\"ID\":3,\"COLUMN_BLOB\":null,\"COLUMN_CLOB\":null,\"COLUMN_DATE\":null,\"COLUMN_DOUBLE\":null,\"COLUMN_INTEGER\":null,\"COLUMN_TIMESTAMP\":null,\"COLUMN_VARCHAR\":null}"
							+ "]",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\\\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlBeautified() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM TEST_TBL ORDER BY id\">\n"
							+ "	<line>\n"
							+ "		<ID>1</ID>\n"
							+ "		<COLUMN_BLOB><test_text_base64></COLUMN_BLOB>\n"
							+ "		<COLUMN_CLOB><test_text></COLUMN_CLOB>\n"
							+ "		<COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE>\n"
							+ "		<COLUMN_DOUBLE>1,123</COLUMN_DOUBLE>\n"
							+ "		<COLUMN_INTEGER>1</COLUMN_INTEGER>\n"
							+ "		<COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP>\n"
							+ "		<COLUMN_VARCHAR><test_text></COLUMN_VARCHAR>\n"
							+ "	</line>\n"
							+ "	<line>\n"
							+ "		<ID>2</ID>\n"
							+ "		<COLUMN_BLOB><test_text_base64></COLUMN_BLOB>\n"
							+ "		<COLUMN_CLOB><test_text></COLUMN_CLOB>\n"
							+ "		<COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE>\n"
							+ "		<COLUMN_DOUBLE>2,123</COLUMN_DOUBLE>\n"
							+ "		<COLUMN_INTEGER>2</COLUMN_INTEGER>\n"
							+ "		<COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP>\n"
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
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlWithNullString() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-n", "NULL", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM TEST_TBL ORDER BY id\">"
							+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>3</ID><COLUMN_BLOB>NULL</COLUMN_BLOB><COLUMN_CLOB>NULL</COLUMN_CLOB><COLUMN_DATE>NULL</COLUMN_DATE><COLUMN_DOUBLE>NULL</COLUMN_DOUBLE><COLUMN_INTEGER>NULL</COLUMN_INTEGER><COLUMN_TIMESTAMP>NULL</COLUMN_TIMESTAMP><COLUMN_VARCHAR>NULL</COLUMN_VARCHAR></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXml() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM TEST_TBL ORDER BY id\">"
							+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>01.02.2003 04:05:06</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>01.02.2003 04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>3</ID><COLUMN_BLOB></COLUMN_BLOB><COLUMN_CLOB></COLUMN_CLOB><COLUMN_DATE></COLUMN_DATE><COLUMN_DOUBLE></COLUMN_DOUBLE><COLUMN_INTEGER></COLUMN_INTEGER><COLUMN_TIMESTAMP></COLUMN_TIMESTAMP><COLUMN_VARCHAR></COLUMN_VARCHAR></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSql() {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "sql", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM TEST_TBL ORDER BY id\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (1, '<test_text_base64>', '<test_text>', '2003-02-01 04:05:06', 1.123, 1, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (2, '<test_text_base64>', '<test_text>', '2003-02-01 04:05:06', 2.123, 2, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlSelect() throws Exception {
		try {
			DbExport._main(new String[] { "oracle", HOSTNAME, DBNAME, USERNAME, "-export", "SELECT column_varchar FROM test_tbl WHERE 1 = 1", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.sql", "-x", "sql", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT column_varchar FROM test_tbl WHERE 1 = 1\n"
							+ "INSERT INTO export_tbl (COLUMN_VARCHAR) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (COLUMN_VARCHAR) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (COLUMN_VARCHAR) VALUES (NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
			throw e;
		}
	}
}
