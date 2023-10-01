package de.soderer.dbexport;

import java.io.File;
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
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.zip.ZipUtilities;

public class DbExportTest_PostgreSQL {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_POSTGRESQL_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_POSTGRESQL_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_POSTGRESQL_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_POSTGRESQL_TEST");

	public static LocalDateTime TEST_DATETIME;
	public static LocalDate TEST_DATE;
	public static Object[][] DATA_VALUES;
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "REAL", "VARCHAR(1024)", "BYTEA", "TEXT", "TIMESTAMP", "DATE" };

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

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.PostgreSQL, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray()), false)) {
			String dataColumnsPart = "";
			String dataColumnsPartForInsert = "";
			for (final String dataType : DATA_TYPES) {
				String columnName = "column_" + dataType.toLowerCase();
				if (columnName.contains("(")) {
					columnName = columnName.substring(0, columnName.indexOf("("));
				}
				columnName = columnName.replace("real", "double");
				columnName = columnName.replace("bytea", "blob");
				columnName = columnName.replace("text", "clob");

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

				statement.execute("CREATE TABLE test_tbl (id SERIAL PRIMARY KEY, " + dataColumnsPart + ")");
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_tbl (" + dataColumnsPartForInsert + ") VALUES (" + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
				for (final Object[] itemData : DATA_VALUES) {
					preparedStatement.clearParameters();
					int paramIndex = 1;
					for (final Object itemValue : itemData) {
						if (itemValue instanceof Date) {
							preparedStatement.setTimestamp(paramIndex++, new Timestamp(((Date) itemValue).getTime()));
						} else {
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

	@Before
	public void setup() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();
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
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.PostgreSQL, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray()), false);
				Statement statement = connection.createStatement()) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void testCsvSelect() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "SELECT column_varchar FROM test_tbl WHERE id < 3", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"column_varchar\n\"<test_text>\"\n\"<test_text>\"\n",
					FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsv() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithNullString() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-n", "NULL", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;NULL;NULL;NULL;NULL;NULL;NULL;NULL\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvBeautified() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"id;column_blob                                                                                                                                                                     ;column_clob                                                                                                         ;column_date        ;column_double;column_integer;column_timestamp   ;column_varchar                                                                                                      \n"
							+ " 1;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;        1,123;             1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ " 2;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;        2,123;             2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ " 3;                                                                                                                                                                                ;                                                                                                                    ;                   ;             ;              ;                   ;                                                                                                                    \n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvZipped() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-z", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			Assert.assertEquals(
					"id;column_blob;column_clob;column_date;column_double;column_integer;column_timestamp;column_varchar\n"
							+ "1;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;1,123;1;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";01.02.2003 00:00:00;2,123;2;01.02.2003 04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonBeautified() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"[\n"
							+ "	{\n"
							+ "		\"id\": 1,\n"
							+ "		\"column_blob\": \"<test_text_base64>\",\n"
							+ "		\"column_clob\": \"<test_text>\",\n"
							+ "		\"column_date\": \"2003-02-01T00:00:00+01\",\n"
							+ "		\"column_double\": 1.123,\n"
							+ "		\"column_integer\": 1,\n"
							+ "		\"column_timestamp\": \"2003-02-01T04:05:06+01\",\n"
							+ "		\"column_varchar\": \"<test_text>\"\n"
							+ "	},\n"
							+ "	{\n"
							+ "		\"id\": 2,\n"
							+ "		\"column_blob\": \"<test_text_base64>\",\n"
							+ "		\"column_clob\": \"<test_text>\",\n"
							+ "		\"column_date\": \"2003-02-01T00:00:00+01\",\n"
							+ "		\"column_double\": 2.123,\n"
							+ "		\"column_integer\": 2,\n"
							+ "		\"column_timestamp\": \"2003-02-01T04:05:06+01\",\n"
							+ "		\"column_varchar\": \"<test_text>\"\n"
							+ "	},\n"
							+ "	{\n"
							+ "		\"id\": 3,\n"
							+ "		\"column_blob\": null,\n"
							+ "		\"column_clob\": null,\n"
							+ "		\"column_date\": null,\n"
							+ "		\"column_double\": null,\n"
							+ "		\"column_integer\": null,\n"
							+ "		\"column_timestamp\": null,\n"
							+ "		\"column_varchar\": null\n"
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
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"["
							+ "{\"id\":1,\"column_blob\":\"<test_text_base64>\",\"column_clob\":\"<test_text>\",\"column_date\":\"2003-02-01T00:00:00+01\",\"column_double\":1.123,\"column_integer\":1,\"column_timestamp\":\"2003-02-01T04:05:06+01\",\"column_varchar\":\"<test_text>\"},"
							+ "{\"id\":2,\"column_blob\":\"<test_text_base64>\",\"column_clob\":\"<test_text>\",\"column_date\":\"2003-02-01T00:00:00+01\",\"column_double\":2.123,\"column_integer\":2,\"column_timestamp\":\"2003-02-01T04:05:06+01\",\"column_varchar\":\"<test_text>\"},"
							+ "{\"id\":3,\"column_blob\":null,\"column_clob\":null,\"column_date\":null,\"column_double\":null,\"column_integer\":null,\"column_timestamp\":null,\"column_varchar\":null}"
							+ "]",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\\\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlBeautified() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-beautify", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">\n"
							+ "	<line>\n"
							+ "		<id>1</id>\n"
							+ "		<column_blob><test_text_base64></column_blob>\n"
							+ "		<column_clob><test_text></column_clob>\n"
							+ "		<column_date>01.02.2003 00:00:00</column_date>\n"
							+ "		<column_double>1,123</column_double>\n"
							+ "		<column_integer>1</column_integer>\n"
							+ "		<column_timestamp>01.02.2003 04:05:06</column_timestamp>\n"
							+ "		<column_varchar><test_text></column_varchar>\n"
							+ "	</line>\n"
							+ "	<line>\n"
							+ "		<id>2</id>\n"
							+ "		<column_blob><test_text_base64></column_blob>\n"
							+ "		<column_clob><test_text></column_clob>\n"
							+ "		<column_date>01.02.2003 00:00:00</column_date>\n"
							+ "		<column_double>2,123</column_double>\n"
							+ "		<column_integer>2</column_integer>\n"
							+ "		<column_timestamp>01.02.2003 04:05:06</column_timestamp>\n"
							+ "		<column_varchar><test_text></column_varchar>\n"
							+ "	</line>\n"
							+ "	<line>\n"
							+ "		<id>3</id>\n"
							+ "		<column_blob></column_blob>\n"
							+ "		<column_clob></column_clob>\n"
							+ "		<column_date></column_date>\n"
							+ "		<column_double></column_double>\n"
							+ "		<column_integer></column_integer>\n"
							+ "		<column_timestamp></column_timestamp>\n"
							+ "		<column_varchar></column_varchar>\n"
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
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-n", "NULL", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
							+ "<line><id>1</id><column_blob><test_text_base64></column_blob><column_clob><test_text></column_clob><column_date>01.02.2003 00:00:00</column_date><column_double>1,123</column_double><column_integer>1</column_integer><column_timestamp>01.02.2003 04:05:06</column_timestamp><column_varchar><test_text></column_varchar></line>"
							+ "<line><id>2</id><column_blob><test_text_base64></column_blob><column_clob><test_text></column_clob><column_date>01.02.2003 00:00:00</column_date><column_double>2,123</column_double><column_integer>2</column_integer><column_timestamp>01.02.2003 04:05:06</column_timestamp><column_varchar><test_text></column_varchar></line>"
							+ "<line><id>3</id><column_blob>NULL</column_blob><column_clob>NULL</column_clob><column_date>NULL</column_date><column_double>NULL</column_double><column_integer>NULL</column_integer><column_timestamp>NULL</column_timestamp><column_varchar>NULL</column_varchar></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXml() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
							+ "<line><id>1</id><column_blob><test_text_base64></column_blob><column_clob><test_text></column_clob><column_date>01.02.2003 00:00:00</column_date><column_double>1,123</column_double><column_integer>1</column_integer><column_timestamp>01.02.2003 04:05:06</column_timestamp><column_varchar><test_text></column_varchar></line>"
							+ "<line><id>2</id><column_blob><test_text_base64></column_blob><column_clob><test_text></column_clob><column_date>01.02.2003 00:00:00</column_date><column_double>2,123</column_double><column_integer>2</column_integer><column_timestamp>01.02.2003 04:05:06</column_timestamp><column_varchar><test_text></column_varchar></line>"
							+ "<line><id>3</id><column_blob></column_blob><column_clob></column_clob><column_date></column_date><column_double></column_double><column_integer></column_integer><column_timestamp></column_timestamp><column_varchar></column_varchar></line>"
							+ "</table>",
							FileUtilities.readFileToString(OUTPUTFILE_XML, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSql() {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "sql", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar) VALUES (1, '<test_text_base64>', '<test_text>', '2003-02-01 00:00:00', 1.123, 1, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar) VALUES (2, '<test_text_base64>', '<test_text>', '2003-02-01 00:00:00', 2.123, 2, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlSelect() throws Exception {
		try {
			DbExport._main(new String[] { "postgresql", HOSTNAME, DBNAME, USERNAME, "-export", "SELECT column_varchar FROM test_tbl WHERE 1 = 1", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.sql", "-x", "sql", PASSWORD });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT column_varchar FROM test_tbl WHERE 1 = 1\n"
							+ "INSERT INTO export_tbl (column_varchar) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (column_varchar) VALUES ('<test_text>');\n"
							+ "INSERT INTO export_tbl (column_varchar) VALUES (NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
			throw e;
		}
	}
}
