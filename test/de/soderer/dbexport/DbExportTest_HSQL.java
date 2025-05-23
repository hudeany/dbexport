package de.soderer.dbexport;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.InputStreamWithOtherItemsToClose;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.zip.TarGzUtilities;
import de.soderer.utilities.zip.ZipUtilities;
import net.lingala.zip4j.ZipFile;

public class DbExportTest_HSQL {
	public static final String HSQL_DB_FILE = System.getProperty("user.home") + File.separator + "temp" + File.separator + "test.hsql";

	public static LocalDateTime TEST_DATETIME;
	public static LocalDate TEST_DATE;
	public static Object[][] DATA_VALUES;
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "DOUBLE", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File OUTPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File OUTPUTFILE_CSV_ZIPPED = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.zip"));
	public static File OUTPUTFILE_CSV_TARGZ = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.tar.gz"));
	public static File OUTPUTFILE_CSV_TGZ = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.tgz"));
	public static File OUTPUTFILE_CSV_GZ = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv.gz"));
	public static File OUTPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File OUTPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File OUTPUTFILE_SQL = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.sql"));

	@BeforeClass
	public static void setupTestClass() throws Exception {
		try {
			DbUtilities.deleteDatabase(DbVendor.HSQL, HSQL_DB_FILE);
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Do nothing
		}

		try {
			final File folder = new File(Utilities.replaceUsersHome("~" + File.separator + "temp"));
			final File[] files = folder.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(final File dir, final String name) {
					return name.matches("dbstructure_.*\\.json\\.zip");
				}
			});
			for (final File file : files) {
				file.delete();
			}
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}

		TEST_DATETIME = DateUtilities.parseLocalDateTime(DateUtilities.DD_MM_YYYY_HH_MM_SS, "01.02.2003 04:05:06");
		TEST_DATE = DateUtilities.parseLocalDate(DateUtilities.DD_MM_YYYY, "01.02.2003");
		DATA_VALUES =
				new Object[][]{
			new Object[]{ 1, 1.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATETIME},
			new Object[]{ 2, 2.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATETIME},
			new Object[]{ null, null, null, null, null, null, null}
		};

		try {
			DbUtilities.deleteDatabase(DbVendor.HSQL, HSQL_DB_FILE);
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Do nothing
		}

		try (Connection connection = DbUtilities.createNewDatabase(DbVendor.HSQL, HSQL_DB_FILE)) {
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
				statement.execute("CREATE TABLE test_tbl (id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1) PRIMARY KEY, " + dataColumnsPart + ")");
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_tbl (" + dataColumnsPartForInsert + ") VALUES (" + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
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
		OUTPUTFILE_CSV_TARGZ.delete();
		OUTPUTFILE_CSV_TGZ.delete();
		OUTPUTFILE_CSV_GZ.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();
	}

	@After
	public void tearDown() {
		OUTPUTFILE_CSV.delete();
		OUTPUTFILE_CSV_ZIPPED.delete();
		OUTPUTFILE_CSV_TARGZ.delete();
		OUTPUTFILE_CSV_TGZ.delete();
		OUTPUTFILE_CSV_GZ.delete();
		OUTPUTFILE_XML.delete();
		OUTPUTFILE_JSON.delete();
		OUTPUTFILE_SQL.delete();

		try {
			final File folder = new File(Utilities.replaceUsersHome("~" + File.separator + "temp"));
			final File[] files = folder.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(final File dir, final String name) {
					return name.matches("dbstructure_.*\\.json\\.zip");
				}
			});
			for (final File file : files) {
				file.delete();
			}
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void tearDownTestClass() throws Exception {
		try {
			DbUtilities.deleteDatabase(DbVendor.HSQL, HSQL_DB_FILE);
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Do nothing
		}
	}

	@Test
	public void testCsvSelect() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "SELECT COLUMN_VARCHAR FROM test_tbl WHERE id < 3", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", null });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"COLUMN_VARCHAR\n"
							+ "\"<test_text>\"\n"
							+ "\"<test_text>\"\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvSelectErroneousSql() {
		try {
			Assert.assertEquals(1, DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "SELECT COLUMN_VARCHAR FROM test_tbl WHERE id < 3 ORDER BY not_existing_column", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.csv", null }));

			Assert.assertFalse(OUTPUTFILE_CSV.exists());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsv() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", null });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvWithNullString() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-n", "NULL", null });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;NULL;NULL;NULL;NULL;NULL;NULL;NULL\n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvBeautified() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-beautify", null });

			Assert.assertTrue(OUTPUTFILE_CSV.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB                                                                                                                                                                     ;COLUMN_CLOB                                                                                                         ;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP   ;COLUMN_VARCHAR                                                                                                      \n"
							+ " 1;<test_text_base64>;\"<test_text>\";2003-02-01 ;        1,123;             1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ " 2;<test_text_base64>;\"<test_text>\";2003-02-01 ;        2,123;             2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ " 3;                                                                                                                                                                                ;                                                                                                                    ;           ;             ;              ;                   ;                                                                                                                    \n",
							FileUtilities.readFileToString(OUTPUTFILE_CSV, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonBeautified() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", "-beautify", null });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
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
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\"").replace("/", "\\/"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)).replace("/", "\\/"), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJson() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "json", null });

			Assert.assertTrue(OUTPUTFILE_JSON.exists());
			Assert.assertEquals(
					"["
							+ "{\"ID\":1,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01\",\"COLUMN_DOUBLE\":1.123,\"COLUMN_INTEGER\":1,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
							+ "{\"ID\":2,\"COLUMN_BLOB\":\"<test_text_base64>\",\"COLUMN_CLOB\":\"<test_text>\",\"COLUMN_DATE\":\"2003-02-01\",\"COLUMN_DOUBLE\":2.123,\"COLUMN_INTEGER\":2,\"COLUMN_TIMESTAMP\":\"2003-02-01T04:05:06\",\"COLUMN_VARCHAR\":\"<test_text>\"},"
							+ "{\"ID\":3,\"COLUMN_BLOB\":null,\"COLUMN_CLOB\":null,\"COLUMN_DATE\":null,\"COLUMN_DOUBLE\":null,\"COLUMN_INTEGER\":null,\"COLUMN_TIMESTAMP\":null,\"COLUMN_VARCHAR\":null}"
							+ "]",
							FileUtilities.readFileToString(OUTPUTFILE_JSON, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\\", "\\\\").replace("\"", "\\\"").replace("/", "\\/"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)).replace("/", "\\/"), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlBeautified() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-beautify", null });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
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
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testXmlWithNullString() {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", "-n", "NULL", null });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
							+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
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
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "xml", null });

			Assert.assertTrue(OUTPUTFILE_XML.exists());
			Assert.assertEquals(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
							+ "<table statement=\"SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\">"
							+ "<line><ID>1</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>1,123</COLUMN_DOUBLE><COLUMN_INTEGER>1</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
							+ "<line><ID>2</ID><COLUMN_BLOB><test_text_base64></COLUMN_BLOB><COLUMN_CLOB><test_text></COLUMN_CLOB><COLUMN_DATE>2003-02-01</COLUMN_DATE><COLUMN_DOUBLE>2,123</COLUMN_DOUBLE><COLUMN_INTEGER>2</COLUMN_INTEGER><COLUMN_TIMESTAMP>2003-02-01T04:05:06</COLUMN_TIMESTAMP><COLUMN_VARCHAR><test_text></COLUMN_VARCHAR></line>"
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
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "sql", null });

			Assert.assertTrue(OUTPUTFILE_SQL.exists());
			Assert.assertEquals(
					"--SELECT id, column_blob, column_clob, column_date, column_double, column_integer, column_timestamp, column_varchar FROM test_tbl ORDER BY id\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (1, '<test_text_base64>', '<test_text>', '2003-02-01', 1.123, 1, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (2, '<test_text_base64>', '<test_text>', '2003-02-01', 2.123, 2, '2003-02-01 04:05:06', '<test_text>');\n"
							+ "INSERT INTO export_tbl (ID, COLUMN_BLOB, COLUMN_CLOB, COLUMN_DATE, COLUMN_DOUBLE, COLUMN_INTEGER, COLUMN_TIMESTAMP, COLUMN_VARCHAR) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);\n",
							FileUtilities.readFileToString(OUTPUTFILE_SQL, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("'", "''"), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSqlSelect() throws Exception {
		try {
			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "SELECT column_varchar FROM test_tbl WHERE 1 = 1", "-output", "~" + File.separator + "temp" + File.separator + "test_tbl.sql", "-x", "sql", null });

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

	@Test
	public void testCsvZipped() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-z",
					null
			});

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvCompressZipped() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-compress", "ZIP",
					null
			});

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(ZipUtilities.readExistingZipFile(OUTPUTFILE_CSV_ZIPPED).get("test_tbl.csv"), StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvCompressTarGz() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-compress", "TARGZ",
					null
			});

			Assert.assertTrue(OUTPUTFILE_CSV_TARGZ.exists());
			try (InputStream inputStream = TarGzUtilities.openCompressedFile(OUTPUTFILE_CSV_TARGZ, "test_tbl.csv")) {
				final byte[] testOutoputData = IoUtilities.toByteArray(inputStream);
				Assert.assertEquals(
						"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
								+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "3;;;;;;;\n",
								new String(testOutoputData, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
			}
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvCompressTgz() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-compress", "TGZ",
					null
			});

			Assert.assertTrue(OUTPUTFILE_CSV_TGZ.exists());
			try (InputStream inputStream = TarGzUtilities.openCompressedFile(OUTPUTFILE_CSV_TGZ, "test_tbl.csv")) {
				final byte[] testOutoputData = IoUtilities.toByteArray(inputStream);
				Assert.assertEquals(
						"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
								+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "3;;;;;;;\n",
								new String(testOutoputData, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
			}
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvCompressGz() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-compress", "GZ",
					null
			});

			Assert.assertTrue(OUTPUTFILE_CSV_GZ.exists());
			try (InputStream inputStream = new GZIPInputStream(new FileInputStream(OUTPUTFILE_CSV_GZ))) {
				final byte[] testOutoputData = IoUtilities.toByteArray(inputStream);
				Assert.assertEquals(
						"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
								+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
								+ "3;;;;;;;\n",
								new String(testOutoputData, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
			}
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvZippedWithPasswordWithAES256() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-z", null,
					"-zippassword", "abc123"
			});

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			final ZipFile zipFile = new ZipFile(OUTPUTFILE_CSV_ZIPPED, "abc123".toCharArray());
			final byte[] data;
			try (InputStream inputStream = new InputStreamWithOtherItemsToClose(zipFile.getInputStream(zipFile.getFileHeaders().get(0)), zipFile)) {
				data = IoUtilities.toByteArray(inputStream);
			}

			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(data, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvZippedWithPasswordWithZipCrypto() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-z", null,
					"-zippassword", "abc123",
					"-useZipCrypto"
			});

			Assert.assertTrue(OUTPUTFILE_CSV_ZIPPED.exists());
			final ZipFile zipFile = new ZipFile(OUTPUTFILE_CSV_ZIPPED, "abc123".toCharArray());
			final byte[] data;
			try (InputStream inputStream = new InputStreamWithOtherItemsToClose(zipFile.getInputStream(zipFile.getFileHeaders().get(0)), zipFile)) {
				data = IoUtilities.toByteArray(inputStream);
			}

			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_INTEGER;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;<test_text_base64>;\"<test_text>\";2003-02-01;1,123;1;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "2;<test_text_base64>;\"<test_text>\";2003-02-01;2,123;2;2003-02-01T04:05:06;\"<test_text>\"\n"
							+ "3;;;;;;;\n",
							new String(data, StandardCharsets.UTF_8).replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>").replace(Utilities.encodeBase64(TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8)), "<test_text_base64>"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testBigTable() {
		final File BIG_OUTPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_big_tbl.csv"));

		try {
			try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.HSQL, "", HSQL_DB_FILE, null, null, false, null, null), false)) {
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
					statement.execute("CREATE TABLE test_big_tbl (id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1) PRIMARY KEY, " + dataColumnsPart + ")");
				}

				try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_big_tbl (" + dataColumnsPartForInsert + ") VALUES (" + Utilities.repeat("?", DATA_TYPES.length, ", ") + ")")) {
					final Object[] itemData = new Object[]{ 1, 1.123, TextUtilities.GERMAN_TEST_STRING, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8), TextUtilities.GERMAN_TEST_STRING, TEST_DATETIME, TEST_DATETIME};
					for (int i = 0; i < 10000; i++) {
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

			DbExport._main(new String[] { "hsql", "", HSQL_DB_FILE, "", "-export", "test_big_tbl", "-output", "~" + File.separator + "temp" + File.separator + "", "-x", "csv", null });

			Assert.assertTrue(BIG_OUTPUTFILE_CSV.exists());
			Assert.assertEquals(10001, FileUtilities.getLineCount(BIG_OUTPUTFILE_CSV));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			if (BIG_OUTPUTFILE_CSV.exists()) {
				BIG_OUTPUTFILE_CSV.delete();
			}
		}
	}

	@Test
	public void testStructureExport() {
		try {
			DbExport._main(new String[] {
					"hsql",
					"",
					HSQL_DB_FILE,
					"",
					"-export", "test_tbl",
					"-output", "~" + File.separator + "temp" + File.separator + "",
					"-z",
					"-structure"
			});

			boolean foundFile = false;
			final File folder = new File(Utilities.replaceUsersHome("~" + File.separator + "temp"));
			final File[] files = folder.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(final File dir, final String name) {
					return name.matches("dbstructure_.*\\.json\\.zip");
				}
			});

			foundFile = files != null && files.length > 0;

			Assert.assertTrue(foundFile);
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
