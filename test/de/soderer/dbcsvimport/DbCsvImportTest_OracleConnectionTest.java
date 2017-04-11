package de.soderer.dbcsvimport;

import java.io.File;
import java.sql.Connection;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Assert;
import org.junit.Test;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;

public class DbCsvImportTest_OracleConnectionTest {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_ORACLE_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_ORACLE_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_ORACLE_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_ORACLE_TEST");
	
	public static final Date TEST_DATE = new Date(new Date().getTime() / 1000 * 1000);
	public static final Date TEST_DATE_WITHOUT_TIME = DateUtilities.getDayWithoutTime(new GregorianCalendar()).getTime();
	public static final String[] DATA_TYPES = new String[] { "INTEGER", "NUMBER", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };
	
	public static File INPUTFILE_CSV = new File("~/temp/test_tbl.csv".replace("~", System.getProperty("user.home")));
	
	@Test
	public void testOracleConnections() {
		try {
			for (int i = 1; i <= 10; i++) {
				try (Connection connection = DbUtilities.createConnection(DbVendor.Oracle, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray())) {
					System.out.println("Create connection OK " + i);
				}
			}
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
