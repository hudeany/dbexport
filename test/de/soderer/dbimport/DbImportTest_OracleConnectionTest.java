package de.soderer.dbimport;

import java.io.File;
import java.sql.Connection;

import org.junit.Assert;
import org.junit.Test;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;

public class DbImportTest_OracleConnectionTest {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_ORACLE_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_ORACLE_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_ORACLE_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_ORACLE_TEST");

	public static final String[] DATA_TYPES = new String[] { "INTEGER", "NUMBER", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File INPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));

	@Test
	public void testOracleConnections() {
		try {
			for (int i = 1; i <= 10; i++) {
				try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Oracle, HOSTNAME, DBNAME, USERNAME, PASSWORD.toCharArray()), false)) {
					System.out.println("Create connection OK " + i);
				}
			}
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
