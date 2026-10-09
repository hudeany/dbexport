package de.soderer.dbexport;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbDefinitionException;

/**
 * Parameters of a database connection test ("connectiontest"): connects to the database repeatedly
 * and optionally executes a check statement.
 */
public class ConnectionTestDefinition extends DbConnectionDefinition {
	/** The statement to use for check. */
	private String checkStatement;

	private int iterations = 1;
	private int sleepTime = 1;

	/**
	 * Creates a definition with the default values (1 iteration, 1 second sleep time, vendor default check statement).
	 */
	public ConnectionTestDefinition() {
		// Nothing to initialize
	}

	/**
	 * Returns the SQL statement to execute on each check.
	 *
	 * @return the SQL statement, the keyword "vendor" for the vendor's default check statement, or null for none
	 */
	public String getCheckStatement() {
		return checkStatement;
	}

	/**
	 * Sets the SQL statement to execute on each check.
	 *
	 * @param checkStatement the SQL statement, the keyword "vendor" for the vendor's default check statement, or null for none
	 */
	public void setCheckStatement(final String checkStatement) {
		this.checkStatement = checkStatement;
	}

	/**
	 * Returns the number of connection checks.
	 *
	 * @return the number of connection checks, 0 means unlimited
	 */
	public int getIterations() {
		return iterations;
	}

	/**
	 * Sets the number of connection checks.
	 *
	 * @param iterations the number of connection checks, 0 means unlimited
	 */
	public void setIterations(final int iterations) {
		this.iterations = iterations;
	}

	/**
	 * Returns the time to wait after each check.
	 *
	 * @return the time to wait after each check in seconds
	 */
	public int getSleepTime() {
		return sleepTime;
	}

	/**
	 * Sets the time to wait after each check.
	 *
	 * @param sleepTime the time to wait after each check in seconds
	 */
	public void setSleepTime(final int sleepTime) {
		this.sleepTime = sleepTime;
	}

	/**
	 * Checks the parameters and supplies the database driver (it may be asked for interactively, if it is missing).
	 *
	 * @throws Exception if the database driver is not available, or a parameter is missing or invalid
	 */
	@Override
	public void checkParameters() throws Exception {
		if (getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, getDbVendor()).supplyDriver(DbExport.APPLICATION_NAME, DbExport.CONFIGURATION_FILE)) {
					throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor(), e);
			}
		}

		super.checkParameters();

		if (iterations < 0) {
			throw new DbExportException("Invalid connectiontest iterations");
		}

		if (sleepTime < 0) {
			throw new DbExportException("Invalid connectiontest sleep time");
		}
	}

	/**
	 * Returns the command line parameters for this connection test, e.g. to show them in the console menu.
	 *
	 * @return the command line parameters, starting with "connectiontest"
	 */
	public String toParamsString() {
		String params = "connectiontest";
		params += " " + getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostnameAndPort();
		}
		params += " " + getDbName();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
			if (getUsername() != null) {
				params += " " + getUsername();
			}
		}
		if (getPassword() != null) {
			params += " '" + new String(getPassword()).replace("'", "\\'") + "'";
		}

		if (getIterations() != 1) {
			params += " " + "-iter" + " " + getIterations();
		}

		if (getSleepTime() != 1) {
			params += " " + "-sleep" + " " + getSleepTime();
		}

		if (Utilities.isNotBlank(getCheckStatement())) {
			params += " -check '" + getCheckStatement().replace("'", "\\'") + "'";
		}

		return params;
	}
}
