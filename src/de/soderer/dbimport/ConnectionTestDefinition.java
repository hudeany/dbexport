package de.soderer.dbimport;

import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;

public class ConnectionTestDefinition extends DbDefinition {
	/** The statement to use for check. */
	private String checkStatement;

	private int iterations = 1;
	private int sleepTime = 1;

	public String getCheckStatement() {
		return checkStatement;
	}

	public void setCheckStatement(final String checkStatement) {
		this.checkStatement = checkStatement;
	}

	public int getIterations() {
		return iterations;
	}

	public void setIterations(final int iterations) {
		this.iterations = iterations;
	}

	public int getSleepTime() {
		return sleepTime;
	}

	public void setSleepTime(final int sleepTime) {
		this.sleepTime = sleepTime;
	}

	@Override
	public void checkParameters() throws DbImportException {
		super.checkParameters();

		if (iterations < 0) {
			throw new DbImportException("Invalid connectiontest iterations");
		}

		if (sleepTime < 0) {
			throw new DbImportException("Invalid connectiontest sleep time");
		}
	}

	public String toParamsString() {
		String params = "connectiontest";
		params += " " + getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostname();
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
