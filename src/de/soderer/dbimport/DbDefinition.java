package de.soderer.dbimport;

import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;

public class DbDefinition {
	/** The db vendor. */
	protected DbUtilities.DbVendor dbVendor = null;

	/** The hostname. */
	protected String hostname;

	/** The db name. */
	protected String dbName;

	/** The username. */
	protected String username;

	/** The password, may be entered interactivly */
	protected char[] password;

	protected boolean secureConnection;

	protected String trustStoreFilePath;

	protected char[] trustStorePassword;

	public DbUtilities.DbVendor getDbVendor() {
		return dbVendor;
	}

	public void setDbVendor(final DbUtilities.DbVendor dbVendor) {
		this.dbVendor = dbVendor;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(final String hostname) {
		this.hostname = hostname;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(final String dbName) {
		this.dbName = dbName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(final String username) {
		this.username = username;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(final char[] password) {
		this.password = password;
	}

	public boolean getSecureConnection() {
		return secureConnection;
	}

	public void setSecureConnection(final boolean secureConnection) {
		this.secureConnection = secureConnection;
	}

	public String getTrustStoreFilePath() {
		return trustStoreFilePath;
	}

	public void setTrustStoreFilePath(final String trustStoreFilePath) {
		this.trustStoreFilePath = trustStoreFilePath;
	}

	public char[] getTrustStorePassword() {
		return trustStorePassword;
	}

	public void setTrustStorePassword(final char[] trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}

	public void checkParameters() throws DbImportException {
		if (getDbVendor() != null) {
			try {
				if (!new DbImportDriverSupplier(null, getDbVendor()).supplyDriver()) {
					throw new DbImportException("Cannot aquire db driver for db vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbImportException("Cannot aquire db driver for db vendor: " + getDbVendor(), e);
			}
		}

		if (dbVendor == DbVendor.SQLite) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbImportException("SQLite db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbImportException("SQLite db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbImportException("SQLite db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.Derby) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbImportException("Derby db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbImportException("Derby db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbImportException("Derby db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbName = Utilities.replaceUsersHome(dbName);
			if (dbName.startsWith("/")) {
				if (Utilities.isNotBlank(hostname)) {
					throw new DbImportException("HSQL file db connections do not support the hostname parameter");
				} else if (Utilities.isNotBlank(username)) {
					throw new DbImportException("HSQL file db connections do not support the username parameter");
				} else if (Utilities.isNotBlank(password)) {
					throw new DbImportException("HSQL file db connections do not support the password parameter");
				}
			}
		} else if (dbVendor == DbVendor.Cassandra) {
			if (Utilities.isBlank(hostname)) {
				throw new DbImportException("Missing or invalid hostname");
			}
			// username and password may be left empty
		} else {
			if (Utilities.isBlank(hostname)) {
				throw new DbImportException("Missing or invalid hostname");
			} else {
				final String[] hostParts = hostname.split(":");
				if (hostParts.length == 2) {
					if (!NumberUtilities.isInteger(hostParts[1])) {
						throw new DbImportException("Invalid port in hostname: " + hostname);
					}
				} else if (hostParts.length > 2) {
					throw new DbImportException("Invalid hostname: " + hostname);
				}
			}
			if (Utilities.isBlank(username)) {
				throw new DbImportException("Missing or invalid username");
			}
			if (Utilities.isBlank(password)) {
				throw new DbImportException("Missing or invalid empty password");
			}
		}
	}

	public void importParameters(final DbDefinition otherDbDefinition) {
		if (otherDbDefinition != null) {
			dbVendor = otherDbDefinition.getDbVendor();
			hostname = otherDbDefinition.getHostname();
			dbName = otherDbDefinition.getDbName();
			username = otherDbDefinition.getUsername();
			password = otherDbDefinition.getPassword();
			secureConnection = otherDbDefinition.getSecureConnection();
			trustStoreFilePath = otherDbDefinition.getTrustStoreFilePath();
			trustStorePassword = otherDbDefinition.getTrustStorePassword();
		} else {
			dbVendor = null;
			hostname = null;
			dbName = null;
			username = null;
			password = null;
			secureConnection = false;
			trustStoreFilePath = null;
			trustStorePassword = null;
		}
	}
}
