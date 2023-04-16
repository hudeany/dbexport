package de.soderer.utilities;

import java.io.File;

import de.soderer.utilities.DbUtilities.DbVendor;

public class DbDefinition {
	/** The db vendor. */
	protected DbUtilities.DbVendor dbVendor = null;

	/** The hostname. */
	protected String hostnameAndPort;

	/** The db name. */
	protected String dbName;

	/** The username. */
	protected String username;

	/** The password, may be entered interactivly */
	protected char[] password;

	protected boolean secureConnection = false;

	protected File trustStoreFile = null;

	protected char[] trustStorePassword = null;

	public DbDefinition() {
		// do nothing
	}

	public DbDefinition(final DbVendor dbVendor, final String hostnameAndPort, final String dbName, final String username, final char[] password) {
		this.dbVendor = dbVendor;
		this.hostnameAndPort = hostnameAndPort;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
	}

	public DbDefinition(final DbVendor dbVendor, final String hostnameAndPort, final String dbName, final String username, final char[] password, final boolean secureConnection, final File trustStoreFile, final char[] trustStorePassword) {
		this.dbVendor = dbVendor;
		this.hostnameAndPort = hostnameAndPort;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.secureConnection = secureConnection;
		this.trustStoreFile = trustStoreFile;
		this.trustStorePassword = trustStorePassword;
	}

	public DbUtilities.DbVendor getDbVendor() {
		return dbVendor;
	}

	public void setDbVendor(final DbUtilities.DbVendor dbVendor) {
		this.dbVendor = dbVendor;
	}

	public String getHostnameAndPort() {
		return hostnameAndPort;
	}

	public void setHostnameAndPort(final String hostnameAndPort) {
		this.hostnameAndPort = hostnameAndPort;
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

	public boolean isSecureConnection() {
		return secureConnection;
	}

	public void setSecureConnection(final boolean secureConnection) {
		this.secureConnection = secureConnection;
	}

	public File getTrustStoreFile() {
		return trustStoreFile;
	}

	public void setTrustStoreFile(final File trustStoreFile) {
		this.trustStoreFile = trustStoreFile;
	}

	public char[] getTrustStorePassword() {
		return trustStorePassword;
	}

	public void setTrustStorePassword(final char[] trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}

	public void checkParameters(final String applicationName, final File configurationFile) throws Exception {
		if (getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, getDbVendor()).supplyDriver(applicationName, configurationFile)) {
					throw new DbDefinitionException("Cannot aquire db driver for db vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot aquire db driver for db vendor: " + getDbVendor(), e);
			}
		}

		if (dbVendor == DbVendor.SQLite) {
			if (Utilities.isNotBlank(hostnameAndPort)) {
				throw new DbDefinitionException("SQLite db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbDefinitionException("SQLite db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbDefinitionException("SQLite db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.Derby) {
			if (Utilities.isNotBlank(hostnameAndPort)) {
				throw new DbDefinitionException("Derby db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbDefinitionException("Derby db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbDefinitionException("Derby db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbName = Utilities.replaceUsersHome(dbName);
			if (dbName.startsWith("/")) {
				if (Utilities.isNotBlank(hostnameAndPort)) {
					throw new DbDefinitionException("HSQL file db connections do not support the hostname parameter");
				} else if (Utilities.isNotBlank(username)) {
					throw new DbDefinitionException("HSQL file db connections do not support the username parameter");
				} else if (Utilities.isNotBlank(password)) {
					throw new DbDefinitionException("HSQL file db connections do not support the password parameter");
				}
			}
		} else if (dbVendor == DbVendor.Cassandra) {
			if (Utilities.isBlank(hostnameAndPort)) {
				throw new DbDefinitionException("Missing or invalid hostname");
			}
			// username and password may be left empty
		} else {
			if (Utilities.isBlank(hostnameAndPort)) {
				throw new DbDefinitionException("Missing or invalid hostname");
			} else {
				final String[] hostParts = hostnameAndPort.split(":");
				if (hostParts.length == 2) {
					if (!NumberUtilities.isInteger(hostParts[1])) {
						throw new DbDefinitionException("Invalid port in hostname: " + hostnameAndPort);
					}
				} else if (hostParts.length > 2) {
					throw new DbDefinitionException("Invalid hostname: " + hostnameAndPort);
				}
			}
			if (Utilities.isBlank(username)) {
				throw new DbDefinitionException("Missing or invalid username");
			}
			if (Utilities.isBlank(password)) {
				throw new DbDefinitionException("Missing or invalid empty password");
			}
		}
	}

	public void importParameters(final DbDefinition otherDbDefinition) {
		if (otherDbDefinition != null) {
			dbVendor = otherDbDefinition.getDbVendor();
			hostnameAndPort = otherDbDefinition.getHostnameAndPort();
			dbName = otherDbDefinition.getDbName();
			username = otherDbDefinition.getUsername();
			password = otherDbDefinition.getPassword();
			secureConnection = otherDbDefinition.isSecureConnection();
			trustStoreFile = otherDbDefinition.getTrustStoreFile();
			trustStorePassword = otherDbDefinition.getTrustStorePassword();
		} else {
			dbVendor = null;
			hostnameAndPort = null;
			dbName = null;
			username = null;
			password = null;
			secureConnection = false;
			trustStoreFile = null;
			trustStorePassword = null;
		}
	}
}
