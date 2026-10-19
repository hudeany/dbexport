package de.soderer.dbexport;

import java.awt.Window;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

import javax.swing.JFileChooser;

import de.soderer.console.SimpleConsoleInput;
import de.soderer.swing.QuestionDialog;
import de.soderer.swing.SwingColor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.SectionedProperties;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;

/**
 * Makes sure, that the JDBC driver of a database vendor is available.
 *
 * <p>
 * If the driver is not on the class path, the driver jar file configured in the configuration file is loaded
 * by its own class loader and registered at the {@link DriverManager}.
 * If there is none, the user is asked for the driver jar file (via a file dialog in the GUI or on the console)
 * and the chosen file is stored in the configuration file.
 * </p>
 */
public class DbDriverSupplier {
	private final Window parent;
	private final DbVendor dbVendor;

	/**
	 * Creates a driver supplier.
	 *
	 * @param parent parent window for the dialogs, or null to ask on the console
	 * @param dbVendor the database vendor, whose driver is needed
	 * @throws Exception if no database vendor is given
	 */
	public DbDriverSupplier(final Window parent, final DbVendor dbVendor) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Invalid empty database vendor");
		}

		this.parent = parent;
		this.dbVendor = dbVendor;
	}

	/**
	 * Makes the JDBC driver available, if it is not yet.
	 *
	 * @param applicationName application name shown in the dialogs
	 * @param configurationFile configuration file, which contains the path of the driver jar file
	 * @return true if the driver is available, false if the user canceled the selection of a driver file
	 * @throws Exception if the configuration file cannot be read or written
	 */
	public boolean supplyDriver(final String applicationName, final File configurationFile) throws Exception {
		if (checkDriverIsAvailable()) {
			return true;
		} else {
			String driverFile = getDriverFilePathFromConfigFile(configurationFile);
			if (driverFile != null) {
				if (checkDriverFile(driverFile)) {
					return true;
				} else {
					driverFile = aquireDriverFileFromUser(applicationName, configurationFile);
				}
			} else {
				driverFile = aquireDriverFileFromUser(applicationName, configurationFile);
			}
			while (Utilities.isNotBlank(driverFile)) {
				if (checkDriverFile(driverFile)) {
					writeDriverFilePathToConfigFile(configurationFile, driverFile);
					return true;
				}
				driverFile = aquireDriverFileFromUser(applicationName, configurationFile);
			}
			return false;
		}
	}

	private String aquireDriverFileFromUser(final String applicationName, final File configurationFile) throws Exception {
		if (parent == null) {
			System.out.println(LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), configurationFile));
			System.out.println(LangResources.get("emptyForCancel"));
			return new SimpleConsoleInput().withPrompt(LangResources.get("enterDriverFile") + ": ").readInput();
		} else {
			new QuestionDialog(parent, applicationName + " database driver", LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), configurationFile), LangResources.get("ok")).withBackgroundColor(SwingColor.LightRed).open();
			final JFileChooser fileChooser = new JFileChooser();
			fileChooser.setDialogTitle(applicationName + " " + LangResources.get("driverFileSelectTitle", dbVendor.toString()));
			if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(parent)) {
				return fileChooser.getSelectedFile().toString();
			} else {
				return null;
			}
		}
	}

	/**
	 * Check if the driver is included in the jar/classpath or was already loaded from a driver jar file
	 *
	 * @return true if the driver is available
	 */
	private boolean checkDriverIsAvailable() {
		prepareDriverLoading();
		try {
			Class.forName(dbVendor.getDriverClassName());
			return true;
		} catch (@SuppressWarnings("unused") final ClassNotFoundException e) {
			// A driver loaded from a jar file by its own class loader is only visible via its registered shim
			for (final Driver driver : Collections.list(DriverManager.getDrivers())) {
				if (driver instanceof DriverShim && ((DriverShim) driver).getDriverClassName().equals(dbVendor.getDriverClassName())) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Settings, which must be done before a driver class is loaded
	 */
	private void prepareDriverLoading() {
		if (dbVendor == DbVendor.Derby) {
			// Prevent creation of file "derby.log"
			System.setProperty("derby.stream.error.field", "de.soderer.utilities.db.DbUtilities.DEV_NULL");
		}
	}

	/**
	 * Check if the given file contains the wanted driver and register it at the DriverManager.
	 *
	 * <p>
	 * Since Java 9 the system class loader cannot be extended at runtime anymore, so the jar file gets its own
	 * class loader. The DriverManager only uses drivers, whose class is visible to the class loader of the caller,
	 * so the driver is registered wrapped in a {@link DriverShim}, which is loaded by the application class loader.
	 * </p>
	 *
	 * @param driverFilePath path of the driver jar file
	 * @return true if the driver was loaded and registered
	 */
	private boolean checkDriverFile(final String driverFilePath) {
		final File driverFile = new File(driverFilePath);
		if (!driverFile.isFile()) {
			return false;
		}

		prepareDriverLoading();
		URLClassLoader driverClassLoader = null;
		try {
			driverClassLoader = new URLClassLoader(new URL[] { driverFile.toURI().toURL() }, DbDriverSupplier.class.getClassLoader());
			final Class<?> driverClass = Class.forName(dbVendor.getDriverClassName(), true, driverClassLoader);
			final Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
			DriverManager.registerDriver(new DriverShim(driver));
			// The class loader is not closed, because the driver classes are used for the rest of the application's lifetime
			return true;
		} catch (@SuppressWarnings("unused") final Exception | LinkageError e) {
			// Wrong jar file, missing driver class, missing dependencies or driver compiled for a newer Java version
			Utilities.closeQuietly(driverClassLoader);
			return false;
		}
	}

	/**
	 * Wrapper for a JDBC driver loaded by a separate class loader.
	 * The DriverManager ignores drivers, whose class is not visible to the class loader of the caller, but this wrapper
	 * class is loaded by the application class loader, so the DriverManager uses it and it delegates to the real driver.
	 */
	private static class DriverShim implements Driver {
		private final Driver driver;

		DriverShim(final Driver driver) {
			this.driver = driver;
		}

		String getDriverClassName() {
			return driver.getClass().getName();
		}

		@Override
		public Connection connect(final String url, final Properties info) throws SQLException {
			return driver.connect(url, info);
		}

		@Override
		public boolean acceptsURL(final String url) throws SQLException {
			return driver.acceptsURL(url);
		}

		@Override
		public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
			return driver.getPropertyInfo(url, info);
		}

		@Override
		public int getMajorVersion() {
			return driver.getMajorVersion();
		}

		@Override
		public int getMinorVersion() {
			return driver.getMinorVersion();
		}

		@Override
		public boolean jdbcCompliant() {
			return driver.jdbcCompliant();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return driver.getParentLogger();
		}
	}

	/**
	 * Load data from configuration file
	 *
	 * @param dbVendor
	 * @return
	 * @throws Exception
	 */
	private String getDriverFilePathFromConfigFile(final File configurationFile) throws Exception {
		final SectionedProperties configuration = new SectionedProperties(true);
		if (!configurationFile.exists()) {
			createParentDirectory(configurationFile);
			// Create prefilled configuration file
			for (final DbVendor vendorToCreate : DbVendor.values()) {
				configuration.setValue(vendorToCreate.toString().toLowerCase(), "driver_location", "");
			}
			try (OutputStream outputStream = new FileOutputStream(configurationFile)) {
				configuration.save(outputStream);
			}
			return null;
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(configurationFile)) {
				configuration.load(inputStream);
			}
			final String driverFile = configuration.getValue(dbVendor.toString().toLowerCase(), "driver_location");
			if (driverFile == null) {
				// Create the missing entry with an empty value
				configuration.setValue(dbVendor.toString().toLowerCase(), "driver_location", "");
				try (OutputStream outputStream = new FileOutputStream(configurationFile)) {
					configuration.save(outputStream);
				}
				return null;
			} else if (Utilities.isNotBlank(driverFile)) {
				return driverFile;
			} else {
				return null;
			}
		}
	}

	/**
	 * Creates the directory of the configuration file ("~/.DbExport"), which does not exist on a fresh installation.
	 */
	private static void createParentDirectory(final File configurationFile) throws Exception {
		final File directory = configurationFile.getAbsoluteFile().getParentFile();
		if (directory != null && !directory.isDirectory() && !directory.mkdirs()) {
			throw new Exception("Cannot create directory '" + directory.getAbsolutePath() + "'");
		}
	}

	/**
	 * Save data to configuration file
	 *
	 * @param dbVendor
	 * @param driverFilePath
	 * @throws Exception
	 */
	private void writeDriverFilePathToConfigFile(final File configurationFile, final String driverFilePath) throws Exception {
		final SectionedProperties configuration = new SectionedProperties(true);
		if (!configurationFile.exists()) {
			createParentDirectory(configurationFile);
			// Create prefilled configuration file
			for (final DbVendor vendorToCreate : DbVendor.values()) {
				if (vendorToCreate == dbVendor) {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), "driver_location", driverFilePath);
				} else {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), "driver_location", "");
				}
			}
			try (OutputStream outputStream = new FileOutputStream(configurationFile)) {
				configuration.save(outputStream);
			}
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(configurationFile)) {
				configuration.load(inputStream);
			}
			configuration.setValue(dbVendor.toString().toLowerCase(), "driver_location", driverFilePath);
			try (OutputStream outputStream = new FileOutputStream(configurationFile)) {
				configuration.save(outputStream);
			}
		}
	}
}
