package de.soderer.dbexport;

import java.awt.Window;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.swing.JFileChooser;

import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.SectionedProperties;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.SimpleConsoleInput;
import de.soderer.utilities.swing.QuestionDialog;
import de.soderer.utilities.swing.SwingColor;

public class DbExportDriverSupplier {
	private final Window parent;
	private final DbVendor dbVendor;

	public DbExportDriverSupplier(final Window parent, final DbVendor dbVendor) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Invalid empty db vendor");
		}

		this.parent = parent;
		this.dbVendor = dbVendor;
	}

	public boolean supplyDriver() throws Exception {
		if (checkDriverIsAvailable()) {
			return true;
		} else {
			String driverFile = getDriverFilePathFromConfigFile();
			if (driverFile != null) {
				if (checkDriverFile(driverFile)) {
					return true;
				} else {
					driverFile = aquireDriverFileFromUser();
				}
			} else {
				driverFile = aquireDriverFileFromUser();
			}
			while (Utilities.isNotBlank(driverFile)) {
				if (checkDriverFile(driverFile)) {
					writeDriverFilePathToConfigFile(driverFile);
					return true;
				}
				driverFile = aquireDriverFileFromUser();
			}
			return false;
		}
	}

	private String aquireDriverFileFromUser() throws Exception {
		if (parent == null) {
			System.out.println(LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), DbExport.CONFIGURATION_FILE));
			System.out.println(LangResources.get("emptyForCancel"));
			return new SimpleConsoleInput().setPrompt(LangResources.get("enterDriverFile") + ": ").readInput();
		} else {
			new QuestionDialog(parent, DbExport.APPLICATION_NAME + " DB driver", LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), DbExport.CONFIGURATION_FILE), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
			final JFileChooser fileChooser = new JFileChooser();
			fileChooser.setDialogTitle(DbExport.APPLICATION_NAME + " " + LangResources.get("driverFileSelectTitle", dbVendor.toString()));
			if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(parent)) {
				return fileChooser.getSelectedFile().toString();
			} else {
				return null;
			}
		}
	}

	/**
	 * Check if driver is included in jar/classpath
	 *
	 * @param dbVendor
	 * @return
	 */
	private boolean checkDriverIsAvailable() {
		try {
			if (dbVendor == DbVendor.Derby) {
				// Prevent creation of file "derby.log"
				System.setProperty("derby.stream.error.field", "de.soderer.utilities.DbUtilities.DEV_NULL");
			}

			Class.forName(dbVendor.getDriverClassName());
			return true;
		} catch (@SuppressWarnings("unused") final ClassNotFoundException e) {
			return false;
		}
	}

	/**
	 * Check if the given file contains the wanted drive
	 *
	 * @param dbVendor
	 * @param driverFilePath
	 * @return
	 */
	private boolean checkDriverFile(final String driverFilePath) {
		if (!new File(driverFilePath).exists()) {
			return false;
		} else {
			try {
				Utilities.addFileToClasspath(driverFilePath);
			} catch (@SuppressWarnings("unused") final Exception e) {
				return false;
			}
			return checkDriverIsAvailable();
		}
	}

	/**
	 * Load data from configuration file
	 *
	 * @param dbVendor
	 * @return
	 * @throws Exception
	 */
	private String getDriverFilePathFromConfigFile() throws Exception {
		final SectionedProperties configuration = new SectionedProperties(true);
		if (!DbExport.CONFIGURATION_FILE.exists()) {
			// Create prefilled configuration file
			for (final DbVendor vendorToCreate : DbVendor.values()) {
				configuration.setValue(vendorToCreate.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
			}
			try (OutputStream outputStream = new FileOutputStream(DbExport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
			return null;
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(DbExport.CONFIGURATION_FILE)) {
				configuration.load(inputStream);
			}
			final String driverFile = configuration.getValue(dbVendor.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME);
			if (driverFile == null) {
				// Create the missing entry with an empty value
				configuration.setValue(dbVendor.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
				try (OutputStream outputStream = new FileOutputStream(DbExport.CONFIGURATION_FILE)) {
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
	 * Save data to configuration file
	 *
	 * @param dbVendor
	 * @param driverFilePath
	 * @throws Exception
	 */
	private void writeDriverFilePathToConfigFile(final String driverFilePath) throws Exception {
		final SectionedProperties configuration = new SectionedProperties(true);
		if (!DbExport.CONFIGURATION_FILE.exists()) {
			// Create prefilled configuration file
			for (final DbVendor vendorToCreate : DbVendor.values()) {
				if (vendorToCreate == dbVendor) {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, driverFilePath);
				} else {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
				}
			}
			try (OutputStream outputStream = new FileOutputStream(DbExport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(DbExport.CONFIGURATION_FILE)) {
				configuration.load(inputStream);
			}
			configuration.setValue(dbVendor.toString().toLowerCase(), DbExport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, driverFilePath);
			try (OutputStream outputStream = new FileOutputStream(DbExport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
		}
	}
}
