package de.soderer.dbcsvimport;

import java.awt.Window;
import java.io.Console;
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
import de.soderer.utilities.swing.TextDialog;

public class DbCsvImportDriverSupplier {
	private Window parent;
	private DbVendor dbVendor;
	
	public DbCsvImportDriverSupplier(Window parent, DbVendor dbVendor) throws Exception {
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
			System.out.println(LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), DbCsvImport.CONFIGURATION_FILE));
			System.out.println(LangResources.get("emptyForCancel"));
			Console console = System.console();
			if (console == null) {
				throw new Exception("Couldn't get Console instance");
			}

			return console.readLine(LangResources.get("enterDriverFile") + ": ");
		} else {
			new TextDialog(parent, DbCsvImport.APPLICATION_NAME + " DB driver", LangResources.get("driverIsMissing", dbVendor.toString() + "(" + dbVendor.getDriverClassName() + ")\nDownload URL: " + DbUtilities.getDownloadUrl(dbVendor), DbCsvImport.CONFIGURATION_FILE), LangResources.get("ok")).setVisible(true);
			JFileChooser fileChooser = new JFileChooser();
			fileChooser.setDialogTitle(DbCsvImport.APPLICATION_NAME + " " + LangResources.get("driverFileSelectTitle", dbVendor.toString()));
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
			Class.forName(dbVendor.getDriverClassName());
			return true;
		} catch (ClassNotFoundException e) {
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
	private boolean checkDriverFile(String driverFilePath) {
		if (!new File(driverFilePath).exists()) {
			return false;
		} else {
			try {
				Utilities.addFileToClasspath(driverFilePath);
			} catch (Exception e) {
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
		SectionedProperties configuration = new SectionedProperties(true);
		if (!DbCsvImport.CONFIGURATION_FILE.exists()) {
			// Create prefilled configuration file
			for (DbVendor vendorToCreate : DbVendor.values()) {
				configuration.setValue(vendorToCreate.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
			}
			try (OutputStream outputStream = new FileOutputStream(DbCsvImport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
			return null;
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(DbCsvImport.CONFIGURATION_FILE)) {
				configuration.load(inputStream);
			}
			String driverFile = configuration.getValue(dbVendor.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME);
			if (driverFile == null) {
				// Create the missing entry with an empty value
				configuration.setValue(dbVendor.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
				try (OutputStream outputStream = new FileOutputStream(DbCsvImport.CONFIGURATION_FILE)) {
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
	private void writeDriverFilePathToConfigFile(String driverFilePath) throws Exception {
		SectionedProperties configuration = new SectionedProperties(true);
		if (!DbCsvImport.CONFIGURATION_FILE.exists()) {
			// Create prefilled configuration file
			for (DbVendor vendorToCreate : DbVendor.values()) {
				if (vendorToCreate == dbVendor) {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, driverFilePath);
				} else {
					configuration.setValue(vendorToCreate.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, "");
				}
			}
			try (OutputStream outputStream = new FileOutputStream(DbCsvImport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
		} else {
			// Load and fill existing configuration file
			try (InputStream inputStream = new FileInputStream(DbCsvImport.CONFIGURATION_FILE)) {
				configuration.load(inputStream);
			}
			configuration.setValue(dbVendor.toString().toLowerCase(), DbCsvImport.CONFIGURATION_DRIVERLOCATIONPROPERTYNAME, driverFilePath);
			try (OutputStream outputStream = new FileOutputStream(DbCsvImport.CONFIGURATION_FILE)) {
				configuration.save(outputStream);
			}
		}
	}
}
