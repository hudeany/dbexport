package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.kdbx.KdbxDatabase;
import de.soderer.utilities.kdbx.KdbxWriter;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbKdbxExportWorker extends AbstractDbExportWorker {
	private char[] kdbxPassword = null;
	private KdbxWriter kdbxWriter = null;
	private KdbxDatabase kdbxDatabase = null;
	private KdbxEntry kdbxEntry = null;

	public DbKdbxExportWorker(final WorkerParentDual parent, final DbDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath, final char[] kdbxPassword) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);

		this.kdbxPassword = kdbxPassword;

		setDateFormat(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		setDateTimeFormat(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		String configurationLogString = "File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n";

		if (compression == FileCompressionType.ZIP) {
			configurationLogString += "Compression: zip\n";
			if (zipPassword != null) {
				configurationLogString += "ZipPassword: true\n";
			}
		} else if (compression == FileCompressionType.TARGZ) {
			configurationLogString += "Compression: targz\n";
		} else if (compression == FileCompressionType.TGZ) {
			configurationLogString += "Compression: tgz\n";
		} else if (compression == FileCompressionType.GZ) {
			configurationLogString += "Compression: gz\n";
		}

		configurationLogString += "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles;

		return configurationLogString;
	}

	@Override
	protected String getFileExtension() {
		return "kdbx";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		kdbxWriter = new KdbxWriter(outputStream);
		kdbxDatabase = new KdbxDatabase();
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		// Do nothing
	}

	@Override
	protected void startTableLine() throws Exception {
		kdbxEntry = new KdbxEntry();
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		kdbxEntry.getItems().put(columnName, value);
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		kdbxEntry.getItems().put(columnName, localDateValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		kdbxEntry.getItems().put(columnName, localDateTimeValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		kdbxEntry.getItems().put(columnName, zonedDateTimeValue);
	}

	@Override
	protected void endTableLine() throws Exception {
		kdbxDatabase.getEntries().add(kdbxEntry);
		kdbxEntry = null;
	}

	@Override
	protected void endOutput() throws Exception {
		kdbxWriter.writeKdbxDatabase(kdbxDatabase, kdbxPassword);
		kdbxDatabase = null;
	}

	@Override
	protected void closeWriter() throws Exception {
		if (kdbxWriter != null) {
			try {
				kdbxWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			kdbxWriter = null;
		}
	}
}
