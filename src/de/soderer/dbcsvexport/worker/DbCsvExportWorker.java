package de.soderer.dbcsvexport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.soderer.utilities.CsvFormat;
import de.soderer.utilities.CsvFormat.QuoteMode;
import de.soderer.utilities.CsvReader;
import de.soderer.utilities.CsvWriter;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentDual;

public class DbCsvExportWorker extends AbstractDbExportWorker {
	// Default optional parameters
	private char separator = ';';
	private char stringQuote = '"';
	private String nullValueText = "";
	private boolean alwaysQuote = false;
	private boolean noHeaders = false;
	
	private CsvWriter csvWriter = null;
	
	private CsvWriter beautifiedCsvWriter = null;
	private File temporaryUglifiedFile = null;
	private boolean[] columnPaddings = null;
	private int[] minimumColumnSizes = null;
	
	private List<String> values = null;
	
	public DbCsvExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, boolean isStatementFile, String sqlStatementOrTablelist, String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, isStatementFile, sqlStatementOrTablelist, outputpath);
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public void setStringQuote(char stringQuote) {
		this.stringQuote = stringQuote;
	}

	public void setAlwaysQuote(boolean alwaysQuote) {
		this.alwaysQuote = alwaysQuote;
	}

	public void setNoHeaders(boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public void setNullValueText(String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName + "\n"
			+ "Format: " + getFileExtension().toUpperCase() + "\n"
			+ "Separator: " + separator + "\n"
			+ "Zip: " + zip + "\n"
			+ "Encoding: " + encoding + "\n"
			+ "StringQuote: " + stringQuote + "\n"
			+ "AlwaysQuote: " + alwaysQuote + "\n"
			+ "SqlStatement: " + sqlStatement + "\n"
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage() + "\n"
			+ "CreateBlobFiles: " + createBlobFiles + "\n"
			+ "CreateClobFiles: " + createClobFiles + "\n"
			+ "Beautify: " + beautify;
	}

	@Override
	protected String getFileExtension() {
		return "csv";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		if (beautify) {
			temporaryUglifiedFile = File.createTempFile("DbCsvExport_Uglified", ".csv", new File(System.getProperty("java.io.tmpdir")));
			csvWriter = new CsvWriter(new FileOutputStream(temporaryUglifiedFile), encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setQuoteMode(alwaysQuote ? QuoteMode.QUOTE_ALL_DATA : QuoteMode.QUOTE_IF_NEEDED));
			beautifiedCsvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote));
		} else {
			csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setQuoteMode(alwaysQuote ? QuoteMode.QUOTE_ALL_DATA : QuoteMode.QUOTE_IF_NEEDED));
		}
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		if (!noHeaders) {
			csvWriter.writeValues(columnNames);
		}
		
		minimumColumnSizes = new int[columnNames.size()];
		for (int i = 0; i < columnNames.size(); i++) {
			minimumColumnSizes[i] = csvWriter.calculateOutputSizesOfValue(columnNames.get(i));
		}
		
		columnPaddings = new boolean[columnNames.size()];
		for (int i = 0; i < columnPaddings.length; i++) {
			columnPaddings[i] = true;
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		values = new ArrayList<String>();
	}

	@Override
	protected void writeColumn(String columnName, Object value) throws Exception {
		if (value == null) {
			values.add(nullValueText);
		} else if (value instanceof String) {
			values.add((String) value);
		} else if (value instanceof Date) {
			values.add(dateFormat.format(value));
		} else if (value instanceof Number) {
			columnPaddings[values.size()] = false;
			values.add(decimalFormat.format(value));
		} else {
			values.add(value.toString());
		}
		
		minimumColumnSizes[values.size() - 1] = Math.max(minimumColumnSizes[values.size() - 1], csvWriter.calculateOutputSizesOfValue(values.get(values.size() - 1)));
	}

	@Override
	protected void endTableLine() throws Exception {
		csvWriter.writeValues(values);
		values = null;
	}

	@Override
	protected void endOutput() throws Exception {
		// Do nothing
	}

	@Override
	protected void closeWriter() throws Exception {
		if (csvWriter != null) {
			try {
				csvWriter.flush();
				csvWriter.close();
				csvWriter = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// Beautify data from uglified csv file
		if (beautifiedCsvWriter != null) {
			CsvReader csvReaderFinal = null;
			try {
				beautifiedCsvWriter.setColumnPaddings(columnPaddings);
				beautifiedCsvWriter.setMinimumColumnSizes(minimumColumnSizes);
				csvReaderFinal = new CsvReader(new FileInputStream(temporaryUglifiedFile), encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote));
				List<String> nextLine;
				while ((nextLine = csvReaderFinal.readNextCsvLine()) != null) {
					beautifiedCsvWriter.writeValues(nextLine);
				}
			} finally {
				Utilities.closeQuietly(csvReaderFinal);
				Utilities.closeQuietly(beautifiedCsvWriter);
			}
			beautifiedCsvWriter = null;
		}
		
		if (temporaryUglifiedFile != null) {
			temporaryUglifiedFile.delete();
		}
	}
}
