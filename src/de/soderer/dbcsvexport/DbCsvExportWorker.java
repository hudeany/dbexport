package de.soderer.dbcsvexport;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import de.soderer.utilities.CsvWriter;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentDual;

public class DbCsvExportWorker extends AbstractDbExportWorker {
	// Default optional parameters
	private char separator = ';';
	private char stringQuote = '"';
	private boolean alwaysQuote = false;
	private boolean noHeaders = false;
	
	private CsvWriter csvWriter = null;
	
	private List<String> columnNames = null;
	
	private List<String> values = null;
	
	private boolean headersDone = false;
	
	public DbCsvExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) {
		super(parent, dbVendor, hostname, dbName, username, password, sqlStatementOrTablelist, outputpath);
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
	
	protected void preRead(Connection connection, String sqlStatement) throws Exception {
		// Scan all data for column lengths
		int[] minimumColumnSizes = null;
		boolean[] columnPaddings = null;
		if (beautify) {
			Statement statement = null;
			ResultSet resultSet = null;

			try {
				statement = connection.createStatement();
				resultSet = statement.executeQuery(sqlStatement);
				ResultSetMetaData metaData = resultSet.getMetaData();
	
				columnPaddings = new boolean[metaData.getColumnCount()];
				for (int i = 0; i < columnPaddings.length; i++) {
					columnPaddings[i] = true;
				}
	
				// Scan headers
				List<String> headers = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					headers.add(metaData.getColumnName(i));
				}
				minimumColumnSizes = csvWriter.calculateOutputSizesOfValues(headers);
	
				if (currentItemName == null) {
					itemsDone++;
					showProgress();
				} else {
					subItemsDone++;
					showItemProgress();
				}
	
				// Scan values
				while (resultSet.next() && !cancel) {
					List<String> values = new ArrayList<String>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						String valueString;
						if (resultSet.getObject(i) == null) {
							valueString = "";
						} else if (metaData.getColumnType(i) == Types.BLOB) {
							if (createBlobFiles) {
								valueString = "Clobfile to be created later";
							} else {
								byte[] data = Utilities.toByteArray(resultSet.getBlob(i).getBinaryStream());
								valueString = Base64.getEncoder().encodeToString(data);
							}
						} else if (metaData.getColumnType(i) == Types.CLOB) {
							if (createClobFiles) {
								valueString = "Blobfile to be created later";
							} else {
								valueString = resultSet.getString(i);
							}
						} else if (metaData.getColumnType(i) == Types.DATE || metaData.getColumnType(i) == Types.TIMESTAMP) {
							valueString = dateFormat.format(resultSet.getObject(i));
						} else if (metaData.getColumnType(i) == Types.DECIMAL || metaData.getColumnType(i) == Types.DOUBLE || metaData.getColumnType(i) == Types.FLOAT) {
							valueString = decimalFormat.format(resultSet.getObject(i));
							columnPaddings[i - 1] = false;
						} else if (metaData.getColumnType(i) == Types.BIGINT || metaData.getColumnType(i) == Types.BIT || metaData.getColumnType(i) == Types.INTEGER
								|| metaData.getColumnType(i) == Types.NUMERIC || metaData.getColumnType(i) == Types.SMALLINT || metaData.getColumnType(i) == Types.TINYINT) {
							valueString = resultSet.getString(i);
							columnPaddings[i - 1] = false;
						} else {
							valueString = resultSet.getString(i);
						}
						values.add(valueString);
					}
					int[] nextColumnSizes = csvWriter.calculateOutputSizesOfValues(values);
					for (int i = 0; i < nextColumnSizes.length; i++) {
						minimumColumnSizes[i] = Math.max(minimumColumnSizes[i], nextColumnSizes[i]);
					}
	
					if (currentItemName == null) {
						itemsDone++;
						showProgress();
					} else {
						subItemsDone++;
						showItemProgress();
					}
				}
			} finally {
				if (resultSet != null) {
					try {
						resultSet.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				if (statement != null) {
					try {
						statement.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			resultSet.close();
			resultSet = null;

			if (currentItemName == null) {
				itemsDone = 0;
				showProgress();
			} else {
				subItemsDone = 0;
				showItemProgress();
			}
		}
		csvWriter.setColumnPaddings(columnPaddings);
		csvWriter.setMinimumColumnSizes(minimumColumnSizes);
	}

	@Override
	protected String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName
			+ "Format: " + getFileExtension().toUpperCase()
			+ "Separator: " + separator
			+ "Zip: " + zip
			+ "Encoding: " + encoding
			+ "StringQuote: " + stringQuote
			+ "AlwaysQuote: " + alwaysQuote
			+ "SqlStatement: " + sqlStatement
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "CreateBlobFiles: " + createBlobFiles
			+ "CreateClobFiles: " + createClobFiles
			+ "Beautify: " + beautify;
	}

	@Override
	protected String getFileExtension() {
		return "csv";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		csvWriter = new CsvWriter(outputStream, encoding, separator, stringQuote);
		csvWriter.setAlwaysQuote(alwaysQuote);
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		preRead(connection, sqlStatement);
		
		if (!noHeaders) {
			csvWriter.writeValues(columnNames);
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		columnNames = new ArrayList<String>();
		values = new ArrayList<String>();
	}

	@Override
	protected void writeColumn(String columnName, Object value) throws Exception {
		columnNames.add(columnName);
		if (value == null) {
			values.add("");
		} else if (value instanceof String) {
			values.add((String) value);
		} else if (value instanceof Date) {
			values.add(dateFormat.format(value));
		} else if (value instanceof Number) {
			values.add(decimalFormat.format(value));
		} else {
			values.add(value.toString());
		}
	}

	@Override
	protected void endTableLine() throws Exception {
		if (!noHeaders && !headersDone) {
			csvWriter.writeValues(columnNames);
			headersDone = true;
		}
		csvWriter.writeValues(values);
		
		columnNames = null;
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
	}
}
