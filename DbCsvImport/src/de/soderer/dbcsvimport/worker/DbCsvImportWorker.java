package de.soderer.dbcsvimport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipInputStream;

import de.soderer.dbcsvimport.DbCsvImportException;
import de.soderer.utilities.CsvDataException;
import de.soderer.utilities.CsvReader;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;

public class DbCsvImportWorker extends AbstractDbImportWorker {
	// Default optional parameters
	private char separator = ';';
	private char stringQuote = '"';
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean noHeaders = false;
	private boolean trimData = false;
	
	private CsvReader csvReader = null;
	private List<String> columnNames = null;
	private Map<String, SimpleDataType> dataTypes = null;
	private Integer itemsAmount = null;
	
	public DbCsvImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, String importFilePath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, tableName, importFilePath);
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public void setStringQuote(char stringQuote) {
		this.stringQuote = stringQuote;
	}

	public void setAllowUnderfilledLines(boolean allowUnderfilledLines) {
		this.allowUnderfilledLines = allowUnderfilledLines;
	}

	public void setNoHeaders(boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public void setNullValueText(String nullValueText) {
		this.nullValueText = nullValueText;
	}

	public void setTrimData(boolean trimData) {
		this.trimData = trimData;
	}

	@Override
	public String getConfigurationLogString() {
		return
			"File: " + importFilePath
			+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePath, ".zip")
			+ "Format: CSV"
			+ "Encoding: " + encoding
			+ "Separator: " + separator
			+ "StringQuote: " + stringQuote
			+ "AllowUnderfilledLines: " + allowUnderfilledLines
			+ "TrimData: " + trimData
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly
			+ "Null value text: " + nullValueText
			+ "Table name: " + tableName
			+ "Import file path: " + importFilePath;
	}
	
	@Override
	public Map<String, SimpleDataType> scanDataPropertyTypes() throws Exception {
		if (dataTypes == null) {
			CsvReader csvReader = null;
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(new File(importFilePath));
				if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
					inputStream = new ZipInputStream(inputStream);
					((ZipInputStream) inputStream).getNextEntry();
				}
				
				csvReader = new CsvReader(inputStream, encoding, separator, stringQuote);
				csvReader.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines);
				csvReader.setAlwaysTrim(trimData);
				
				if (!noHeaders) {
					// Read headers from file
					columnNames = csvReader.readNextCsvLine();
				}
				
				dataTypes = new HashMap<String, SimpleDataType>();
				
				// Scan all data for maximum
				List<String> values;
				while ((values = csvReader.readNextCsvLine()) != null) {
					for (int i = 0; i < values.size(); i++) {
						String columnName = (columnNames == null || columnNames.size() <= i  ? Integer.toString(i) : columnNames.get(i));
						String formatInfo = null;
						for (Tuple<String, String> mappingValue : mapping.values()) {
							if (mappingValue.getFirst().equals(columnName)) {
								if (formatInfo == null && Utilities.isNotBlank(mappingValue.getSecond())) {
									formatInfo = mappingValue.getSecond();
									break;
								}
							}
						}
						
						SimpleDataType currentType = dataTypes.get(columnName);
						if (currentType != SimpleDataType.Blob) {
							String currentValue = values.get(i);
							if (Utilities.isEmpty(currentValue)) {
								dataTypes.put(columnName, null);
							} else if ("file".equalsIgnoreCase(formatInfo) || currentValue.length() > 4000) {
								dataTypes.put(columnName, SimpleDataType.Blob);
							} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !formatInfo.equals(".") && !formatInfo.equals(",") && !formatInfo.equalsIgnoreCase("file")) {
								try {
									new SimpleDateFormat(formatInfo).parse(currentValue.trim());
									dataTypes.put(columnName, SimpleDataType.Date);
								} catch (Exception e) {
									dataTypes.put(columnName, SimpleDataType.String);
								}
								dataTypes.put(columnName, SimpleDataType.Date);
							} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.Double && Utilities.isInteger(currentValue)) {
								dataTypes.put(columnName, SimpleDataType.Integer);
							} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && Utilities.isDouble(currentValue)) {
								dataTypes.put(columnName, SimpleDataType.Double);
							} else {
								dataTypes.put(columnName, SimpleDataType.String);
							}
						}
					}
				}
			} catch (Exception e) {
				throw e;
			} finally {
				Utilities.closeQuietly(csvReader);
				Utilities.closeQuietly(inputStream);
			}
		}
		
		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			CsvReader csvReader = null;
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(new File(importFilePath));
				if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
					inputStream = new ZipInputStream(inputStream);
					((ZipInputStream) inputStream).getNextEntry();
				}
				
				csvReader = new CsvReader(inputStream, encoding, separator, stringQuote);
				csvReader.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines);
				csvReader.setAlwaysTrim(true);
				
				if (noHeaders) {
					List<String> returnList = new ArrayList<String>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						List<String> values;
						int maxColumns = 0;
						while ((values = csvReader.readNextCsvLine()) != null) {
							maxColumns = Math.max(maxColumns, values.size());
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						List<String> values = csvReader.readNextCsvLine();
						for (int i = 0; i < values.size(); i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					columnNames = csvReader.readNextCsvLine();
				}
			} catch (Exception e) {
				throw e;
			} finally {
				Utilities.closeQuietly(csvReader);
				Utilities.closeQuietly(inputStream);
			}
		}
		
		return columnNames;
	}

	@Override
	protected int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			CsvReader csvReader = null;
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(new File(importFilePath));
				if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
					inputStream = new ZipInputStream(inputStream);
					((ZipInputStream) inputStream).getNextEntry();
				}
				
				csvReader = new CsvReader(inputStream, encoding, separator, stringQuote);
				csvReader.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines);
				
				if (noHeaders) {
					itemsAmount = csvReader.getCsvLineCount();
				} else {
					itemsAmount = csvReader.getCsvLineCount() - 1;
				}
			} catch (CsvDataException e) {
				throw new DbCsvImportException(e.getMessage(), e);
			} catch (Exception e) {
				throw e;
			} finally {
				Utilities.closeQuietly(csvReader);
				Utilities.closeQuietly(inputStream);
			}
		}
		return itemsAmount;
	}

	@Override
	protected void openReader() throws Exception {
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(new File(importFilePath));
			if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
				inputStream = new ZipInputStream(inputStream);
				((ZipInputStream) inputStream).getNextEntry();
			}
			csvReader = new CsvReader(inputStream, encoding, separator, stringQuote);
			csvReader.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines);
			csvReader.setAlwaysTrim(trimData);
			
			if (!noHeaders) {
				// Skip headers
				csvReader.readNextCsvLine();
			}
		} catch (Exception e) {
			Utilities.closeQuietly(csvReader);
			Utilities.closeQuietly(inputStream);
		}
	}

	@Override
	protected Map<String, Object> getNextItemData() throws Exception {
		List<String> values = csvReader.readNextCsvLine();
		if (values != null) {
			Map<String, Object> returnMap = new HashMap<String, Object>();
			for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
				String columnName = getAvailableDataPropertyNames().get(i);
				if (values.size() > i) {
					if (nullValueText != null && nullValueText.equals(values.get(i))) {
						returnMap.put(columnName, null);
					} else {
						returnMap.put(columnName, values.get(i));
					}
				} else {
					returnMap.put(columnName, null);
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	protected void closeReader() throws Exception {
		Utilities.closeQuietly(csvReader);
	}
}
