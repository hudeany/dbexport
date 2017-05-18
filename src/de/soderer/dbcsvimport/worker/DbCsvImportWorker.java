package de.soderer.dbcsvimport.worker;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.soderer.dbcsvimport.DbCsvImportDefinition.DataType;
import de.soderer.dbcsvimport.DbCsvImportException;
import de.soderer.utilities.CsvDataException;
import de.soderer.utilities.CsvFormat;
import de.soderer.utilities.CsvReader;
import de.soderer.utilities.CsvWriter;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.ZipUtilities;

public class DbCsvImportWorker extends AbstractDbImportWorker {
	// Default optional parameters
	private char separator = ';';
	private Character stringQuote = '"';
	private char escapeStringQuote = '"';
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean noHeaders = false;
	private boolean trimData = false;
	
	private CsvReader csvReader = null;
	private List<String> columnNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;
	
	public DbCsvImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, char[] password, String tableName, boolean isInlineData, String importFilePathOrData) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, tableName, isInlineData, importFilePathOrData, DataType.CSV);
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public void setStringQuote(Character stringQuote) {
		this.stringQuote = stringQuote;
	}

	public void setEscapeStringQuote(char escapeStringQuote) {
		this.escapeStringQuote = escapeStringQuote;
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
		String dataPart;
		if (isInlineData) {
			dataPart = "Data: " + importFilePathOrData + "\n";
		} else {
			dataPart = "File: " + importFilePathOrData + "\n"
			+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") + "\n";
		}
		return
			dataPart
			+ "Format: CSV" + "\n"
			+ "Encoding: " + encoding + "\n"
			+ "Separator: " + separator + "\n"
			+ "StringQuote: " + stringQuote + "\n"
			+ "EscapeStringQuote: " + escapeStringQuote + "\n"
			+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
			+ "TrimData: " + trimData + "\n"
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly + "\n"
			+ "CreateNewIndexIfNeeded: " + createNewIndexIfNeeded + "\n"
			+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n"
			+ "Table name: " + tableName + "\n"
			+ "Import mode: " + importMode + "\n"
			+ "Duplicate mode: " + duplicateMode + "\n"
			+ "Key columns: " + Utilities.join(keyColumns, ", ") + "\n"
			+ (createTableIfNotExists ? "New table was created: " + tableWasCreated + "\n" : "")
			+ "Mapping: \n" + TextUtilities.addLeadingTab(convertMappingToString(mapping)) + "\n"
			+ "Additional insert values: " + additionalInsertValues + "\n"
			+ "Additional update values: " + additionalUpdateValues + "\n"
			+ "Update with null values: " + updateWithNullValues + "\n";
	}
	
	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes() throws Exception {
		if (dataTypes == null) {
			CsvReader csvReader = null;
			InputStream inputStream = null;
			try {
				if (!isInlineData) {
					inputStream = new FileInputStream(new File(importFilePathOrData));
					if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
						inputStream = new ZipInputStream(inputStream);
						((ZipInputStream) inputStream).getNextEntry();
					}
				} else {
					inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
				}
				
				CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setAlwaysTrim(trimData);
				
				csvReader = new CsvReader(inputStream, encoding, csvFormat);
				
				if (!noHeaders) {
					// Read headers from file
					columnNames = csvReader.readNextCsvLine();
				}
				
				dataTypes = new HashMap<String, DbColumnType>();
				
				// Scan all data for maximum
				List<String> values;
				while ((values = csvReader.readNextCsvLine()) != null) {
					for (int i = 0; i < values.size(); i++) {
						String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						String formatInfo = null;
						if (mapping != null) {
							for (Tuple<String, String> mappingValue : mapping.values()) {
								if (mappingValue.getFirst().equals(columnName)) {
									if (formatInfo == null && Utilities.isNotBlank(mappingValue.getSecond())) {
										formatInfo = mappingValue.getSecond();
										break;
									}
								}
							}
						}
						
						SimpleDataType currentType = dataTypes.get(columnName) == null ? null : dataTypes.get(columnName).getSimpleDataType();
						if (currentType != SimpleDataType.Blob) {
							String currentValue = values.get(i);
							if (Utilities.isEmpty(currentValue)) {
								if (!dataTypes.containsKey(columnName)) {
									dataTypes.put(columnName, null);
								}
							} else if ("file".equalsIgnoreCase(formatInfo) || currentValue.length() > 4000) {
								dataTypes.put(columnName, new DbColumnType("BLOB", -1, -1, -1, true, false));
							} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !".".equals(formatInfo) && !",".equals(formatInfo) && !"file".equalsIgnoreCase(formatInfo) && !"lc".equalsIgnoreCase(formatInfo) && !"uc".equalsIgnoreCase(formatInfo)) {
								try {
									new SimpleDateFormat(formatInfo).parse(currentValue.trim());
									dataTypes.put(columnName, new DbColumnType("DATE", -1, -1, -1, true, false));
								} catch (Exception e) {
									dataTypes.put(columnName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(columnName) == null ? 0 : dataTypes.get(columnName).getCharacterLength(), currentValue.getBytes(encoding).length), -1, -1, true, false));
								}
								dataTypes.put(columnName, new DbColumnType("DATE", -1, -1, -1, true, false));
							} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.Double && NumberUtilities.isInteger(currentValue) && currentValue.trim().length() <= 10) {
								dataTypes.put(columnName, new DbColumnType("INTEGER", -1, -1, -1, true, false));
							} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
								dataTypes.put(columnName, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
							} else {
								dataTypes.put(columnName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(columnName) == null ? 0 : dataTypes.get(columnName).getCharacterLength(), currentValue.getBytes(encoding).length), -1, -1, true, false));
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
				if (!isInlineData) {
					inputStream = new FileInputStream(new File(importFilePathOrData));
					if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
						inputStream = new ZipInputStream(inputStream);
						((ZipInputStream) inputStream).getNextEntry();
					}
				} else {
					inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
				}
				
				CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setAlwaysTrim(true);
				
				csvReader = new CsvReader(inputStream, encoding, csvFormat);
				
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
							returnList.add("column_" + Integer.toString(i + 1));
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
				if (!isInlineData) {
					inputStream = new FileInputStream(new File(importFilePathOrData));
					if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
						inputStream = new ZipInputStream(inputStream);
						((ZipInputStream) inputStream).getNextEntry();
					}
				} else {
					inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
				}
				
				CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines);
				
				csvReader = new CsvReader(inputStream, encoding, csvFormat);
				
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

	private void openReader() throws Exception {
		if (csvReader != null) {
			throw new Exception("Reader was already opened before");
		}
		
		InputStream inputStream = null;
		try {
			if (!isInlineData) {
				inputStream = new FileInputStream(new File(importFilePathOrData));
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
					inputStream = new ZipInputStream(inputStream);
					((ZipInputStream) inputStream).getNextEntry();
				}
			} else {
				inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
			}

			CsvFormat csvFormat = new CsvFormat()
				.setSeparator(separator)
				.setStringQuote(stringQuote)
				.setStringQuoteEscapeCharacter(escapeStringQuote)
				.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
				.setAlwaysTrim(trimData);
			
			csvReader = new CsvReader(inputStream, encoding, csvFormat);
			
			if (!noHeaders) {
				// Skip headers
				csvReader.readNextCsvLine();
			}
		} catch (Exception e) {
			Utilities.closeQuietly(csvReader);
			Utilities.closeQuietly(inputStream);
			throw e;
		}
	}

	@Override
	protected Map<String, Object> getNextItemData() throws Exception {
		if (csvReader == null) {
			openReader();
		}
		
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
	public void close() {
		Utilities.closeQuietly(csvReader);
		csvReader = null;
	}

	@Override
	protected File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		CsvWriter csvWriter = null;
		try {
			openReader();
			
			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".csv.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePathOrData + "." + fileSuffix + ".csv").getName()));
			} else {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".csv");
				outputStream = new FileOutputStream(filteredDataFile);
			}
			
			csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setStringQuoteEscapeCharacter(escapeStringQuote));
			
			csvWriter.writeValues(columnNames);
			
			Map<String, Object> item;
			int itemIndex = 0;
			while ((item = getNextItemData()) != null) {
				itemIndex++;
				if (indexList.contains(itemIndex)) {
					List<String> values = new ArrayList<String>();
					for (String columnName : columnNames) {
						if (item.get(columnName) == null) {
							values.add(nullValueText);
						} else if (item.get(columnName) instanceof String) {
							values.add((String) item.get(columnName));
						} else if (item.get(columnName) instanceof Date) {
							values.add(new SimpleDateFormat(DateUtilities.YYYY_MM_DD_HHMMSS).format(item.get(columnName)));
						} else if (item.get(columnName) instanceof Number) {
							values.add(item.get(columnName).toString());
						} else {
							values.add(item.get(columnName).toString());
						}
					}
					csvWriter.writeValues(values);
				}
			}
			
			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(csvWriter);
			Utilities.closeQuietly(outputStream);
		}
	}
}
