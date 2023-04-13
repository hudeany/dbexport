package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class ExcelDataProvider extends DataProvider {
	// Default optional parameters
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private InputStream inputStream = null;
	private String dataPath = null;
	private HSSFWorkbook hssfWorkbook;
	private HSSFSheet hssfSheet;
	private XSSFWorkbook xssfWorkbook;
	private XSSFSheet xssfSheet;
	private int currentRowNumber;
	private int maxRowNumber;

	private List<String> columnNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;

	private final String importFilePathOrData;
	private final char[] zipPassword;

	public ExcelDataProvider(final String importFilePathOrData, final char[] zipPassword, final boolean allowUnderfilledLines, final boolean noHeaders, final String nullValueText, final boolean trimData, final String dataPath) throws Exception {
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
		this.dataPath = dataPath;
	}

	@Override
	public String getConfigurationLogString() {
		final String dataPart = "File: " + importFilePathOrData + "\n"
				+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") + "\n";
		return
				dataPart
				+ "Format: EXCEL" + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "TrimData: " + trimData + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		getAvailableDataPropertyNames();

		if (dataTypes == null) {
			try {
				openReader();

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				while (currentRowNumber <= maxRowNumber) {
					final List<String> values = new ArrayList<>();
					if (hssfSheet != null) {
						for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final HSSFCell cell =  hssfSheet.getRow(currentRowNumber).getCell(i);
							final String cellValue = cell == null ? null : cell.getStringCellValue();
							values.add(trimData ? Utilities.trim(cellValue) : cellValue);
						}
					} else {
						for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final XSSFCell cell =  xssfSheet.getRow(currentRowNumber).getCell(i);
							final String cellValue = cell == null ? null : cell.getStringCellValue();
							values.add(trimData ? Utilities.trim(cellValue) : cellValue);
						}
					}

					for (int i = 0; i < values.size(); i++) {
						final String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						final String currentValue = values.get(i);
						detectNextDataType(mapping, dataTypes, columnName, currentValue);
					}

					currentRowNumber++;
				}
			} catch (final Exception e) {
				throw e;
			} finally {
				close();
			}
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			try {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("Import file does not exist: " + importFilePathOrData);
				} else if (new File(importFilePathOrData).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + importFilePathOrData);
				}

				openReader();

				columnNames = new ArrayList<>();
				if (noHeaders) {
					final List<String> returnList = new ArrayList<>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						int maxColumns = 0;
						if (hssfSheet != null) {
							for (int i = hssfSheet.getFirstRowNum(); i <= maxRowNumber; i++) {
								maxColumns = Math.max(maxColumns, hssfSheet.getRow(i).getLastCellNum() - hssfSheet.getRow(i).getFirstCellNum());
							}
						} else {
							for (int i = xssfSheet.getFirstRowNum(); i <= maxRowNumber; i++) {
								maxColumns = Math.max(maxColumns, xssfSheet.getRow(i).getLastCellNum() - xssfSheet.getRow(i).getFirstCellNum());
							}
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						int maxColumns = 0;
						if (hssfSheet != null) {
							maxColumns = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum() - hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum();
						} else {
							maxColumns = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum() - xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum();
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					if (hssfSheet != null) {
						for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final HSSFCell cell = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getCell(i);
							final String cellValue = cell == null ? null : cell.getStringCellValue();
							columnNames.add(trimData ? Utilities.trim(cellValue) : cellValue);
						}
					} else {
						for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final XSSFCell cell = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getCell(i);
							final String cellValue = cell == null ? null : cell.getStringCellValue();
							columnNames.add(trimData ? Utilities.trim(cellValue) : cellValue);
						}
					}
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return columnNames;
	}

	@Override
	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			try {
				openReader();

				if (hssfSheet != null) {
					itemsAmount = maxRowNumber - hssfSheet.getFirstRowNum() + 1;
				} else {
					itemsAmount = maxRowNumber - xssfSheet.getFirstRowNum() + 1;
				}
				if (!noHeaders) {
					// Skip header row
					itemsAmount--;
				}
			} catch (final Exception e) {
				throw e;
			}
		}
		return itemsAmount;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (hssfSheet != null) {
			if (currentRowNumber > maxRowNumber) {
				return null;
			}
		} else {
			if (currentRowNumber > maxRowNumber) {
				return null;
			}
		}

		final List<Object> values = new ArrayList<>();
		if (hssfSheet != null) {
			for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
				final HSSFCell cell = hssfSheet.getRow(currentRowNumber).getCell(i);
				if (cell == null) {
					values.add(null);
				} else if (cell.getCellType() == CellType.NUMERIC) {
					if (DateUtil.isCellDateFormatted(cell)) {
						values.add(cell.getDateCellValue());
					} else {
						values.add(cell.getNumericCellValue());
					}
				} else if (cell.getCellType() == CellType.STRING) {
					values.add(trimData ? Utilities.trim(cell.getStringCellValue()) : cell.getStringCellValue());
				} else if (cell.getCellType() == CellType.BLANK) {
					values.add(null);
				} else {
					values.add(trimData ? Utilities.trim(cell.getStringCellValue()) : cell.getStringCellValue());
				}
			}
		} else {
			for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
				final XSSFCell cell = xssfSheet.getRow(currentRowNumber).getCell(i);
				if (cell == null) {
					values.add(null);
				} else if (cell.getCellType() == CellType.NUMERIC) {
					if (DateUtil.isCellDateFormatted(cell)) {
						values.add(cell.getDateCellValue());
					} else {
						values.add(cell.getNumericCellValue());
					}
				} else if (cell.getCellType() == CellType.STRING) {
					values.add(trimData ? Utilities.trim(cell.getStringCellValue()) : cell.getStringCellValue());
				} else if (cell.getCellType() == CellType.BLANK) {
					values.add(null);
				} else {
					values.add(trimData ? Utilities.trim(cell.getStringCellValue()) : cell.getStringCellValue());
				}
			}
		}

		final Map<String, Object> returnMap = new HashMap<>();
		for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
			final String columnName = getAvailableDataPropertyNames().get(i);
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

		currentRowNumber++;

		return returnMap;
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		try {
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
				if (hssfSheet != null) {
					filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xls.zip");
					outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePathOrData + "." + fileSuffix + ".xls").getName()));
				} else {
					filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xlsx.zip");
					outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePathOrData + "." + fileSuffix + ".xlsx").getName()));
				}
			} else {
				if (hssfSheet != null) {
					filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xls");
				} else {
					filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xlsx");
				}
				outputStream = new FileOutputStream(filteredDataFile);
			}

			if (hssfSheet != null) {
				try (HSSFWorkbook workbook = new HSSFWorkbook()) {
					final HSSFSheet sheet = workbook.createSheet();
					int rowIndex = 0;
					HSSFRow currentRow;

					if (!noHeaders) {
						currentRow = sheet.createRow(rowIndex++);
						int columnIndex = 0;
						for (final String propertyName : getAvailableDataPropertyNames()) {
							final HSSFCell cell = currentRow.createCell(columnIndex++);
							cell.setCellValue(propertyName);
						}
					}

					Map<String, Object> item;
					int itemIndex = 0;
					while ((item = getNextItemData()) != null) {
						itemIndex++;
						if (indexList.contains(itemIndex)) {
							currentRow = sheet.createRow(rowIndex++);
							int columnIndex = 0;
							for (final String propertyName : getAvailableDataPropertyNames()) {
								final HSSFCell cell = currentRow.createCell(columnIndex++);
								cell.setCellValue(item.get(propertyName).toString());
							}
						}
					}

					workbook.write(outputStream);
				}
			} else {
				try (XSSFWorkbook workbook = new XSSFWorkbook()) {
					final XSSFSheet sheet = workbook.createSheet();
					int rowIndex = 0;
					XSSFRow currentRow;

					if (!noHeaders) {
						currentRow = sheet.createRow(rowIndex++);
						int columnIndex = 0;
						for (final String propertyName : getAvailableDataPropertyNames()) {
							final XSSFCell cell = currentRow.createCell(columnIndex++);
							cell.setCellValue(propertyName);
						}
					}

					Map<String, Object> item;
					int itemIndex = 0;
					while ((item = getNextItemData()) != null) {
						itemIndex++;
						if (indexList.contains(itemIndex)) {
							currentRow = sheet.createRow(rowIndex++);
							int columnIndex = 0;
							for (final String propertyName : getAvailableDataPropertyNames()) {
								final XSSFCell cell = currentRow.createCell(columnIndex++);
								cell.setCellValue(item.get(propertyName).toString());
							}
						}
					}

					workbook.write(outputStream);
				}
			}

			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
		}
	}

	@Override
	public void close() {
		if (hssfWorkbook != null) {
			Utilities.closeQuietly(hssfWorkbook);
			hssfWorkbook = null;
			hssfSheet = null;
		}
		if (xssfWorkbook != null) {
			Utilities.closeQuietly(xssfWorkbook);
			xssfWorkbook = null;
			xssfSheet = null;
		}
		Utilities.closeQuietly(inputStream);
	}

	@Override
	public long getImportDataAmount() {
		return new File(importFilePathOrData).length();
	}

	private void openReader() throws Exception {
		try {
			if (xssfSheet == null && hssfSheet == null) {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("Import file does not exist: " + importFilePathOrData);
				} else if (new File(importFilePathOrData).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + importFilePathOrData);
				} else if (new File(importFilePathOrData).length() == 0) {
					throw new DbImportException("Import file is empty: " + importFilePathOrData);
				}

				boolean isXslx;
				try {
					if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
						if (zipPassword != null) {
							inputStream = Zip4jUtilities.openPasswordSecuredZipFile(importFilePathOrData, zipPassword);
							if (importFilePathOrData.toLowerCase().endsWith("xlsx")) {
								isXslx = true;
							} else {
								isXslx = false;
							}
						} else {
							final List<String> filepathsFromZipArchiveFile = Utilities.getFilepathsFromZipArchiveFile(new File(importFilePathOrData));
							if (filepathsFromZipArchiveFile.size() == 0) {
								throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
							} else if (filepathsFromZipArchiveFile.size() > 1) {
								throw new DbImportException("Zipped import file contains more than one file: " + importFilePathOrData);
							}

							inputStream = new ZipInputStream(new FileInputStream(new File(importFilePathOrData)));
							final ZipEntry zipEntry = ((ZipInputStream) inputStream).getNextEntry();
							if (zipEntry == null) {
								throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
							} else if (zipEntry.getSize() == 0) {
								throw new DbImportException("Zipped import file is empty: " + importFilePathOrData + ": " + zipEntry.getName());
							}

							if (zipEntry.getName().toLowerCase().endsWith("xls")) {
								isXslx = false;
							} else if (zipEntry.getName().toLowerCase().endsWith("xlsx")) {
								isXslx = true;
							} else {
								throw new Exception("Unknown file extension for excel files");
							}
						}
					} else {
						inputStream = new FileInputStream(new File(importFilePathOrData));
						if (importFilePathOrData.toLowerCase().endsWith("xls")) {
							isXslx = false;
						} else if (importFilePathOrData.toLowerCase().endsWith("xlsx")) {
							isXslx = true;
						} else {
							throw new Exception("Unknown file extension for excel files");
						}
					}
				} catch (final Exception e) {
					if (inputStream != null) {
						try {
							inputStream.close();
						} catch (@SuppressWarnings("unused") final IOException e1) {
							// do nothing
						}
					}
					throw e;
				}

				if (isXslx) {
					xssfWorkbook = new XSSFWorkbook(inputStream);
					if (Utilities.isNotBlank(dataPath)) {
						xssfSheet = xssfWorkbook.getSheet(dataPath);
					} else {
						xssfSheet = xssfWorkbook.getSheetAt(0);
					}
				} else {
					hssfWorkbook = new HSSFWorkbook(inputStream);
					if (Utilities.isNotBlank(dataPath)) {
						hssfSheet = hssfWorkbook.getSheet(dataPath);
					} else {
						hssfSheet = hssfWorkbook.getSheetAt(0);
					}
				}

				if (hssfSheet != null) {
					currentRowNumber = hssfSheet.getFirstRowNum();
					maxRowNumber = hssfSheet.getLastRowNum();
					// Microsoft Excel shows ~1.046.000 available lines, but they are not used at all. So detect the last used line
					for (int rowIndex = maxRowNumber; rowIndex >= currentRowNumber; rowIndex--) {
						final HSSFRow row = hssfSheet.getRow(rowIndex);
						if (row != null) {
							boolean rowHasValue = false;
							for (int columnIndex = row.getFirstCellNum(); columnIndex < row.getLastCellNum(); columnIndex++) {
								if (row.getCell(columnIndex) != null) {
									rowHasValue = true;
									break;
								}
							}
							if (rowHasValue) {
								maxRowNumber = rowIndex;
								break;
							}
						}
					}
				} else {
					currentRowNumber = xssfSheet.getFirstRowNum();
					maxRowNumber = xssfSheet.getLastRowNum();
					// Microsoft Excel shows ~1.046.000 available lines, but they are not used at all. So detect the last used line
					for (int rowIndex = maxRowNumber; rowIndex >= currentRowNumber; rowIndex--) {
						final XSSFRow row = xssfSheet.getRow(rowIndex);
						if (row != null) {
							boolean rowHasValue = false;
							for (int columnIndex = row.getFirstCellNum(); columnIndex < row.getLastCellNum(); columnIndex++) {
								if (row.getCell(columnIndex) != null) {
									rowHasValue = true;
									break;
								}
							}
							if (rowHasValue) {
								maxRowNumber = rowIndex;
								break;
							}
						}
					}
				}
			}

			if (hssfSheet != null) {
				currentRowNumber = hssfSheet.getFirstRowNum();
			} else {
				currentRowNumber = xssfSheet.getFirstRowNum();
			}
			if (!noHeaders) {
				currentRowNumber++;
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
