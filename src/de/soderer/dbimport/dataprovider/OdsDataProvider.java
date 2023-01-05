package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.zip.ZipUtilities;

public class OdsDataProvider extends DataProvider {
	private final String importFilePath;

	private FileInputStream inputStream = null;
	private XMLStreamReader xmlReader = null;

	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private Integer itemsAmount = null;
	private Map<String, DbColumnType> dataTypes = null;

	private List<String> columnNames = null;
	private int currentRowNumber;

	public OdsDataProvider(final String importFilePath, final boolean allowUnderfilledLines, final boolean noHeaders, final String nullValueText, final boolean trimData) throws Exception {
		this.importFilePath = importFilePath;
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
	}

	@Override
	public String getConfigurationLogString() {
		final String dataPart = "File: " + importFilePath + "\n";
		return
				dataPart
				+ "Format: ODS" + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "NoHeaders: " + noHeaders + "\n"
				+ "TrimData: " + trimData + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			try {
				if (xmlReader == null) {
					openReader();
				}
				currentRowNumber = 0;

				if (!noHeaders) {
					// Read headers from file
					columnNames = readNextRow();
				}

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				List<String> values;
				while ((values = readNextRow()) != null) {
					for (int i = 0; i < values.size(); i++) {
						final String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						final String currentValue = values.get(i);
						detectNextDataType(mapping, dataTypes, columnName, currentValue);
					}
				}
			} catch (final Exception e) {
				throw e;
			}

			close();
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			try {
				if (!new File(importFilePath).exists()) {
					throw new DbImportException("Import file does not exist: " + importFilePath);
				} else if (new File(importFilePath).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + importFilePath);
				}

				if (xmlReader == null) {
					openReader();
				}
				currentRowNumber = 0;

				columnNames = new ArrayList<>();
				if (noHeaders) {
					final List<String> returnList = new ArrayList<>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						List<String> values;
						int maxColumns = 0;
						while ((values = readNextRow()) != null) {
							maxColumns = Math.max(maxColumns, values.size());
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						final List<String> values = readNextRow();
						for (int i = 0; i < values.size(); i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					columnNames = readNextRow();
				}
			} catch (final Exception e) {
				throw e;
			}

			close();
		}

		return columnNames;
	}

	private List<String> readNextRow() throws Exception {
		List<String> returnList = null;
		while (xmlReader.next() > 0) {
			if (xmlReader.isStartElement() && xmlReader.getLocalName().equals("table-row")) {
				currentRowNumber++;
				returnList = new ArrayList<>();

				while (xmlReader.next() > 0) {
					if (xmlReader.isStartElement() && xmlReader.getLocalName().equals("table-cell")) {
						if (xmlReader.getAttributeValue("xxxurn:oasis:names:tc:opendocument:xmlns:table:1.0", "number-columns-repeated") != null) {
							// TODO
							System.out.println("TODO");

							while (xmlReader.next() > 0) {
								if (!xmlReader.isEndElement() || !xmlReader.getLocalName().equals("table-cell")) {
									break;
								}
							}
						} else if (xmlReader.getAttributeValue("urn:oasis:names:tc:opendocument:xmlns:office:1.0", "value-type") != null && !"string".equalsIgnoreCase(xmlReader.getAttributeValue("urn:oasis:names:tc:opendocument:xmlns:office:1.0", "value-type"))) {
							final String dataType = xmlReader.getAttributeValue("urn:oasis:names:tc:opendocument:xmlns:office:1.0", "value-type");
							if ("float".equalsIgnoreCase(dataType)) {
								returnList.add(xmlReader.getAttributeValue("urn:oasis:names:tc:opendocument:xmlns:office:1.0", "value"));
							} else if ("date".equalsIgnoreCase(dataType)) {
								returnList.add(xmlReader.getAttributeValue("urn:oasis:names:tc:opendocument:xmlns:office:1.0", "date-value"));
							} else {
								throw new Exception("Unsupported datatype: " + dataType);
							}

							while (xmlReader.next() > 0) {
								if (!xmlReader.isEndElement() || !xmlReader.getLocalName().equals("table-cell")) {
									break;
								}
							}
						} else {
							boolean isEmptyCell = true;
							while (xmlReader.next() > 0) {
								if (xmlReader.isStartElement() && xmlReader.getLocalName().equals("p")) {
									String value = "";
									while (true) {
										xmlReader.next();
										if (xmlReader.isCharacters()) {
											value += xmlReader.getText().trim();
											isEmptyCell = false;
										} else if (xmlReader.isStartElement() && xmlReader.getLocalName().equals("s")) {
											value += " ";
											xmlReader.next();
										} else {
											break;
										}
									}

									if (nullValueText != null && nullValueText.equals(value)) {
										value = null;
									}
									returnList.add(value);

									if (!xmlReader.isEndElement() || !xmlReader.getLocalName().equals("p")) {
										throw new Exception("Invalid xml data. Missing closing tag 'p'");
									}
								} else if (xmlReader.isEndElement() && xmlReader.getLocalName().equals("table-cell")) {
									if (isEmptyCell) {
										returnList.add("");
									}
									break;
								}
							}
						}
					} else if (xmlReader.isEndElement() && xmlReader.getLocalName().equals("table-row")) {
						break;
					}
				}
				break;
			}
		}
		if (returnList != null && returnList.size() == 1 && returnList.get(0).equals("")) {
			return null;
		} else {
			return returnList;
		}
	}

	@Override
	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			if (xmlReader == null) {
				openReader();
			}
			currentRowNumber = 0;

			try {
				int numberOfRows = 0;
				while (readNextRow() != null) {
					numberOfRows++;
				}

				if (noHeaders) {
					itemsAmount = numberOfRows;
				} else {
					itemsAmount = numberOfRows - 1;
				}
			} catch (final Exception e) {
				throw e;
			}

			close();
		}
		return itemsAmount;
	}

	public static String readElementBody(final XMLStreamReader xmlStreamReader) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		int depth = 0;
		while (xmlStreamReader.hasNext()) {
			final int xmlType = xmlStreamReader.next();

			if (xmlType == XMLStreamConstants.START_ELEMENT) {
				if ("s".equals(xmlStreamReader.getLocalName())) {
					int numberOfSpaces = 1;
					for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
						if ("c".equals(xmlStreamReader.getAttributeLocalName(i))) {
							numberOfSpaces = Integer.parseInt(xmlStreamReader.getAttributeValue(i));
							break;
						}
					}
					for (int i = 0; i < numberOfSpaces; i++) {
						buffer.append(" ");
					}
				}

				depth++;
			} else if (xmlType == XMLStreamConstants.END_ELEMENT) {
				depth--;

				if (depth < 0) {
					break;
				}
			} else {
				buffer.append(xmlStreamReader.getText());
			}
		}

		return buffer.toString();
	}

	private void openReader() throws Exception {
		if (xmlReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			final File tempContentFile = File.createTempFile("importTempFile", ".xml");
			tempContentFile.delete();
			ZipUtilities.unzipFile(new File(importFilePath), tempContentFile, null, "content.xml");

			final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			inputStream = new FileInputStream(tempContentFile);
			xmlReader = xmlInputFactory.createXMLStreamReader(inputStream);

			final String dataPath = "document-content/body/spreadsheet/table";
			final Stack<String> currentPath = new Stack<>();
			xmlReader.next();
			currentPath.push(xmlReader.getLocalName());
			while (currentPath.size() > 0 && !Utilities.join(currentPath, "/").equals(dataPath)) {
				xmlReader.next();
				if (xmlReader.isStartElement()) {
					currentPath.push(xmlReader.getLocalName());
				} else if (xmlReader.isEndElement()) {
					currentPath.pop();
				}
			}
			if (currentPath.size() == 0) {
				throw new Exception("Path '" + dataPath + "' is not part of the xml data");
			}

			currentRowNumber = 0;
		} catch (final Exception e) {
			close();
			throw e;
		}
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (columnNames == null) {
			getAvailableDataPropertyNames();
		}

		if (xmlReader == null) {
			openReader();
		}

		if (currentRowNumber == 0 && !noHeaders) {
			// Skip Header line
			readNextRow();
		}

		final List<String> values = readNextRow();
		if (values != null) {
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
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	public void close() {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			inputStream = null;
		}
		if (xmlReader != null) {
			try {
				xmlReader.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			xmlReader = null;
		}
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		throw new Exception("filterDataItems method is not supported by OdsDataProvider");
	}

	@Override
	public long getImportDataAmount() {
		return new File(importFilePath).length();
	}
}
