package de.soderer.dbcsvimport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;

public class DbXmlImportWorker extends AbstractDbImportWorker {
	// Default optional parameters
	private String nullValueText = null;
	
	private InputStream inputStream = null;
	private XMLStreamReader xmlReader = null;
	private Integer itemsAmount = null;
	private Map<String, SimpleDataType> dataTypes = null;
	private String itemEntryTagName = null;
	
	public DbXmlImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, String importFilePath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, tableName, importFilePath);
	}

	public void setNullValueText(String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString() {
		return
			"File: " + importFilePath
			+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePath, ".zip")
			+ "Format: XML"
			+ "Encoding: " + encoding
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly
			+ "Null value text: " + nullValueText
			+ "Table name: " + tableName
			+ "Import file path: " + importFilePath;
	}
	
	@Override
	public Map<String, SimpleDataType> scanDataPropertyTypes() throws Exception {
		if (dataTypes == null) {
			getAvailableDataPropertyNames();
		}
		
		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (dataTypes == null) {
			openReader();
			
			int itemCount = 0;
			dataTypes = new HashMap<String, SimpleDataType>();
			
			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (Entry<String, Object> itemProperty : nextItem.entrySet()) {
					String propertyName = itemProperty.getKey();
					Object propertyValue = itemProperty.getValue();
					
					String formatInfo = null;
					for (Tuple<String, String> mappingValue : mapping.values()) {
						if (mappingValue.getFirst().equals(propertyName)) {
							if (formatInfo == null && Utilities.isNotBlank(mappingValue.getSecond())) {
								formatInfo = mappingValue.getSecond();
								break;
							}
						}
					}
					
					SimpleDataType currentType = dataTypes.get(propertyName);
					if (currentType != SimpleDataType.Blob) {
						if (propertyValue == null) {
							dataTypes.put(propertyName, null);
						} else if ("file".equalsIgnoreCase(formatInfo) || (propertyValue instanceof String && ((String) propertyValue).length() > 4000)) {
							dataTypes.put(propertyName, SimpleDataType.Blob);
						} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !formatInfo.equals(".") && !formatInfo.equals(",") && !formatInfo.equalsIgnoreCase("file") && propertyValue instanceof String) {
							String value = ((String) propertyValue).trim();
							try {
								new SimpleDateFormat(formatInfo).parse(value);
								dataTypes.put(propertyName, SimpleDataType.Date);
							} catch (Exception e1) {
								try {
									DateUtilities.ISO_8601_DATETIME_FORMAT.parse(value);
									dataTypes.put(propertyName, SimpleDataType.Date);
								} catch (Exception e2) {
									dataTypes.put(propertyName, SimpleDataType.String);
								}
							}
							dataTypes.put(propertyName, SimpleDataType.Date);
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.Double && propertyValue instanceof Integer) {
							dataTypes.put(propertyName, SimpleDataType.Integer);
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && (propertyValue instanceof Float || propertyValue instanceof Double)) {
							dataTypes.put(propertyName, SimpleDataType.Double);
						} else {
							dataTypes.put(propertyName, SimpleDataType.String);
						}
					}
				}
				
				itemCount++;
			}
			
			itemsAmount = itemCount;
			
			closeReader();
		}
		
		return new ArrayList<String>(dataTypes.keySet());
	}

	@Override
	protected int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			getAvailableDataPropertyNames();
		}
		
		return itemsAmount;
	}

	@Override
	protected void openReader() throws Exception {
		try {
			inputStream = new FileInputStream(new File(importFilePath));
			if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
				inputStream = new ZipInputStream(inputStream);
				((ZipInputStream) inputStream).getNextEntry();
			}
			
			XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			xmlReader = xmlInputFactory.createXMLStreamReader(inputStream);
			
			// Read root tag
			xmlReader.nextTag();
			if (!xmlReader.isStartElement()) {
				throw new Exception("Invalid xml data. roottagname: " + xmlReader.getLocalName());
			}
		} catch (Exception e) {
			closeReader();
		}
	}

	@Override
	protected Map<String, Object> getNextItemData() throws Exception {
		// Read item entries
		if (xmlReader.nextTag() > 0) {
			if (xmlReader.isStartElement()) {
				if (itemEntryTagName == null) {
					itemEntryTagName = xmlReader.getLocalName();
				} else if (!itemEntryTagName.equals(xmlReader.getLocalName())) {
					throw new Exception("Invalid xml data. tagname: " + xmlReader.getLocalName());
				}
			} else if (xmlReader.isEndElement()) {
				return null;
			}
			
				
			// Read properties
			Map<String, Object> returnMap = new HashMap<String, Object>();
			while (xmlReader.next() > 0) {
				if (xmlReader.isStartElement()) {
					String propertyName = xmlReader.getLocalName();
					xmlReader.next();
					if (xmlReader.isCharacters()) {
						String value = xmlReader.getText();
						if (nullValueText != null && nullValueText.equals(value)) {
							value = null;
						}
						returnMap.put(propertyName, value);
						
						xmlReader.next();
						if (!xmlReader.isEndElement()) {
							throw new Exception("Invalid xml data. Missing closing tag for: " + propertyName);
						}
					} else if (xmlReader.isEndElement()) {
						returnMap.put(propertyName, "");
					} else {
						throw new Exception("Invalid xml data");
					}
				} else if (xmlReader.isEndElement()) {
					break;
				} else {
					throw new Exception("Invalid xml data");
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	protected void closeReader() throws Exception {
		if (xmlReader != null) {
			// Caution: doesn't close the underlying stream
			xmlReader.close();
		}
		Utilities.closeQuietly(inputStream);
	}
}
