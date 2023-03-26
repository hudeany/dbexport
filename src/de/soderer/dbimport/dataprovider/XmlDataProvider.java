package de.soderer.dbimport.dataprovider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.xml.IndentedXMLStreamWriter;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class XmlDataProvider extends DataProvider {
	// Default optional parameters
	private String nullValueText = null;

	private InputStream inputStream = null;
	private XMLStreamReader xmlReader = null;
	private Integer itemsAmount = null;
	private Map<String, DbColumnType> dataTypes = null;
	private List<String> dataPropertyNames = null;
	private String itemEntryTagName = null;
	private String listEnclosingTagName = null;
	private String dataPath = null;
	private String schemaFilePath = null;

	private final boolean isInlineData;
	private final String importFilePathOrData;
	private final char[] zipPassword;

	private final Charset encoding = StandardCharsets.UTF_8;

	public XmlDataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword, final String nullValueText, final String dataPath, final String schemaFilePath) throws Exception {
		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
		this.nullValueText = nullValueText;
		this.dataPath = dataPath;
		this.schemaFilePath = schemaFilePath;
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
				+ "Format: XML" + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			if (!isInlineData) {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("Import file does not exist: " + importFilePathOrData);
				} else if (new File(importFilePathOrData).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + importFilePathOrData);
				}
			}

			openReader();

			int itemCount = 0;
			dataTypes = new HashMap<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					final Object propertyValue = itemProperty.getValue();
					detectNextDataType(mapping, dataTypes, propertyName, (String) propertyValue);
				}

				itemCount++;
			}

			close();

			itemsAmount = itemCount;
			dataPropertyNames = new ArrayList<>(dataTypes.keySet());
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (dataPropertyNames == null) {
			if (!isInlineData) {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("Import file does not exist: " + importFilePathOrData);
				} else if (new File(importFilePathOrData).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + importFilePathOrData);
				}
			}

			openReader();

			int itemCount = 0;
			dataPropertyNames = new ArrayList<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					dataPropertyNames.add(propertyName);
				}

				itemCount++;
			}

			itemsAmount = itemCount;

			close();
		}

		return dataPropertyNames;
	}

	@Override
	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			getAvailableDataPropertyNames();
		}

		return itemsAmount;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (xmlReader == null) {
			openReader();
		}

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
			final Map<String, Object> returnMap = new HashMap<>();
			while (xmlReader.next() > 0) {
				if (xmlReader.isStartElement()) {
					final String propertyName = xmlReader.getLocalName();
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
	public void close() {
		if (xmlReader != null) {
			// Caution: XMLStreamReader.close() doesn't close the underlying stream
			Utilities.closeQuietly(xmlReader, inputStream);
			xmlReader = null;
			inputStream = null;
		}
	}

	@SuppressWarnings("resource")
	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		try {
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xml.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePathOrData + "." + fileSuffix + ".xml").getName()));
			} else {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".xml");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (IndentedXMLStreamWriter xmlWriter = new IndentedXMLStreamWriter(outputStream, encoding, "\t")) {
				xmlWriter.writeStartDocument(encoding.name(), "1.0");
				xmlWriter.writeStartElement(listEnclosingTagName);

				Map<String, Object> item;
				int itemIndex = 0;
				while ((item = getNextItemData()) != null) {
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						xmlWriter.writeStartElement("entry");

						for (final Entry<String, Object> entry : item.entrySet()) {
							xmlWriter.writeStartElement(entry.getKey());
							if (entry.getValue() == null) {
								xmlWriter.writeCharacters(nullValueText);
							} else if (entry.getValue() instanceof Date) {
								xmlWriter.writeCharacters(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, (Date) entry.getValue()));
							} else if (entry.getValue() instanceof Number) {
								xmlWriter.writeCharacters(entry.getValue().toString());
							} else if (entry.getValue() instanceof String) {
								xmlWriter.writeCharacters((String) entry.getValue());
							} else {
								xmlWriter.writeCharacters(entry.getValue().toString());
							}
							xmlWriter.writeEndElement();
						}

						xmlWriter.writeEndElement();
					}
				}
			}

			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
		}
	}

	@Override
	public long getImportDataAmount() {
		return isInlineData ? importFilePathOrData.length() : new File(importFilePathOrData).length();
	}

	private InputStream getInputStream() throws Exception {
		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).length() == 0) {
				throw new DbImportException("Import file is empty: " + importFilePathOrData);
			}

			InputStream datainputstream = null;
			try {
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || Utilities.isZipArchiveFile(new File(importFilePathOrData))) {
					if (zipPassword != null)  {
						datainputstream = Zip4jUtilities.openPasswordSecuredZipFile(importFilePathOrData, zipPassword);
					} else {
						final List<String> filepathsFromZipArchiveFile = Utilities.getFilepathsFromZipArchiveFile(new File(importFilePathOrData));
						if (filepathsFromZipArchiveFile.size() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (filepathsFromZipArchiveFile.size() > 1) {
							throw new DbImportException("Zipped import file contains more than one file: " + importFilePathOrData);
						}

						datainputstream = new ZipInputStream(new FileInputStream(new File(importFilePathOrData)));
						final ZipEntry zipEntry = ((ZipInputStream) datainputstream).getNextEntry();
						if (zipEntry == null) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (zipEntry.getSize() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData + ": " + zipEntry.getName());
						}
					}
				} else {
					datainputstream = new FileInputStream(new File(importFilePathOrData));
				}
				return datainputstream;
			} catch (final Exception e) {
				if (datainputstream != null) {
					try {
						datainputstream.close();
					} catch (@SuppressWarnings("unused") final IOException e1) {
						// do nothing
					}
				}
				throw e;
			}
		} else {
			return new ByteArrayInputStream(importFilePathOrData.getBytes(StandardCharsets.UTF_8));
		}
	}

	private void openReader() throws Exception {
		if (xmlReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			if (Utilities.isNotEmpty(schemaFilePath)) {
				XMLStreamReader xmlStreamReader = null;
				try (InputStream validationStream = getInputStream()) {
					xmlStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(validationStream);
					final SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
					final Schema schema = factory.newSchema(new File(schemaFilePath));

					final Validator validator = schema.newValidator();
					validator.validate(new StAXSource(xmlStreamReader));
				} catch (final Exception e) {
					throw new Exception("XML data does not comply to XSD '" + schemaFilePath + "': " + e.getMessage());
				} finally {
					if (xmlStreamReader != null) {
						xmlStreamReader.close();
					}
				}
			}

			final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			inputStream = getInputStream();
			xmlReader = xmlInputFactory.createXMLStreamReader(inputStream);

			if (Utilities.isNotEmpty(dataPath)) {
				// Read xml path
				if (dataPath.startsWith("/")) {
					dataPath = dataPath.substring(1);
				}
				if (dataPath.endsWith("/")) {
					dataPath = dataPath.substring(0, dataPath.length() - 1);
				}
				final Stack<String> currentPath = new Stack<>();
				xmlReader.nextTag();
				currentPath.push(xmlReader.getLocalName());
				while (currentPath.size() > 0 && !Utilities.join(currentPath, "/").equals(dataPath)) {
					if (XMLStreamConstants.START_ELEMENT == xmlReader.nextTag()) {
						currentPath.push(xmlReader.getLocalName());
					} else {
						currentPath.pop();
					}
				}
				if (currentPath.size() == 0) {
					throw new Exception("Path '" + dataPath + "' is not part of the xml data");
				}
			} else {
				// Read root tag
				xmlReader.nextTag();
			}
			listEnclosingTagName = xmlReader.getLocalName();
			if (!xmlReader.isStartElement()) {
				throw new Exception("Invalid xml data. roottagname: " + xmlReader.getLocalName());
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
