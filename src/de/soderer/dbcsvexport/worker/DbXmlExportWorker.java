package de.soderer.dbcsvexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.xml.IndentedXMLStreamWriter;

public class DbXmlExportWorker extends AbstractDbExportWorker {
	private XMLStreamWriter xmlWriter = null;
	
	private String indentation = "\t";
	private String nullValueText = "";
	
	public DbXmlExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, sqlStatementOrTablelist, outputpath);
	}
	
	public void setIndentation(String indentation) {
		this.indentation = indentation;
	}
	
	public void setIndentation(char indentationCharacter) {
		this.indentation = Character.toString(indentationCharacter);
	}

	public void setNullValueText(String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName
			+ "Format: " + getFileExtension().toUpperCase()
			+ "Zip: " + zip
			+ "Encoding: " + encoding
			+ "SqlStatement: " + sqlStatement
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "CreateBlobFiles: " + createBlobFiles
			+ "CreateClobFiles: " + createClobFiles
			+ "Beautify: " + beautify
			+ "Indentation: \"" + indentation + "\"";
	}

	@Override
	protected String getFileExtension() {
		return "xml";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		if (beautify) {
			xmlWriter = new IndentedXMLStreamWriter(outputStream, encoding, indentation);
		} else {
			xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding);
		}
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		// Create root node
		xmlWriter.writeStartDocument("utf-8", "1.0");
		xmlWriter.writeStartElement("table");
		xmlWriter.writeAttribute("statement", sqlStatement);
	}

	@Override
	protected void startTableLine() throws Exception {
		xmlWriter.writeStartElement("line");
	}

	@Override
	protected void writeColumn(String columnName, Object value) throws Exception {
		xmlWriter.writeStartElement(columnName);
		if (value == null) {
			xmlWriter.writeCharacters(nullValueText);
		} else if (value instanceof Date) {
			xmlWriter.writeCharacters(dateFormat.format(value));
		} else if (value instanceof Number) {
			xmlWriter.writeCharacters(decimalFormat.format(value));
		} else if (value instanceof String) {
			xmlWriter.writeCharacters((String) value);
		} else {
			xmlWriter.writeCharacters(value.toString());
		}
		xmlWriter.writeEndElement();
	}

	@Override
	protected void endTableLine() throws Exception {
		xmlWriter.writeEndElement();
	}

	@Override
	protected void endOutput() throws Exception {
		// Close root node
		xmlWriter.writeEndElement();
		// Close document
		xmlWriter.writeEndDocument();
	}

	@Override
	protected void closeWriter() throws Exception {
		if (xmlWriter != null) {
			try {
				xmlWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			xmlWriter = null;
		}
	}
}
