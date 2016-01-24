package de.soderer.dbcsvexport;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;

import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.WorkerParentDual;

public class DbXmlExportWorker extends AbstractDbExportWorker {
	private XMLStreamWriter xmlWriter = null;
	
	public DbXmlExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) {
		super(parent, dbVendor, hostname, dbName, username, password, sqlStatementOrTablelist, outputpath);
	}

	@Override
	protected String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName
			+ "Format: " + getFileExtension().toUpperCase()
			+ "Zip: " + zip
			+ "Encoding: " + encoding
			+ "SqlStatement: " + sqlStatement
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "CreateBlobFiles: " + createBlobFiles
			+ "CreateClobFiles: " + createClobFiles
			+ "Beautify: " + beautify;
	}

	@Override
	protected String getFileExtension() {
		return "xml";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		if (beautify) {
			xmlWriter = new IndentingXMLStreamWriter(XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding));
		} else {
			xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding);
		}
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		// Create root node
		xmlWriter.writeStartDocument("utf-8","1.0");
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
			xmlWriter.writeCharacters("");
		} else if (value instanceof Date) {
			xmlWriter.writeCharacters(dateFormat.format(value));
		} else if (value instanceof Number) {
			xmlWriter.writeCharacters(decimalFormat.format(value));
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
