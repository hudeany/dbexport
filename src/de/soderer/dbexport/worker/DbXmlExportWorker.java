package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.worker.WorkerParentDual;
import de.soderer.utilities.xml.IndentedXMLStreamWriter;

public class DbXmlExportWorker extends AbstractDbExportWorker {
	private XMLStreamWriter xmlWriter = null;

	private String indentation = "\t";
	private String nullValueText = "";

	public DbXmlExportWorker(final WorkerParentDual parent, final DbDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);
	}

	public void setIndentation(final String indentation) {
		this.indentation = indentation;
	}

	public void setIndentation(final char indentationCharacter) {
		indentation = Character.toString(indentationCharacter);
	}

	public void setNullValueText(final String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		return
				"File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n"
				+ "Zip: " + zip + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles + "\n"
				+ "Beautify: " + beautify + "\n"
				+ "Indentation: \"" + indentation + "\"";
	}

	@Override
	protected String getFileExtension() {
		return "xml";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		if (beautify) {
			xmlWriter = new IndentedXMLStreamWriter(outputStream, encoding, indentation);
		} else {
			xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding.name());
		}
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		// Create root node
		xmlWriter.writeStartDocument(encoding.name(), "1.0");
		xmlWriter.writeStartElement("table");
		xmlWriter.writeAttribute("statement", sqlStatement);
	}

	@Override
	protected void startTableLine() throws Exception {
		xmlWriter.writeStartElement("line");
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		xmlWriter.writeStartElement(columnName);
		if (value == null) {
			xmlWriter.writeCharacters(nullValueText);
		} else if (value instanceof Date) {
			xmlWriter.writeCharacters(getDateFormatter().format(DateUtilities.getLocalDateTimeForDate((Date) value)));
		} else if (value instanceof Number) {
			if (decimalSeparator != null) {
				xmlWriter.writeCharacters(NumberUtilities.formatNumber((Number) value, decimalSeparator, null));
			} else {
				xmlWriter.writeCharacters(decimalFormat.format(value));
			}
		} else if (value instanceof String) {
			xmlWriter.writeCharacters((String) value);
		} else {
			xmlWriter.writeCharacters(value.toString());
		}
		xmlWriter.writeEndElement();
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		xmlWriter.writeStartElement(columnName);
		if (localDateValue == null) {
			xmlWriter.writeCharacters(nullValueText);
		} else {
			xmlWriter.writeCharacters(getDateFormatter().format(localDateValue));
		}
		xmlWriter.writeEndElement();
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		xmlWriter.writeStartElement(columnName);
		if (localDateTimeValue == null) {
			xmlWriter.writeCharacters(nullValueText);
		} else {
			xmlWriter.writeCharacters(getDateTimeFormatter().format(localDateTimeValue));
		}
		xmlWriter.writeEndElement();
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		xmlWriter.writeStartElement(columnName);
		if (zonedDateTimeValue == null) {
			xmlWriter.writeCharacters(nullValueText);
		} else {
			xmlWriter.writeCharacters(getDateTimeFormatter().format(zonedDateTimeValue));
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
			} catch (final Exception e) {
				e.printStackTrace();
			}
			xmlWriter = null;
		}
	}
}
