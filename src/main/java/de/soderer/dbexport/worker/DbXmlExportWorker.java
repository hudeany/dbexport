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
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.worker.WorkerParentDual;
import de.soderer.utilities.xml.IndentedXMLStreamWriter;

/**
 * Export worker for XML data (a "table" element with a "line" element per data line).
 */
public class DbXmlExportWorker extends AbstractDbExportWorker {
	private XMLStreamWriter xmlWriter = null;

	private String indentation = "\t";
	private String nullValueText = "";

	/**
	 * Creates the worker.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param isStatementFile true if sqlStatementOrTablelist is the path of a file containing the statement or table list
	 * @param sqlStatementOrTablelist SQL select statement, or comma separated table name patterns
	 * @param outputpath output file (single statement) or directory (table list), or "console" or "gui"
	 */
	public DbXmlExportWorker(final WorkerParentDual parent, final DbConnectionDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);
	}

	/**
	 * Sets the indentation of beautified output. Default is a tab.
	 *
	 * @param indentation the indentation string
	 */
	public void setIndentation(final String indentation) {
		this.indentation = indentation;
	}

	/**
	 * Sets the indentation of beautified output to a single character.
	 *
	 * @param indentationCharacter the indentation character
	 */
	public void setIndentation(final char indentationCharacter) {
		indentation = Character.toString(indentationCharacter);
	}

	/**
	 * Sets the text for null values. Default is an empty text.
	 *
	 * @param nullValueText the text for null values
	 */
	public void setNullValueText(final String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		String configurationLogString = "File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n";

		if (compression == FileCompressionType.ZIP) {
			configurationLogString += "Compression: zip\n";
			if (zipPassword != null) {
				configurationLogString += "ZipPassword: true\n";
			}
		} else if (compression == FileCompressionType.TARGZ) {
			configurationLogString += "Compression: targz\n";
		} else if (compression == FileCompressionType.TGZ) {
			configurationLogString += "Compression: tgz\n";
		} else if (compression == FileCompressionType.GZ) {
			configurationLogString += "Compression: gz\n";
		}

		configurationLogString += "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles + "\n"
				+ "Beautify: " + beautify + "\n"
				+ "Indentation: \"" + indentation + "\"";

		return configurationLogString;
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
