package de.soderer.utilities.xml;

import java.io.OutputStream;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class IndentedXMLStreamWriter implements XMLStreamWriter {
	private int currentDepth = 0;
	private boolean closeElementInNewLine = false;

	private String indentationString;

	private XMLStreamWriter writer;
	
	public IndentedXMLStreamWriter(OutputStream outputStream, String encoding, char indentationCharacter) throws Exception {
		this(XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding), indentationCharacter);
	}

	public IndentedXMLStreamWriter(OutputStream outputStream, String encoding, char indentationCharacter, int characterCountPerIndentation) throws Exception {
		this(XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding), indentationCharacter, characterCountPerIndentation);
	}
	
	public IndentedXMLStreamWriter(OutputStream outputStream, String encoding, String indentationString) throws Exception {
		this(XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding), indentationString);
	}
	
	public IndentedXMLStreamWriter(XMLStreamWriter writer, String indentationString) {
		this.writer = writer;
		this.indentationString = indentationString;
	}
	
	public IndentedXMLStreamWriter(XMLStreamWriter writer, char indentationCharacter) {
		this(writer, indentationCharacter, 1);
	}

	public IndentedXMLStreamWriter(XMLStreamWriter writer, char indentationCharacter, int characterCountPerIndentation) {
		this.writer = writer;
		StringBuilder indentationStringBuilder = new StringBuilder();
		for (int i = 0; i < characterCountPerIndentation; i++) {
			indentationStringBuilder.append(indentationCharacter);
		}
		indentationString = indentationStringBuilder.toString();
	}

	@Override
	public void writeStartDocument() throws XMLStreamException {
		writer.writeStartDocument();
		writer.writeCharacters("\n");
	}

	@Override
	public void writeStartDocument(String version) throws XMLStreamException {
		writer.writeStartDocument(version);
		writer.writeCharacters("\n");
	}

	@Override
	public void writeStartDocument(String encoding, String version) throws XMLStreamException {
		writer.writeStartDocument(encoding, version);
		writer.writeCharacters("\n");
	}

	@Override
	public void writeStartElement(String localName) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(localName);
	}

	@Override
	public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(namespaceURI, localName);
	}

	@Override
	public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(prefix, localName, namespaceURI);
	}

	@Override
	public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(namespaceURI, localName);
	}

	@Override
	public void writeEmptyElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(prefix, localName, namespaceURI);
	}

	@Override
	public void writeEmptyElement(String localName) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(localName);
	}

	@Override
	public void writeEndElement() throws XMLStreamException {
		onEndElement();
		writer.writeEndElement();
	}

	@Override
	public void writeCharacters(String text) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCharacters(text);
	}

	@Override
	public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCharacters(text, start, len);
	}

	@Override
	public void writeCData(String data) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCData(data);
	}

	@Override
	public void writeComment(String data) throws XMLStreamException {
		writer.writeComment(data);
	}

	@Override
	public void flush() throws XMLStreamException {
		writer.flush();
	}

	@Override
	public void writeEntityRef(String name) throws XMLStreamException {
		writer.writeEntityRef(name);
	}

	@Override
	public void writeEndDocument() throws XMLStreamException {
		onEndDocument();
		writer.writeEndDocument();
	}

	@Override
	public void close() throws XMLStreamException {
		writer.close();
	}

	@Override
	public void writeAttribute(String localName, String value) throws XMLStreamException {
		writer.writeAttribute(localName, value);
	}

	@Override
	public void writeAttribute(String prefix, String namespaceURI, String localName, String value) throws XMLStreamException {
		writer.writeAttribute(prefix, namespaceURI, localName, value);
	}

	@Override
	public void writeAttribute(String namespaceURI, String localName, String value) throws XMLStreamException {
		writer.writeAttribute(namespaceURI, localName, value);
	}

	@Override
	public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException {
		writer.writeNamespace(prefix, namespaceURI);
	}

	@Override
	public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException {
		writer.writeDefaultNamespace(namespaceURI);
	}

	@Override
	public void writeProcessingInstruction(String target) throws XMLStreamException {
		writer.writeProcessingInstruction(target);
	}

	@Override
	public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
		writer.writeProcessingInstruction(target, data);
	}

	@Override
	public void writeDTD(String dtd) throws XMLStreamException {
		writer.writeDTD(dtd);
	}

	@Override
	public String getPrefix(String uri) throws XMLStreamException {
		return writer.getPrefix(uri);
	}

	@Override
	public void setPrefix(String prefix, String uri) throws XMLStreamException {
		writer.setPrefix(prefix, uri);
	}

	@Override
	public void setDefaultNamespace(String uri) throws XMLStreamException {
		writer.setDefaultNamespace(uri);
	}

	@Override
	public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
		writer.setNamespaceContext(context);
	}

	@Override
	public NamespaceContext getNamespaceContext() {
		return writer.getNamespaceContext();
	}

	@Override
	public Object getProperty(String name) throws IllegalArgumentException {
		return writer.getProperty(name);
	}

	private void onStartElement() throws XMLStreamException {
		closeElementInNewLine = false;
		if (currentDepth > 0) {
			writer.writeCharacters("\n");
		}
		doIndent();
		currentDepth++;
	}

	private void onEndElement() throws XMLStreamException {
		currentDepth--;
		if (closeElementInNewLine) {
			writer.writeCharacters("\n");
			doIndent();
		}
		closeElementInNewLine = true;
	}

	private void onEndDocument() throws XMLStreamException {
		writer.writeCharacters("\n");
	}

	private void onEmptyElement() throws XMLStreamException {
		closeElementInNewLine = true;
		if (currentDepth > 0) {
			writer.writeCharacters("\n");
		}
		doIndent();
	}

	private void doIndent() throws XMLStreamException {
		if (currentDepth > 0) {
			for (int i = 0; i < currentDepth; i++) {
				writer.writeCharacters(indentationString);
			}
		}
	}
}