package de.soderer.utilities.xml;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class IndentedXMLStreamWriter implements XMLStreamWriter, AutoCloseable {
	public static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;
	public static final String DEFAULT_INDENTATION = "\t";

	private final String indentationString;

	private final OutputStream outputStream;

	private final XMLStreamWriter writer;

	private final Charset encoding;

	// Internal status
	private int currentDepth = 0;
	private boolean closeElementInNewLine = false;

	public IndentedXMLStreamWriter(final OutputStream outputStream) throws Exception {
		this(outputStream, DEFAULT_ENCODING, DEFAULT_INDENTATION);
	}

	public IndentedXMLStreamWriter(final OutputStream outputStream, final Charset encoding, final char indentationCharacter, final int characterCountPerIndentation) throws Exception {
		this(outputStream, encoding, multiplyCharacter(indentationCharacter, characterCountPerIndentation));
	}

	public IndentedXMLStreamWriter(final OutputStream outputStream, final Charset encoding, final String indentationString) throws Exception {
		this.outputStream = outputStream;
		writer = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, encoding.name());
		this.encoding = encoding;
		this.indentationString = indentationString;
	}

	private static String multiplyCharacter(final char charcterToMulitply, final int times) {
		final StringBuilder indentationStringBuilder = new StringBuilder();
		for (int i = 0; i < times; i++) {
			indentationStringBuilder.append(charcterToMulitply);
		}
		return indentationStringBuilder.toString();
	}

	@Override
	public void writeStartDocument() throws XMLStreamException {
		writeStartDocument(null, null, null);
	}

	@Override
	public void writeStartDocument(final String version) throws XMLStreamException {
		writeStartDocument(null, null, version);
	}

	@Override
	public void writeStartDocument(final String xmlDeclarationEncoding, final String version) throws XMLStreamException {
		writeStartDocument(null, Charset.forName(xmlDeclarationEncoding), version);
	}

	public void writeStartDocument(final Boolean standAlone, final Charset xmlDeclarationEncoding, final String version) throws XMLStreamException {
		final StringBuilder xmlDeclBuilder = new StringBuilder("<?xml");
		if (version != null) {
			xmlDeclBuilder.append(" version=\"").append(version).append("\"");
		}
		if (xmlDeclarationEncoding != null) {
			xmlDeclBuilder.append(" encoding=\"").append(xmlDeclarationEncoding).append("\"");
		}
		if (standAlone != null) {
			xmlDeclBuilder.append(" standalone=\"").append(standAlone).append("\"");
		}
		xmlDeclBuilder.append("?>\n");
		try {
			outputStream.write(xmlDeclBuilder.toString().getBytes(StandardCharsets.UTF_8));
		} catch (final IOException e) {
			throw new XMLStreamException(e);
		}
	}

	@Override
	public void writeStartElement(final String localName) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(localName);
	}

	@Override
	public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(namespaceURI, localName);
	}

	@Override
	public void writeStartElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
		onStartElement();
		writer.writeStartElement(prefix, localName, namespaceURI);
	}

	@Override
	public void writeEmptyElement(final String namespaceURI, final String localName) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(namespaceURI, localName);
	}

	@Override
	public void writeEmptyElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(prefix, localName, namespaceURI);
	}

	@Override
	public void writeEmptyElement(final String localName) throws XMLStreamException {
		onEmptyElement();
		writer.writeEmptyElement(localName);
	}

	@Override
	public void writeEndElement() throws XMLStreamException {
		onEndElement();
		writer.writeEndElement();
	}

	@Override
	public void writeCharacters(final String text) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCharacters(text);
	}

	@Override
	public void writeCharacters(final char[] text, final int start, final int len) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCharacters(text, start, len);
	}

	@Override
	public void writeCData(final String data) throws XMLStreamException {
		closeElementInNewLine = false;
		writer.writeCData(data);
	}

	@Override
	public void writeComment(final String data) throws XMLStreamException {
		writer.writeCharacters("\n");
		doIndent();
		writer.writeComment(data);
	}

	@Override
	public void flush() throws XMLStreamException {
		writer.flush();
	}

	@Override
	public void writeEntityRef(final String name) throws XMLStreamException {
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
	public void writeAttribute(final String localName, final String value) throws XMLStreamException {
		writer.writeAttribute(localName, value);
	}

	@Override
	public void writeAttribute(final String prefix, final String namespaceURI, final String localName, final String value) throws XMLStreamException {
		writer.writeAttribute(prefix, namespaceURI, localName, value);
	}

	@Override
	public void writeAttribute(final String namespaceURI, final String localName, final String value) throws XMLStreamException {
		writer.writeAttribute(namespaceURI, localName, value);
	}

	@Override
	public void writeNamespace(final String prefix, final String namespaceURI) throws XMLStreamException {
		writer.writeNamespace(prefix, namespaceURI);
	}

	@Override
	public void writeDefaultNamespace(final String namespaceURI) throws XMLStreamException {
		writer.writeDefaultNamespace(namespaceURI);
	}

	@Override
	public void writeProcessingInstruction(final String target) throws XMLStreamException {
		writer.writeProcessingInstruction(target);
	}

	@Override
	public void writeProcessingInstruction(final String target, final String data) throws XMLStreamException {
		writer.writeProcessingInstruction(target, data);
	}

	@Override
	public void writeDTD(final String dtd) throws XMLStreamException {
		writer.writeDTD(dtd);
	}

	@Override
	public String getPrefix(final String uri) throws XMLStreamException {
		return writer.getPrefix(uri);
	}

	@Override
	public void setPrefix(final String prefix, final String uri) throws XMLStreamException {
		writer.setPrefix(prefix, uri);
	}

	@Override
	public void setDefaultNamespace(final String uri) throws XMLStreamException {
		writer.setDefaultNamespace(uri);
	}

	@Override
	public void setNamespaceContext(final NamespaceContext context) throws XMLStreamException {
		writer.setNamespaceContext(context);
	}

	@Override
	public NamespaceContext getNamespaceContext() {
		return writer.getNamespaceContext();
	}

	@Override
	public Object getProperty(final String name) throws IllegalArgumentException {
		return writer.getProperty(name);
	}

	public void write(final Document document) throws Exception {
		if (document.getXmlStandalone()) {
			writeStartDocument(true, encoding, "1.0");
		} else {
			writeStartDocument(encoding.name(), "1.0");
		}
		final NodeList childList = document.getChildNodes();
		for (int childNodeIndex = 0; childNodeIndex < childList.getLength(); childNodeIndex++) {
			final Node childNode = childList.item(childNodeIndex);
			write(childNode);
		}
		writeEndDocument();
	}

	public void write(final Node node) throws Exception {
		switch (node.getNodeType()) {
			case Node.DOCUMENT_TYPE_NODE:
				writer.writeDTD("<!DOCTYPE " + ((DocumentType) node).getName() + " PUBLIC \"" + ((DocumentType) node).getPublicId() + "\" \"" + ((DocumentType) node).getSystemId() + "\">");
				writer.writeCharacters("\n");
				break;
			case Node.ELEMENT_NODE:
				final NodeList childList = node.getChildNodes();
				if (childList.getLength() == 0) {
					writeEmptyElement(node.getNodeName());
					final NamedNodeMap attributeList = node.getAttributes();
					if (attributeList != null) {
						for (int attributeIndex = 0; attributeIndex < attributeList.getLength(); attributeIndex++) {
							final Node attribute = attributeList.item(attributeIndex);
							writeAttribute(attribute.getNodeName(), attribute.getNodeValue());
						}
					}
				} else {
					writeStartElement(node.getNodeName());
					final NamedNodeMap attributeList = node.getAttributes();
					if (attributeList != null) {
						for (int attributeIndex = 0; attributeIndex < attributeList.getLength(); attributeIndex++) {
							final Node attribute = attributeList.item(attributeIndex);
							writeAttribute(attribute.getNodeName(), attribute.getNodeValue());
						}
					}
					for (int childNodeIndex = 0; childNodeIndex < childList.getLength(); childNodeIndex++) {
						final Node childNode = childList.item(childNodeIndex);
						write(childNode);
					}
					writeEndElement();
				}
				break;
			case Node.TEXT_NODE:
				if (node.getNodeValue() != null && (!node.getNodeValue().startsWith("\n") || !isBlank(node.getNodeValue()))) {
					writeCharacters(node.getNodeValue().replace("&", "&amp;").replace("<", "&lt;"));
				}
				break;
			case Node.COMMENT_NODE:
				if (node.getNodeValue() != null) {
					writeComment(node.getNodeValue().replace("&", "&amp;").replace("<", "&lt;"));
				}
				break;
			default:
				throw new Exception("Unexpected xmlnodetype");
		}
	}

	private static boolean isBlank(final String value) {
		if (value == null || value.length() == 0) {
			return true;
		}
		for (int i = 0; i < value.length(); i++) {
			if (!Character.isWhitespace(value.charAt(i))) {
				return false;
			}
		}
		return true;
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
