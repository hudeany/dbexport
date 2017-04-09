package de.soderer.utilities.xml;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;

/**
 * Utilities class to work with XML documents.
 */
public class XmlUtilities {
	/**
	 * Gets the empty document.
	 *
	 * @return the empty document
	 * @throws ParserConfigurationException
	 *             the parser configuration exception
	 */
	public static Document getEmptyDocument() throws ParserConfigurationException {
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.newDocument();
		return document;
	}

	/**
	 * Download and parse xml file.
	 *
	 * @param url
	 *            the url
	 * @return the document
	 * @throws IOException
	 *             the IO exception
	 */
	public static Document downloadAndParseXmlFile(String url) throws IOException {
		BufferedInputStream inputStream = null;
		try {
			inputStream = new BufferedInputStream(new URL(url).openStream());
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(new InputSource(inputStream));
			return document;
		} catch (Exception e) {
			return null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	/**
	 * Parse xml file.
	 *
	 * @param file
	 *            the file
	 * @return the document
	 * @throws IOException
	 *             the IO exception
	 */
	public static Document parseXmlFile(File file) throws IOException {
		BufferedInputStream inputStream = null;
		try {
			inputStream = new BufferedInputStream(new FileInputStream(file));
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(new InputSource(inputStream));
			return document;
		} catch (Exception e) {
			return null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	/**
	 * Gets a single x path node.
	 *
	 * @param document
	 *            the document
	 * @param xpathExpression
	 *            the xpath expression
	 * @return the single x path node
	 */
	public static Node getSingleXPathNode(Document document, String xpathExpression) {
		if (document == null) {
			return null;
		} else if (Utilities.isBlank(xpathExpression)) {
			return null;
		} else {
			try {
				NodeList nodeList = (NodeList) XPathFactory.newInstance().newXPath().evaluate(xpathExpression, document, XPathConstants.NODESET);
				if (nodeList != null && nodeList.getLength() > 0) {
					return nodeList.item(0);
				} else {
					return null;
				}
			} catch (XPathExpressionException e) {
				return null;
			}
		}
	}

	/**
	 * Parses a xml string.
	 *
	 * @param xmlInput
	 *            the xml input
	 * @return the document
	 * @throws Exception
	 *             the exception
	 */
	public static Document parseXmlString(String xmlInput) throws Exception {
		boolean validation = false;
		boolean ignoreWhitespace = true;
		boolean ignoreComments = true;
		boolean putCDATAIntoText = true;
		boolean createEntityRefs = false;

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		// set the configuration options
		dbf.setValidating(validation);
		dbf.setIgnoringComments(ignoreComments);
		dbf.setIgnoringElementContentWhitespace(ignoreWhitespace);
		dbf.setCoalescing(putCDATAIntoText);
		// The opposite of creating entity ref nodes is expanding them inline
		dbf.setExpandEntityReferences(!createEntityRefs);

		DocumentBuilder db = null;
		Document doc = null;
		try {
			db = dbf.newDocumentBuilder();
			doc = db.parse(new InputSource(new StringReader(xmlInput)));
			return doc;

		} catch (Exception e) {
			throw new Exception("Error while parsing xml", e);
		}
	}

	/**
	 * Ignores DTD for better performance. This works faster for XHTML, because it doesn't download the dtd from any website like "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"
	 *
	 * @param xmlInput
	 *            the xml input
	 * @return the document
	 * @throws Exception
	 *             the exception
	 */
	public static Document parseXmlStringIngoringDtd(String xmlInput) throws Exception {
		if (xmlInput == null) {
			throw new Exception("XML-Data is null");
		} else if (Utilities.isBlank(xmlInput)) {
			throw new Exception("XML-Data is empty");
		}

		boolean validation = false;
		boolean ignoreWhitespace = true;
		boolean ignoreComments = true;
		boolean putCDATAIntoText = true;
		boolean createEntityRefs = false;

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		// set the configuration options
		dbf.setValidating(validation);
		dbf.setIgnoringComments(ignoreComments);
		dbf.setIgnoringElementContentWhitespace(ignoreWhitespace);
		dbf.setCoalescing(putCDATAIntoText);
		// The opposite of creating entity ref nodes is expanding them inline
		dbf.setExpandEntityReferences(!createEntityRefs);

		DocumentBuilder db = null;
		Document doc = null;
		try {
			db = dbf.newDocumentBuilder();

			db.setEntityResolver(new EntityResolver() {
				@Override
				public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
					return new InputSource(new StringReader(""));
				}
			});

			doc = db.parse(new InputSource(new StringReader(xmlInput)));
			return doc;
		} catch (Exception e) {
			throw new Exception("Error while parsing xml", e);
		}
	}

	/**
	 * Returns the content of a simple text tag If there are more than one text node or other nodetypes it will return null.
	 *
	 * @param node
	 *            the node
	 * @return the simple text value from node
	 */
	public static String getSimpleTextValueFromNode(Node node) {
		if (node != null && node.getChildNodes().getLength() == 1 && node.getChildNodes().item(0).getNodeType() == Node.TEXT_NODE) {
			return node.getChildNodes().item(0).getTextContent();
		} else {
			return null;
		}
	}

	/**
	 * Gets a attribute value.
	 *
	 * @param pNode
	 *            the p node
	 * @param pAttributeName
	 *            the p attribute name
	 * @return the attribute value
	 */
	public static String getAttributeValue(Node pNode, String pAttributeName) {
		String returnString = null;

		NamedNodeMap attributes = pNode.getAttributes();
		if (attributes != null) {
			for (int i = 0; i < attributes.getLength(); i++) {
				if (attributes.item(i).getNodeName().equalsIgnoreCase(pAttributeName)) {
					returnString = attributes.item(i).getNodeValue();
					break;
				}
			}
		}

		return returnString;
	}

	/**
	 * Gets a node value.
	 *
	 * @param pNode
	 *            the p node
	 * @return the node value
	 */
	public static String getNodeValue(Node pNode) {
		if (pNode.getNodeValue() != null) {
			return pNode.getNodeValue();
		} else if (pNode.getFirstChild() != null) {
			return getNodeValue(pNode.getFirstChild());
		} else {
			return null;
		}
	}

	/**
	 * Gets the node as string.
	 *
	 * @param pNode
	 *            the p node
	 * @param encoding
	 *            the encoding
	 * @param pRemoveXmlLine
	 *            the p remove xml line
	 * @return the node as string
	 * @throws Exception
	 *             the exception
	 */
	public static String getNodeAsString(Node pNode, String encoding, boolean pRemoveXmlLine) throws Exception {
		StringWriter writer = new StringWriter();
		StreamResult result = new StreamResult(writer);
		transformNode(pNode, encoding, pRemoveXmlLine, result);
		return writer.toString();
	}

	/**
	 * Gets the node as raw.
	 *
	 * @param pNode
	 *            the p node
	 * @param encoding
	 *            the encoding
	 * @param pRemoveXmlLine
	 *            the p remove xml line
	 * @return the node as raw
	 * @throws Exception
	 *             the exception
	 */
	public static byte[] getNodeAsRaw(Node pNode, String encoding, boolean pRemoveXmlLine) throws Exception {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		StreamResult result = new StreamResult(outputStream);
		transformNode(pNode, encoding, pRemoveXmlLine, result);
		return outputStream.toByteArray();
	}

	/**
	 * Write document to file.
	 *
	 * @param document
	 *            the document
	 * @param encoding
	 *            the encoding
	 * @param file
	 *            the file
	 * @throws Exception
	 *             the exception
	 */
	public static void writeDocumentToFile(Document document, String encoding, File file) throws Exception {
		OutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(file);
			StreamResult result = new StreamResult(outputStream);
			transformNode(document, encoding, false, result);
		} finally {
			Utilities.closeQuietly(outputStream);
		}
	}

	/**
	 * Transform node.
	 *
	 * @param node
	 *            the node
	 * @param encoding
	 *            the encoding
	 * @param removeXmlHeader
	 *            the remove xml header
	 * @param result
	 *            the result
	 * @throws Exception
	 *             the exception
	 */
	private static void transformNode(Node node, String encoding, boolean removeXmlHeader, StreamResult result) throws Exception {
		TransformerFactory transformerFactory = null;
		Transformer transformer = null;
		DOMSource source = null;

		try {
			transformerFactory = TransformerFactory.newInstance();
			if (transformerFactory == null) {
				throw new Exception("TransformerFactory error");
			}

			transformer = transformerFactory.newTransformer();
			if (transformer == null) {
				throw new Exception("Transformer error");
			}

			transformer.setOutputProperty(OutputKeys.ENCODING, encoding);
			if (removeXmlHeader) {
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			} else {
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
			}

			source = new DOMSource(node);

			transformer.transform(source, result);
		} catch (TransformerFactoryConfigurationError e) {
			throw new Exception("TransformerFactoryConfigurationError", e);
		} catch (TransformerConfigurationException e) {
			throw new Exception("TransformerConfigurationException", e);
		} catch (TransformerException e) {
			throw new Exception("TransformerException", e);
		}
	}

	/**
	 * Parses the xml data and xsd verify by dom.
	 *
	 * @param pData
	 *            the p data
	 * @param byteEncoding
	 *            the byte encoding
	 * @param xsdFileName
	 *            the xsd file name
	 * @return the document
	 * @throws Exception
	 *             the exception
	 * @throws Exception
	 *             the exception
	 */
	public static Document parseXMLDataAndXSDVerifyByDOM(byte[] pData, String byteEncoding, String xsdFileName) throws Exception, Exception {
		try {
			if (pData == null) {
				return null;
			}

			if (pData[pData.length - 1] == 0) {
				pData = new String(pData, "UTF-8").trim().getBytes("UTF-8");
			}

			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			if (docBuilderFactory == null) {
				throw new Exception("DocumentBuilderFactory error");
			}
			docBuilderFactory.setNamespaceAware(true);

			if (xsdFileName != null) {
				String schemaURI = xsdFileName;
				docBuilderFactory.setValidating(true);
				docBuilderFactory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema");
				docBuilderFactory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", schemaURI);
			}

			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			if (docBuilder == null) {
				throw new Exception("DocumentBuilder error");
			}

			InputSource inputSource = new InputSource(new ByteArrayInputStream(pData));
			if (byteEncoding != null) {
				inputSource.setEncoding(byteEncoding);
			}
			ParseErrorHandler errorHandler = new ParseErrorHandler();
			docBuilder.setErrorHandler(errorHandler);

			Document xmlDocument = docBuilder.parse(inputSource);

			if (errorHandler.problemsOccurred()) {
				throw new Exception("ErrorConstGlobals.XML_SCHEMA_ERROR " + xsdFileName + " " + errorHandler.getMessage());
			} else {
				return xmlDocument;
			}
		} catch (ParserConfigurationException e) {
			throw new Exception("ErrorConstException.XML_PROCESSING " + e.getClass().getSimpleName() + " " + e.getMessage(), e);
		} catch (SAXException e) {
			throw new Exception("ErrorConstException.XML_PROCESSING " + e.getClass().getSimpleName() + " " + e.getMessage(), e);
		} catch (IOException e) {
			throw new Exception("ErrorConstException.XML_PROCESSING " + e.getClass().getSimpleName() + " " + e.getMessage(), e);
		}
	}

	/**
	 * The Class ParseErrorHandler.
	 */
	private static class ParseErrorHandler implements ErrorHandler {

		/** The warnings. */
		ArrayList<SAXParseException> warnings = null;

		/** The errors. */
		ArrayList<SAXParseException> errors = null;

		/** The fatal errors. */
		ArrayList<SAXParseException> fatalErrors = null;

		/** The problems. */
		boolean problems = false;

		/**
		 * Problems occurred.
		 *
		 * @return true, if problems occurred
		 */
		public boolean problemsOccurred() {
			return problems;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
		 */
		@Override
		public void warning(SAXParseException exception) throws SAXException {
			problems = true;
			if (warnings == null) {
				warnings = new ArrayList<SAXParseException>();
			}
			warnings.add(exception);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
		 */
		@Override
		public void error(SAXParseException exception) throws SAXException {
			problems = true;
			if (errors == null) {
				errors = new ArrayList<SAXParseException>();
			}
			errors.add(exception);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
		 */
		@Override
		public void fatalError(SAXParseException exception) throws SAXException {
			problems = true;
			if (fatalErrors == null) {
				fatalErrors = new ArrayList<SAXParseException>();
			}
			fatalErrors.add(exception);
		}

		/**
		 * Gets the message.
		 *
		 * @return the message
		 */
		public String getMessage() {
			if (fatalErrors != null && fatalErrors.size() > 0) {
				return fatalErrors.get(0).getMessage();
			} else if (errors != null && errors.size() > 0) {
				return errors.get(0).getMessage();
			} else if (warnings != null && warnings.size() > 0) {
				return warnings.get(0).getMessage();
			} else {
				return "No ParserErrors occured";
			}
		}
	}

	/**
	 * Convert xml to string.
	 *
	 * @param pDocument
	 *            the p document
	 * @param encoding
	 *            the encoding
	 * @return the string
	 * @throws Exception
	 *             the exception
	 */
	public static String convertXML2String(Document pDocument, String encoding) throws Exception {
		TransformerFactory transformerFactory = null;
		Transformer transformer = null;
		DOMSource domSource = null;
		StringWriter writer = new java.io.StringWriter();
		StreamResult result = null;

		try {
			transformerFactory = TransformerFactory.newInstance();
			if (transformerFactory == null) {
				throw new Exception("TransformerFactory error");
			}

			transformer = transformerFactory.newTransformer();
			if (transformer == null) {
				throw new Exception("Transformer error");
			}

			if (encoding != null) {
				transformer.setOutputProperty(OutputKeys.ENCODING, encoding);
			} else {
				transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			}

			domSource = new DOMSource(pDocument);
			result = new StreamResult(writer);

			transformer.transform(domSource, result);

			return writer.toString();
		} catch (TransformerFactoryConfigurationError e) {
			throw new Exception("TransformerFactoryConfigurationError", e);
		} catch (TransformerConfigurationException e) {
			throw new Exception("TransformerConfigurationException", e);
		} catch (TransformerException e) {
			throw new Exception("TransformerException", e);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException ex) {
				throw new Exception("IO error", ex);
			}
		}
	}

	/**
	 * Convert xml to byte array.
	 *
	 * @param pDocument
	 *            the p document
	 * @param encoding
	 *            the encoding
	 * @return the byte[]
	 * @throws Exception
	 *             the exception
	 */
	public static byte[] convertXML2ByteArray(Node pDocument, String encoding) throws Exception {
		TransformerFactory transformerFactory = null;
		Transformer transformer = null;
		DOMSource domSource = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		StreamResult result = null;

		try {
			transformerFactory = TransformerFactory.newInstance();
			if (transformerFactory == null) {
				throw new Exception("TransformerFactory error");
			}

			transformer = transformerFactory.newTransformer();
			if (transformer == null) {
				throw new Exception("Transformer error");
			}

			if (encoding != null) {
				transformer.setOutputProperty(OutputKeys.ENCODING, encoding);
			} else {
				transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			}

			domSource = new DOMSource(pDocument);
			result = new StreamResult(outputStream);

			transformer.transform(domSource, result);

			return outputStream.toByteArray();
		} catch (TransformerFactoryConfigurationError e) {
			throw new Exception("TransformerFactoryConfigurationError", e);
		} catch (TransformerConfigurationException e) {
			throw new Exception("TransformerConfigurationException", e);
		} catch (TransformerException e) {
			throw new Exception("TransformerException", e);
		} finally {
			try {
				if (outputStream != null) {
					outputStream.close();
				}
			} catch (IOException ex) {
				throw new Exception("IO error", ex);
			}
		}
	}

	// public static void parseXMLDataAndXSDVerifyBySAX(byte[] pUploadXmlDocumentArray, UploadXMLContentHandler pContentHandler) throws Exception, Exception {
	// try {
	// if (pUploadXmlDocumentArray == null || pUploadXmlDocumentArray.length == 0) {
	// throw new Exception(ErrorConstAtsGlobals.UPLOAD_XML_EMPTY);
	// }
	// if (pUploadXmlDocumentArray[0] != "<".getBytes("UTF-8")[0]) {
	// throw new Exception(ErrorConstAtsGlobals.INVALID_XML_DATA, "XML data is invalid");
	// }
	//
	// SAXParser parser = new SAXParser();
	// String schemaFileName = pContentHandler.getSchemaFileName();
	// if (schemaFileName != null) {
	// URI schemaFileURI = Utilities.getResource(schemaFileName);
	// if (schemaFileURI == null) {
	// throw new Exception("XML-Schema was not found: " + schemaFileName);
	// }
	// parser.setFeature("http://xml.org/sax/features/validation", true);
	// parser.setFeature("http://apache.org/xml/features/validation/schema", true);
	// parser.setFeature("http://apache.org/xml/features/validation/schema-full-checking", true);
	// parser.setProperty("http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation", schemaFileURI.toString());
	// }
	//
	// parser.setContentHandler(pContentHandler);
	//
	// ParseErrorHandler errorHandler = new ParseErrorHandler();
	// parser.setErrorHandler(errorHandler);
	//
	// InputSource inputSource = new InputSource(new ByteArrayInputStream(pUploadXmlDocumentArray));
	// parser.parse(inputSource);
	//
	// if (errorHandler.problemsOccurred()) {
	// throw new Exception(ErrorConstAtsGlobals.XML_SCHEMA_ERROR, pContentHandler.getSchemaFileName(), errorHandler.getMessage());
	// }
	//
	// /*
	// * String parserClass = "org.apache.xerces.parsers.SAXParser"; String validationFeature = "http://xml.org/sax/features/validation"; String schemaFeature =
	// * "http://apache.org/xml/features/validation/schema";
	// *
	// * XMLReader r = XMLReaderFactory.createXMLReader(parserClass); r.setFeature(validationFeature,true); r.setFeature(schemaFeature,true);
	// *
	// * InputSource inputSourcSAX = new InputSource(pDataStream);
	// * if (byteEncoding != null) {
	// * inputSourcSAX.setEncoding(byteEncoding);
	// * }
	// * ParseErrorHandler errorHandlerSax = new ParseErrorHandler();
	// * r.setErrorHandler(errorHandlerSax); r.parse(inputSourcSAX);
	// */
	// } catch (SAXException e) {
	// logger.error(e.getClass().getSimpleName(), e);
	// throw new Exception(ErrorConstException.XML_PROCESSING, e.getClass().getSimpleName(), e.getMessage());
	// } catch (IOException e) {
	// logger.error(e.getClass().getSimpleName() + " while XML processing", e);
	// throw new Exception(ErrorConstException.XML_PROCESSING, e.getClass().getSimpleName(), e.getMessage());
	// }
	// }

	/**
	 * Creates the root tag node.
	 *
	 * @param baseDocument
	 *            the base document
	 * @param rootTagName
	 *            the root tag name
	 * @return the element
	 */
	public static Element createRootTagNode(Document baseDocument, String rootTagName) {
		Element newNode = baseDocument.createElement(rootTagName);
		baseDocument.appendChild(newNode);
		return newNode;
	}

	/**
	 * Append node.
	 *
	 * @param baseNode
	 *            the base node
	 * @param tagName
	 *            the tag name
	 * @return the element
	 */
	public static Element appendNode(Node baseNode, String tagName) {
		Element newNode = baseNode.getOwnerDocument().createElement(tagName);
		baseNode.appendChild(newNode);
		return newNode;
	}

	/**
	 * Append text value node.
	 *
	 * @param baseNode
	 *            the base node
	 * @param tagName
	 *            the tag name
	 * @param tagValue
	 *            the tag value
	 * @return the element
	 */
	public static Element appendTextValueNode(Node baseNode, String tagName, int tagValue) {
		return appendTextValueNode(baseNode, tagName, Integer.toString(tagValue));
	}

	/**
	 * Append text value node.
	 *
	 * @param baseNode
	 *            the base node
	 * @param tagName
	 *            the tag name
	 * @param tagValue
	 *            the tag value
	 * @return the element
	 */
	public static Element appendTextValueNode(Node baseNode, String tagName, double tagValue) {
		return appendTextValueNode(baseNode, tagName, Double.toString(tagValue));
	}

	/**
	 * Append text value node.
	 *
	 * @param baseNode
	 *            the base node
	 * @param tagName
	 *            the tag name
	 * @param tagValue
	 *            the tag value
	 * @return the element
	 */
	public static Element appendTextValueNode(Node baseNode, String tagName, String tagValue) {
		Element newNode = appendNode(baseNode, tagName);
		if (tagValue == null) {
			newNode.appendChild(baseNode.getOwnerDocument().createTextNode("<null>"));
		} else {
			newNode.appendChild(baseNode.getOwnerDocument().createTextNode(tagValue));
		}
		return newNode;
	}

	/**
	 * Append attribute.
	 *
	 * @param baseNode
	 *            the base node
	 * @param attributeName
	 *            the attribute name
	 * @param attributeValue
	 *            the attribute value
	 */
	public static void appendAttribute(Element baseNode, String attributeName, boolean attributeValue) {
		appendAttribute(baseNode, attributeName, attributeValue ? "true" : "false");
	}

	/**
	 * Append attribute.
	 *
	 * @param baseNode
	 *            the base node
	 * @param attributeName
	 *            the attribute name
	 * @param attributeValue
	 *            the attribute value
	 */
	public static void appendAttribute(Element baseNode, String attributeName, int attributeValue) {
		appendAttribute(baseNode, attributeName, Integer.toString(attributeValue));
	}

	/**
	 * Append attribute.
	 *
	 * @param baseNode
	 *            the base node
	 * @param attributeName
	 *            the attribute name
	 * @param attributeValue
	 *            the attribute value
	 */
	public static void appendAttribute(Element baseNode, String attributeName, String attributeValue) {
		Attr typeAttribute = baseNode.getOwnerDocument().createAttribute(attributeName);
		if (attributeValue == null) {
			typeAttribute.setNodeValue("<null>");
		} else {
			typeAttribute.setNodeValue(attributeValue);
		}
		baseNode.setAttributeNode(typeAttribute);
	}

	/**
	 * Removes the attribute.
	 *
	 * @param baseNode
	 *            the base node
	 * @param attributeName
	 *            the attribute name
	 */
	public static void removeAttribute(Element baseNode, String attributeName) {
		baseNode.getAttributes().removeNamedItem(attributeName);
	}

	/**
	 * Adds the comment to document.
	 *
	 * @param pDocument
	 *            the p document
	 * @param pComment
	 *            the p comment
	 * @return the document
	 */
	public static Document addCommentToDocument(Document pDocument, String pComment) {
		pDocument.getFirstChild().appendChild(pDocument.createComment(pComment));
		return pDocument;
	}

	/**
	 * Gets the encoding of xml byte array.
	 *
	 * @param xmlData
	 *            the xml data
	 * @return the encoding of xml byte array
	 * @throws Exception
	 *             the exception
	 */
	public static String getEncodingOfXmlByteArray(byte[] xmlData) throws Exception {
		String encodingAttributeName = "ENCODING";
		try {
			String first50CharactersInUtf8UpperCase = new String(xmlData, "UTF-8").substring(0, 50).toUpperCase();
			if (first50CharactersInUtf8UpperCase.contains(encodingAttributeName)) {
				int encodingstart = first50CharactersInUtf8UpperCase.indexOf(encodingAttributeName) + encodingAttributeName.length();

				int contentStartEinfach = first50CharactersInUtf8UpperCase.indexOf("'", encodingstart);
				int contentStartDoppelt = first50CharactersInUtf8UpperCase.indexOf("\"", encodingstart);
				int contentStart = Math.min(contentStartEinfach, contentStartDoppelt);
				if (contentStartEinfach < 0) {
					contentStart = contentStartDoppelt;
				}
				if (contentStart < 0) {
					throw new Exception("XmlByteArray-Encoding nicht ermittelbar");
				}
				contentStart = contentStart + 1;

				int contentEndSingle = first50CharactersInUtf8UpperCase.indexOf("'", contentStart);
				int contentEndDouble = first50CharactersInUtf8UpperCase.indexOf("\"", contentStart);
				int contentEnd = Math.min(contentEndSingle, contentEndDouble);
				if (contentEndSingle < 0) {
					contentEnd = contentEndDouble;
				}
				if (contentEnd < 0) {
					throw new Exception("XmlByteArray-Encoding nicht ermittelbar");
				}

				String encodingString = first50CharactersInUtf8UpperCase.substring(contentStart, contentEnd);
				return encodingString;
			} else {
				throw new Exception("XmlByteArray-Encoding nicht ermittelbar");
			}
		} catch (UnsupportedEncodingException e) {
			throw new Exception("XmlByteArray-Encoding nicht ermittelbar");
		}
	}

	/**
	 * Returns all direct simple text value subnodes and their values for a dataNode
	 *
	 * Example XML: <dataNode> <a>1</a> <b>2</b> <c>3</c> </dataNode> Returns: a=1, b=2, c=3.
	 *
	 * @param dataNode
	 *            the data node
	 * @return the simple values of node
	 */
	public static Map<String, String> getSimpleValuesOfNode(Node dataNode) {
		Map<String, String> returnMap = new HashMap<String, String>();

		NodeList list = dataNode.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node.getFirstChild() != null && node.getFirstChild().getNodeType() == Node.TEXT_NODE && node.getChildNodes().getLength() == 1) {
				returnMap.put(node.getNodeName(), node.getFirstChild().getNodeValue());
			}
		}

		return returnMap;
	}

	/**
	 * Gets the sub nodes.
	 *
	 * @param dataNode
	 *            the data node
	 * @return the sub nodes
	 */
	public static List<Node> getSubNodes(Node dataNode) {
		List<Node> returnList = new ArrayList<Node>();

		NodeList list = dataNode.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			returnList.add(node);
		}

		return returnList;
	}

	/**
	 * Gets the nodenames of childs.
	 *
	 * @param dataNode
	 *            the data node
	 * @return the nodenames of childs
	 */
	public static List<String> getNodenamesOfChilds(Node dataNode) {
		List<String> returnList = new ArrayList<String>();

		NodeList list = dataNode.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node.getNodeType() != Node.TEXT_NODE) {
				returnList.add(node.getNodeName());
			}
		}

		return returnList;
	}

	public static String removeFrameTag(String data) throws Exception {
		if (Utilities.isEmpty(data)) {
			return data;
		} else {
			String changedData = data.trim();
			if (changedData.startsWith("<") && changedData.endsWith(">")) {
				Document xmlDocument = parseXmlString(changedData);
				Node rootNode = getRootNodeFromDocument(xmlDocument);
				List<Node> childNodes = getChildNodes(rootNode);
				if (childNodes.size() > 0) {
					return getNodeAsString(childNodes.get(0), "UTF-8", true);
				} else {
					return getNodeValue(rootNode);
				}
			} else {
				return data;
			}
		}
	}

	public static Node getRootNodeFromDocument(Document document) {
		return document.getDocumentElement();
	}

	public static List<Node> getChildNodes(Node dataNode) {
		List<Node> returnList = new ArrayList<Node>();

		NodeList list = dataNode.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node.getNodeType() != Node.TEXT_NODE) {
				returnList.add(node);
			}
		}

		return returnList;
	}

	public static String formatXmlDocument(Document xmlDocument, String encoding, boolean throwExceptionOnError) throws Exception {
		try {
			StringBuilder result = new StringBuilder();
			result.append("<?xml version=\"");
			result.append(xmlDocument.getXmlVersion());
			result.append("\" encoding=\"");
			result.append(encoding.toUpperCase());
			result.append("\" standalone=\"");
			result.append(xmlDocument.getXmlStandalone() ? "yes" : "no");
			result.append("\"?>\n");

			for (int i = 0; i < xmlDocument.getChildNodes().getLength(); i++) {
				Node xmlNode = xmlDocument.getChildNodes().item(i);
				result.append(formatXmlNode(xmlNode, 0));
			}
			return result.toString();
		} catch (Exception e) {
			if (throwExceptionOnError) {
				throw e;
			} else {
				return null;
			}
		}
	}

	public static String formatXmlDocument(Document xmlDocument, boolean throwExceptionOnError) throws Exception {
		try {
			StringBuilder result = new StringBuilder();
			result.append("<?xml version=\"");
			result.append(xmlDocument.getXmlVersion());
			result.append("\" encoding=\"");
			result.append(xmlDocument.getXmlEncoding() == null ? "UTF-8" : xmlDocument.getXmlEncoding());
			result.append("\" standalone=\"");
			result.append(xmlDocument.getXmlStandalone() ? "yes" : "no");
			result.append("\"?>\n");

			for (int i = 0; i < xmlDocument.getChildNodes().getLength(); i++) {
				Node xmlNode = xmlDocument.getChildNodes().item(i);
				result.append(formatXmlNode(xmlNode, 0));
			}
			return result.toString();
		} catch (Exception e) {
			if (throwExceptionOnError) {
				throw e;
			} else {
				return null;
			}
		}
	}

	public static String formatXmlNode(Node xmlNode, int indentationLevel) {
		if (xmlNode.getNodeType() == Node.TEXT_NODE) {
			if (Utilities.isBlank(xmlNode.getNodeValue())) {
				return "";
			} else {
				return xmlNode.getNodeValue().replace("&", "&amp;").replace("<", "&lt;");
			}
		} else if (xmlNode.getNodeType() == Node.COMMENT_NODE) {
			if (Utilities.isBlank(xmlNode.getNodeValue())) {
				return "";
			} else {
				StringBuilder result = new StringBuilder();
				result.append(TextUtilities.repeatString("\t", indentationLevel));
				result.append("<!--");
				result.append(xmlNode.getNodeValue().replace("&", "&amp;").replace("<", "&lt;"));
				result.append("-->\n");
				return result.toString();
			}
		} else if (xmlNode.getNodeType() == Node.DOCUMENT_TYPE_NODE) {
			StringBuilder result = new StringBuilder();
			result.append(TextUtilities.repeatString("\t", indentationLevel));
			result.append("<!DOCTYPE ");
			result.append(((DocumentType) xmlNode).getName());
			result.append(" PUBLIC \"");
			result.append(((DocumentType) xmlNode).getPublicId());
			result.append("\" \"");
			result.append(((DocumentType) xmlNode).getSystemId());
			result.append("\">\n");
			return result.toString();
		} else if (xmlNode.getChildNodes().getLength() == 1 && xmlNode.getChildNodes().item(0).getNodeType() == Node.TEXT_NODE) {
			if (Utilities.isBlank(xmlNode.getChildNodes().item(0).getNodeValue())) {
				StringBuilder result = new StringBuilder();
				result.append(TextUtilities.repeatString("\t", indentationLevel));
				result.append("<");
				result.append(xmlNode.getNodeName());
				result.append(formatAttributes(xmlNode, indentationLevel));
				result.append(" />\n");
				return result.toString();
			} else {
				StringBuilder result = new StringBuilder();
				result.append(TextUtilities.repeatString("\t", indentationLevel));
				result.append("<");
				result.append(xmlNode.getNodeName());
				result.append(formatAttributes(xmlNode, indentationLevel));
				result.append(">");
				result.append(xmlNode.getChildNodes().item(0).getNodeValue().replace("&", "&amp;").replace("<", "&lt;"));
				result.append("</");
				result.append(xmlNode.getNodeName());
				result.append(">\n");
				return result.toString();
			}
		} else {
			StringBuilder innerXml = new StringBuilder();
			if (xmlNode.getChildNodes() != null && xmlNode.getChildNodes().getLength() > 0) {
				for (int nodeIndex = 0; nodeIndex < xmlNode.getChildNodes().getLength(); nodeIndex++) {
					Node xmlSubNode = xmlNode.getChildNodes().item(nodeIndex);
					if (xmlSubNode.getNodeType() == Node.TEXT_NODE) {
						if (Utilities.isNotBlank(xmlSubNode.getNodeValue())) {
							innerXml.append(xmlSubNode.getNodeValue().replace("&", "&amp;").replace("<", "&lt;"));
						}
					} else {
						innerXml.append(formatXmlNode(xmlSubNode, indentationLevel + 1));
					}
				}
			}

			StringBuilder result = new StringBuilder();
			result.append(TextUtilities.repeatString("\t", indentationLevel));
			result.append("<");
			result.append(xmlNode.getNodeName());
			result.append(formatAttributes(xmlNode, indentationLevel));
			if (innerXml.length() == 0) {
				result.append(" />\n");
			} else {
				result.append(">\n");
				result.append(innerXml);
				result.append(TextUtilities.repeatString("\t", indentationLevel));
				result.append("</");
				result.append(xmlNode.getNodeName());
				result.append(">\n");
			}
			return result.toString();
		}
	}

	private static String formatAttributes(Node xmlNode, int indentationLevel) {
		StringBuilder result = new StringBuilder();
		if (xmlNode.getAttributes() != null && xmlNode.getAttributes().getLength() > 0) {
			if (xmlNode.getAttributes().getLength() <= 5) {
				for (int attributeIndex = 0; attributeIndex < xmlNode.getAttributes().getLength(); attributeIndex++) {
					Node attributeNode = xmlNode.getAttributes().item(attributeIndex);
					result.append(" ");
					result.append(attributeNode.getNodeName());
					result.append("=\"");
					result.append(attributeNode.getNodeValue().replaceAll("\"", "&quot;").replaceAll("<", "&lt;").replaceAll("&", "&amp;"));
					result.append("\"");
				}
			} else {
				result.append("\n");
				for (int attributeIndex = 0; attributeIndex < xmlNode.getAttributes().getLength(); attributeIndex++) {
					if (attributeIndex > 0) {
						result.append("\n");
					}
					Node attributeNode = xmlNode.getAttributes().item(attributeIndex);
					result.append(TextUtilities.repeatString("\t", indentationLevel + 1));
					result.append(attributeNode.getNodeName());
					result.append("=\"");
					result.append(attributeNode.getNodeValue().replaceAll("\"", "&quot;").replaceAll("<", "&lt;").replaceAll("&", "&amp;"));
					result.append("\"");
				}
			}
		}
		return result.toString();
	}
}
