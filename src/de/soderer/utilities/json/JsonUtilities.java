package de.soderer.utilities.json;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.json.schema.JsonSchema;

public class JsonUtilities {
	public static JsonObject convertXmlDocument(final Document xmlDocument, final boolean throwExceptionOnError) throws Exception {
		try {
			final JsonObject jsonObject = new JsonObject();
			jsonObject.add(xmlDocument.getChildNodes().item(0).getNodeName(), convertXmlNode(xmlDocument.getChildNodes().item(0)));
			return jsonObject;
		} catch (final Exception e) {
			if (throwExceptionOnError) {
				throw new Exception("Invalid data", e);
			} else {
				return null;
			}
		}
	}

	public static JsonObject convertXmlNode(final Node xmlNode) {
		final JsonObject jsonObject = new JsonObject();
		if (xmlNode.getAttributes() != null && xmlNode.getAttributes().getLength() > 0) {
			for (int attributeIndex = 0; attributeIndex < xmlNode.getAttributes().getLength(); attributeIndex++) {
				final Node attributeNode = xmlNode.getAttributes().item(attributeIndex);
				jsonObject.add(attributeNode.getNodeName(), attributeNode.getNodeValue());
			}
		}
		if (xmlNode.getChildNodes() != null && xmlNode.getChildNodes().getLength() > 0) {
			for (int i = 0; i < xmlNode.getChildNodes().getLength(); i++) {
				final Node childNode = xmlNode.getChildNodes().item(i);
				if (childNode.getNodeType() == Node.TEXT_NODE) {
					if (Utilities.isNotBlank(childNode.getNodeValue())) {
						jsonObject.add("text", childNode.getNodeValue());
					}
				} else if (childNode.getNodeType() == Node.COMMENT_NODE) {
					// do nothing
				} else if (childNode.getChildNodes().getLength() == 1 && childNode.getChildNodes().item(0).getNodeType() == Node.TEXT_NODE) {
					// only one textnode under this node
					jsonObject.add(childNode.getNodeName(), childNode.getChildNodes().item(0).getNodeValue());
				} else {
					final Node xmlSubNode = childNode;
					final JsonObject nodeJsonObject = convertXmlNode(xmlSubNode);
					if (nodeJsonObject != null) {
						jsonObject.add(xmlSubNode.getNodeName(), nodeJsonObject);
					}
				}
			}
		}
		return jsonObject;
	}

	public static Document convertToXmlDocument(final JsonNode jsonNode, final boolean useAttributes) throws Exception {
		try {
			final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			final Document xmlDocument = documentBuilder.newDocument();
			xmlDocument.setXmlStandalone(true);
			List<Node> mainNodes;
			if (jsonNode.isJsonObject()) {
				mainNodes = convertToXmlNodes((JsonObject) jsonNode.getValue(), xmlDocument, useAttributes);
				if (mainNodes == null || mainNodes.size() < 1) {
					throw new Exception("No data found");
				} else if (mainNodes.size() == 1) {
					xmlDocument.appendChild(mainNodes.get(0));
				} else {
					final Node rootNode = xmlDocument.createElement("root");
					for (final Node subNode : mainNodes) {
						if (subNode instanceof Attr) {
							rootNode.getAttributes().setNamedItem(subNode);
						} else {
							rootNode.appendChild(subNode);
						}
					}
					xmlDocument.appendChild(rootNode);
				}
			} else if (jsonNode.isJsonArray()) {
				mainNodes = convertToXmlNodes((JsonArray) jsonNode.getValue(), "root", xmlDocument, useAttributes);
				if (mainNodes == null || mainNodes.size() < 1) {
					throw new Exception("No data found");
				} else if (mainNodes.size() == 1) {
					xmlDocument.appendChild(mainNodes.get(0));
				} else {
					final Node rootNode = xmlDocument.createElement("root");
					for (final Node subNode : mainNodes) {
						if (subNode instanceof Attr) {
							rootNode.getAttributes().setNamedItem(subNode);
						} else {
							rootNode.appendChild(subNode);
						}
					}
					xmlDocument.appendChild(rootNode);
				}
			} else if (jsonNode.isNull()) {
				final Node rootNode = xmlDocument.createElement("root");
				rootNode.setTextContent("null");
				xmlDocument.appendChild(rootNode);
			} else {
				final Node rootNode = xmlDocument.createElement("root");
				rootNode.setTextContent(jsonNode.getValue().toString());
				xmlDocument.appendChild(rootNode);
			}

			return xmlDocument;
		} catch (final Exception e) {
			throw new Exception("Invalid data", e);
		}
	}

	public static List<Node> convertToXmlNodes(final JsonObject jsonObject, final Document xmlDocument, final boolean useAttributes) {
		final List<Node> list = new ArrayList<>();

		for (final String key : jsonObject.keySet()) {
			final Object subItem = jsonObject.get(key);
			if (subItem instanceof JsonObject) {
				final Node newNode = xmlDocument.createElement(key);
				list.add(newNode);
				for (final Node subNode : convertToXmlNodes((JsonObject) subItem, xmlDocument, useAttributes)) {
					if (subNode instanceof Attr) {
						newNode.getAttributes().setNamedItem(subNode);
					} else {
						newNode.appendChild(subNode);
					}
				}
			} else if (subItem instanceof JsonArray) {
				for (final Node subNode : convertToXmlNodes((JsonArray) subItem, key, xmlDocument, useAttributes)) {
					list.add(subNode);
				}
			} else if (useAttributes) {
				final Attr newAttr = xmlDocument.createAttribute(key);
				newAttr.setNodeValue(subItem.toString());
				list.add(newAttr);
			} else {
				final Node newNode = xmlDocument.createElement(key);
				list.add(newNode);
				newNode.setTextContent(subItem.toString());
			}
		}

		return list;
	}

	public static List<Node> convertToXmlNodes(final JsonArray jsonArray, final String nodeName, final Document xmlDocument, final boolean useAttributes) {
		final List<Node> list = new ArrayList<>();

		if (jsonArray.size() > 0) {
			for (final Object subItem : jsonArray) {
				if (subItem instanceof JsonObject) {
					final Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					for (final Node subNode : convertToXmlNodes((JsonObject) subItem, xmlDocument, useAttributes)) {
						if (subNode instanceof Attr) {
							newNode.getAttributes().setNamedItem(subNode);
						} else {
							newNode.appendChild(subNode);
						}
					}
				} else if (subItem instanceof JsonArray) {
					final Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					for (final Node subNode : convertToXmlNodes((JsonArray) subItem, nodeName, xmlDocument, useAttributes)) {
						newNode.appendChild(subNode);
					}
				} else {
					final Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					newNode.setTextContent(subItem.toString());
				}
			}
		} else {
			final Node newNode = xmlDocument.createElement(nodeName);
			list.add(newNode);
		}

		return list;
	}

	/**
	 * JsonPath syntax:<br />
	 *	$ : root<br />
	 *	. or / : child separator<br />
	 *	[n] : array operator<br />
	 *<br />
	 * JsonPath example:<br />
	 * 	"$.list.customer[0].name"<br />
	 *
	 * @param jsonReader
	 * @param jsonPath
	 * @throws Exception
	 */
	public static void readUpToJsonPath(final JsonReader jsonReader, String jsonPath) throws Exception {
		if (jsonPath.startsWith("/") || jsonPath.startsWith("$")) {
			jsonPath = jsonPath.substring(1);
		}
		if (jsonPath.endsWith("/")) {
			jsonPath = jsonPath.substring(0, jsonPath.length() - 1);
		}
		jsonPath = "$" + jsonPath.replace("/", ".");

		while (jsonReader.readNextToken() != null && !jsonReader.getCurrentJsonPath().equals(jsonPath)) {
			// Do nothing
		}

		if (!jsonReader.getCurrentJsonPath().equals(jsonPath)) {
			throw new Exception("Path '" + jsonPath + "' is not part of the JSON data");
		}
	}

	public static JsonNode parseJsonDataAndVerifyJsonSchema(final byte[] jsonData, final Charset encoding, final String jsonSchemaFileName) throws Exception {
		JsonSchema jsonSchema;
		try (InputStream jsonSchemaInputStream = new FileInputStream(jsonSchemaFileName)) {
			jsonSchema = new JsonSchema(jsonSchemaInputStream, encoding);
		}
		return jsonSchema.validate(new ByteArrayInputStream(jsonData), encoding);
	}

	public static JsonNode validateJsonSchema(final byte[] jsonData, final Charset encoding) throws Exception {
		JsonSchema jsonSchema;
		try (InputStream jsonSchemaInputStream = JsonSchema.class.getClassLoader().getResourceAsStream("json/JsonSchemaDescriptionDraftV4.json");) {
			jsonSchema = new JsonSchema(jsonSchemaInputStream, encoding);
		}
		return jsonSchema.validate(new ByteArrayInputStream(jsonData), encoding);
	}

	public static JsonNode validateJson(final byte[] jsonData, final Charset encoding) throws Exception {
		try (JsonReader jsonReader = new JsonReader(new ByteArrayInputStream(jsonData), encoding)) {
			return jsonReader.read();
		}
	}

	public static JsonNode validateJson5(final byte[] jsonData, final Charset encoding) throws Exception {
		try (JsonReader jsonReader = new Json5Reader(new ByteArrayInputStream(jsonData), encoding)) {
			return jsonReader.read();
		}
	}
}
