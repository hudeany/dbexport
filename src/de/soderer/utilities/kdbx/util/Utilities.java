package de.soderer.utilities.kdbx.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Utilities {
	private static Pattern HEXADECIMAL_PATTERN = Pattern.compile("\\p{XDigit}+");

	public static boolean isEmpty(final String value) {
		return value == null || value.length() == 0;
	}

	public static boolean isNotEmpty(final String value) {
		return !isEmpty(value);
	}

	public static boolean isEmpty(final Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

	public static boolean isNotEmpty(final Collection<?> collection) {
		return !isEmpty(collection);
	}

	public static boolean isBlank(final String value) {
		return value == null || value.length() == 0 || value.trim().length() == 0;
	}

	public static boolean isNotBlank(final String value) {
		return !isBlank(value);
	}

	public static boolean isEmpty(final char[] value) {
		return value == null || value.length == 0;
	}

	public static boolean isNotEmpty(final char[] value) {
		return !isEmpty(value);
	}

	public static boolean isBlank(final char[] value) {
		if (value == null || value.length == 0) {
			return true;
		} else {
			for (final char character : value) {
				if (!Character.isWhitespace(character)) {
					return false;
				}
			}
			return true;
		}
	}

	public static boolean isNotBlank(final char[] value) {
		return !isBlank(value);
	}

	public static String repeat(final char valueChar, final int count) {
		return repeat(Character.toString(valueChar), count, null);
	}

	public static String repeat(final String value, final int count) {
		return repeat(value, count, null);
	}

	public static String repeat(final String value, final int count, final String separatorString) {
		if (value == null) {
			return null;
		} else if (value.length() == 0 || count == 0) {
			return "";
		} else {
			final StringBuilder returnValue = new StringBuilder();
			for (int i = 0; i < count; i++) {
				if (separatorString != null && returnValue.length() > 0) {
					returnValue.append(separatorString);
				}
				returnValue.append(value);
			}
			return returnValue.toString();
		}
	}

	public static String leftPad(final String value, final int minimumLength) {
		try {
			return String.format("%1$" + minimumLength + "s", value);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return value;
		}
	}

	public static String leftPad(final String value, final int size, final char padChar) {
		if (value == null) {
			return null;
		} else {
			final int padsize = size - value.length();
			if (padsize <= 0) {
				return value;
			} else {
				return repeat(padChar, padsize).concat(value);
			}
		}
	}

	public static byte[] toByteArray(final InputStream inputStream) throws IOException {
		if (inputStream == null) {
			return null;
		} else {
			try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
				copy(inputStream, byteArrayOutputStream);
				return byteArrayOutputStream.toByteArray();
			}
		}
	}

	public static long copy(final InputStream inputStream, final OutputStream outputStream) throws IOException {
		final byte[] buffer = new byte[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputStream.read(buffer)) > -1) {
			outputStream.write(buffer, 0, lengthRead);
			bytesCopied += lengthRead;
		}
		outputStream.flush();
		return bytesCopied;
	}

	private static final Pattern BASE64_PATTERN = Pattern.compile("^@(?=(.{4})*$)[A-Za-z0-9+/]*={0,2}$");

	public static boolean isBase64(final String s) {
		if (Utilities.isNotBlank(s)) {
			return BASE64_PATTERN.matcher(s).matches();
		}
		return false;
	}

	public static String toHexString(final byte[] data) {
		return toHexString(data, "_");
	}

	public static byte[] fromHexString(final String value) {
		if (value == null) {
			return null;
		} else {
			String decodeValue = value;
			if (value.toLowerCase().startsWith("0x")) {
				decodeValue = decodeValue.substring(2);
			}
			if (!HEXADECIMAL_PATTERN.matcher(decodeValue).matches()) {
				throw new RuntimeException("String contains non hexadecimal character: " + value);
			}
			final int length = decodeValue.length();
			final byte[] data = new byte[length / 2];
			for (int i = 0; i < length; i += 2) {
				data[i / 2] = (byte) ((Character.digit(decodeValue.charAt(i), 16) << 4) + Character.digit(decodeValue.charAt(i + 1), 16));
			}
			return data;
		}
	}

	public static byte[] fromHexString(final String value, final boolean ignoreNonHexCharacters) {
		if (value == null) {
			return null;
		} else if (ignoreNonHexCharacters) {
			return fromHexString(value.toLowerCase().replaceAll("[^abcdef0-9]", ""));
		} else {
			return fromHexString(value);
		}
	}

	public static String toHexString(final byte[] data, final String separator) {
		if (data == null || data.length == 0) {
			return "<empty>";
		}
		final StringBuilder buffer = new StringBuilder();
		final char[] chars = "0123456789ABCDEF".toCharArray();
		for (int i = 0; i < data.length; i++) {
			final int value = data[i] & 0xff;
			final char hi = chars[(value & 0xf0) >>> 4];
			final char lo = chars[value & 0x0f];
			buffer.append(hi).append(lo);
			if ((i + 1) < data.length) {
				buffer.append(separator);
			}
		}
		return buffer.toString();
	}

	public static String toByteString(final byte[] data, final String separator) {
		if (data == null || data.length == 0) {
			return "<empty>";
		}
		final StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			buffer.append(data[i]);
			if ((i + 1) < data.length) {
				buffer.append(separator);
			}
		}
		return buffer.toString();
	}

	public static int readLittleEndianIntFromStream(final InputStream inputStream) throws IOException, Exception {
		final byte[] byteBuffer = new byte[4];
		final int readBytes = inputStream.read(byteBuffer);
		if (readBytes == -1) {
			throw new Exception("Cannot read int from stream: End of stream");
		} else if (readBytes != byteBuffer.length) {
			throw new Exception("Cannot read int from stream: Not enough data left");
		}
		return ByteBuffer.wrap(byteBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}

	public static short readLittleEndianShortFromStream(final InputStream inputStream) throws IOException, Exception {
		final byte[] byteBuffer = new byte[2];
		final int readBytes = inputStream.read(byteBuffer);
		if (readBytes == -1) {
			throw new Exception("Cannot read int from stream: End of stream");
		} else if (readBytes != byteBuffer.length) {
			throw new Exception("Cannot read int from stream: Not enough data left");
		}
		return ByteBuffer.wrap(byteBuffer).order(ByteOrder.LITTLE_ENDIAN).getShort();
	}

	public static long readLittleEndianValueFromByteArray(final byte[] data) {
		switch (data.length) {
			case 1:
				return data[0];
			case 2:
				return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort();
			case 4:
				return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
			case 8:
				return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
			default:
				throw new RuntimeException("Invalid data length for numeric value");
		}
	}

	public static byte[] toBytes(final char[] chars) {
		final CharBuffer charBuffer = CharBuffer.wrap(chars);
		final ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
		final byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
		Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
		return bytes;
	}

	/**
	 * AES Key Derivation
	 */
	public final static byte[] deriveKeyByAES(final byte[] salt, final long rounds, final byte[] originalKey) {
		byte[] result = new byte[originalKey.length];
		System.arraycopy(originalKey, 0, result, 0, result.length);
		try {
			final Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(salt, "AES"));
			for (long i = 0; i < rounds; i++) {
				result = cipher.doFinal(result);
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public static byte[] concatArrays(final byte[] array1, final byte[] array2) {
		final byte[] result = new byte[array1.length + array2.length];
		int writeIndex = 0;
		for (final byte byte1 : array1) {
			result[writeIndex++] = byte1;
		}
		for (final byte byte2 : array2) {
			result[writeIndex++] = byte2;
		}
		return result;
	}

	public static short readShortFromLittleEndianBytes(final byte[] dataBytes) {
		if (dataBytes == null || dataBytes.length != 2) {
			throw new RuntimeException("Invalid data bytes for short value: 2 bytes expected");
		} else {
			return ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
		}
	}

	public static byte[] getLittleEndianBytes(final short value) {
		return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(value).array();
	}

	public static int readIntFromLittleEndianBytes(final byte[] dataBytes) {
		if (dataBytes == null || dataBytes.length != 4) {
			throw new RuntimeException("Invalid data bytes for int value: 4 bytes expected");
		} else {
			return ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
		}
	}

	public static byte[] getLittleEndianBytes(final int value) {
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
	}

	public static long readLongFromLittleEndianBytes(final byte[] dataBytes) {
		if (dataBytes == null || dataBytes.length != 8) {
			throw new RuntimeException("Invalid data bytes for long value: 8 bytes expected");
		} else {
			return ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
		}
	}

	public static byte[] getLittleEndianBytes(final long value) {
		return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
	}

	public static boolean isXmlDocument(final byte[] data) {
		if (data == null || data.length < 6) {
			return false;
		} else {
			return "<?xml ".equals(new String(Arrays.copyOfRange(data, 0, 6), StandardCharsets.UTF_8).toLowerCase());
		}
	}

	public static Document parseXmlFile(final byte[] xmlData) {
		try (BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(xmlData))) {
			final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			final Document document = documentBuilder.parse(inputStream);
			return document;
		} catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	public static String getNodeValue(final Node pNode) {
		if (pNode.getNodeValue() != null) {
			return pNode.getNodeValue();
		} else if (pNode.getFirstChild() != null) {
			return getNodeValue(pNode.getFirstChild());
		} else {
			return null;
		}
	}

	public static String getAttributeValue(final Node pNode, final String pAttributeName) {
		String returnString = null;

		final NamedNodeMap attributes = pNode.getAttributes();
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

	public static Map<String, Node> getChildNodesMap(final Node dataNode) {
		final Map<String, Node> childNodes = new LinkedHashMap<>();
		final NodeList childNodesList = dataNode.getChildNodes();
		for (int i = 0; i < childNodesList.getLength(); i++) {
			final Node childNode = childNodesList.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				childNodes.put(childNode.getNodeName(), childNode);
			}
		}

		return childNodes;
	}

	public static Document createNewDocument() throws ParserConfigurationException {
		final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		final Document document = documentBuilder.newDocument();
		return document;
	}

	public static Element appendNode(final Document document, final String tagName) {
		final Element newNode = document.createElement(tagName);
		document.appendChild(newNode);
		return newNode;
	}

	public static Element appendNode(final Node baseNode, final String tagName) {
		final Element newNode = baseNode.getOwnerDocument().createElement(tagName);
		baseNode.appendChild(newNode);
		return newNode;
	}

	public static Node appendTextValueNode(final Node baseNode, final String tagName, final String tagValue) {
		final Node newNode = appendNode(baseNode, tagName);
		if (tagValue != null) {
			newNode.appendChild(baseNode.getOwnerDocument().createTextNode(tagValue));
		}
		return newNode;
	}

	public static void appendAttribute(final Element baseNode, final String attributeName, final String attributeValue) {
		final Attr typeAttribute = baseNode.getOwnerDocument().createAttribute(attributeName);
		if (attributeValue != null) {
			typeAttribute.setNodeValue(attributeValue);
		}
		baseNode.setAttributeNode(typeAttribute);
	}

	public static byte[] gzip(final byte[] data) throws Exception {
		final ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
		try (final GZIPOutputStream gzipOut = new GZIPOutputStream(bufferStream)) {
			gzipOut.write(data);
		} catch (final IOException e) {
			throw new Exception("GZIP compression failed", e);
		}
		return bufferStream.toByteArray();
	}

	public static byte[] gunzip(final byte[] compressedData) throws Exception {
		try (final GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
			return Utilities.toByteArray(gzipIn);
		} catch (final IOException e) {
			throw new Exception("GZIP decompression failed", e);
		}
	}
}
