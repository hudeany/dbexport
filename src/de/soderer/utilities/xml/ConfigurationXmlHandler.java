package de.soderer.utilities.xml;

import java.io.InputStream;
import java.util.Hashtable;
import java.util.Map;

import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ConfigurationXmlHandler extends DefaultHandler {

	private static final String TAGNAME_ROOT = "Configuration";
	private static final String TAGNAME_ENTRY = "Entry";
	private static final String ATTRIBUTENAME_ENTRY_NAME = "name";
	private static final String ATTRIBUTENAME_ENTRY_value = "value";

	private Map<String, String> values = new Hashtable<String, String>();

	private boolean foundRootTag = false;

	public ConfigurationXmlHandler(InputStream inputStream) throws Exception {
		super();
		try {
			SAXParserFactory.newInstance().newSAXParser().parse(new InputSource(inputStream), this);
		} catch (Exception ex) {
			throw new Exception("Error while parsing XmlConfiguration", ex);
		}
	}

	public Map<String, String> getConfigurationValueMap() {
		return values;
	}

	@Override
	public void startElement(String namespaceURI, String localName, String qName, Attributes attributes) throws SAXException {
		if (!foundRootTag) {
			if (!qName.equals(TAGNAME_ROOT)) {
				throw new SAXException("Invalid RootTag in XmlConfiguration: " + qName);
			} else {
				foundRootTag = true;
			}
		} else if (qName.equals(TAGNAME_ENTRY)) {
			values.put(attributes.getValue(ATTRIBUTENAME_ENTRY_NAME), attributes.getValue(ATTRIBUTENAME_ENTRY_value));
		}
	}

	@Override
	public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
		if (qName.equals(TAGNAME_ROOT)) {
			// do noting
		} else if (qName.equals(TAGNAME_ENTRY)) {
			// do noting
		}
	}
}
