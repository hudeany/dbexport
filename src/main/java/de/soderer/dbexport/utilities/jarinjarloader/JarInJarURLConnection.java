package de.soderer.dbexport.utilities.jarinjarloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class JarInJarURLConnection extends URLConnection {
	private final ClassLoader classLoader;

	public JarInJarURLConnection(final URL url, final ClassLoader classLoader) {
		super(url);
		this.classLoader = classLoader;
	}

	@Override
	public void connect() throws IOException {
		// nothing to do
	}

	@Override
	public InputStream getInputStream() throws IOException {
		// Escape literal '+' to "%2B" first: URLDecoder is built for form-urlencoded data and
		// would otherwise turn a literal '+' in the resource path into a space.
		final String file = URLDecoder.decode(url.getFile().replace("+", "%2B"), StandardCharsets.UTF_8);
		final InputStream result = classLoader.getResourceAsStream(file);
		if (result == null) {
			throw new MalformedURLException("Could not open InputStream for URL '" + url + "'");
		} else {
			return result;
		}
	}
}
