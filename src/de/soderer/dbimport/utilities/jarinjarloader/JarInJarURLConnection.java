package de.soderer.dbimport.utilities.jarinjarloader;

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
		this.classLoader= classLoader;
	}

	@Override
	public void connect() throws IOException {
		// nothing to do
	}

	@Override
	public InputStream getInputStream() throws IOException {
		final String file = URLDecoder.decode(url.getFile(), StandardCharsets.UTF_8.name());
		final InputStream result = classLoader.getResourceAsStream(file);
		if (result == null) {
			throw new MalformedURLException("Could not open InputStream for URL '" + url + "'");
		} else {
			return result;
		}
	}
}
