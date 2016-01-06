package de.soderer.utilities.jarinjarloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;

public class JarInJarURLConnection extends URLConnection {
	private ClassLoader classLoader;

	public JarInJarURLConnection(URL url, ClassLoader classLoader) {
		super(url);
		this.classLoader = classLoader;
	}

	@Override
	public void connect() throws IOException {
		// do nothing
	}

	@Override
	public InputStream getInputStream() throws IOException {
		String file = URLDecoder.decode(url.getFile(), "UTF-8");
		InputStream result = classLoader.getResourceAsStream(file);
		if (result == null) {
			throw new MalformedURLException("Could not open InputStream for URL '" + url + "'");
		}
		return result;
	}
}
