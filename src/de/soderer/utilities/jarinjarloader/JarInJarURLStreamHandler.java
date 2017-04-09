package de.soderer.utilities.jarinjarloader;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

public class JarInJarURLStreamHandler extends java.net.URLStreamHandler {
	private ClassLoader classLoader;

	public JarInJarURLStreamHandler(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	protected URLConnection openConnection(URL url) throws IOException {
		return new JarInJarURLConnection(url, classLoader);
	}

	@Override
	protected void parseURL(URL url, String spec, int start, int limit) {
		String file;
		if (spec.startsWith("rsrc:")) {
			file = spec.substring(5);
		} else if (url.getFile().equals("./")) {
			file = spec;
		} else if (url.getFile().endsWith("/")) {
			file = url.getFile() + spec;
		} else {
			file = spec;
		}
		setURL(url, "rsrc", "", -1, null, null, file, null, null);
	}
}
