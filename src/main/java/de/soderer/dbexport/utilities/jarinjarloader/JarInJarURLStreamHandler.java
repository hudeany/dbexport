package de.soderer.dbexport.utilities.jarinjarloader;

import java.io.IOException;
import java.net.URL;

/**
 * URLStreamHandler for the "rsrc:" protocol, which opens a {@link JarInJarURLConnection}.
 */
public class JarInJarURLStreamHandler extends java.net.URLStreamHandler {
	private final ClassLoader classLoader;

	/**
	 * Creates the handler.
	 *
	 * @param classLoader class loader used to read the addressed resources
	 */
	public JarInJarURLStreamHandler(final ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	protected java.net.URLConnection openConnection(final URL url) throws IOException {
		return new JarInJarURLConnection(url, classLoader);
	}

	@Override
	protected void parseURL(final URL url, final String spec, final int start, final int limit) {
		String file;
		if (spec.startsWith("rsrc:")) {
			file = spec.substring(5);
		} else if ("./".equals(url.getFile())) {
			file = spec;
		} else if (url.getFile().endsWith("/")) {
			file = url.getFile() + spec;
		} else if ("#runtime".equals(spec)) {
			file = url.getFile();
		} else {
			file = spec;
		}
		setURL(url, "rsrc", "", -1, null, null, file, null, null);
	}
}
