package de.soderer.dbexport.utilities.jarinjarloader;

import java.io.IOException;
import java.net.URL;

/**
 * Stream handler for the "rsrc:" URL protocol, which addresses resources inside the running application jar.
 */
public class JarInJarURLStreamHandler extends java.net.URLStreamHandler {
	private final ClassLoader classLoader;

	/**
	 * Creates a stream handler, that reads the resources via the given class loader.
	 *
	 * @param classLoader class loader to read the resources with
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
