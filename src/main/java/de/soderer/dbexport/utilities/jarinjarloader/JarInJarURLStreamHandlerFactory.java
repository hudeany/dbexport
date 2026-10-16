package de.soderer.dbexport.utilities.jarinjarloader;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * Factory, which provides the stream handler for the "rsrc:" URL protocol. All other protocols are left to the
 * default handlers.
 */
public class JarInJarURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private final ClassLoader classLoader;

	/**
	 * Creates a factory, whose stream handlers read the resources via the given class loader.
	 *
	 * @param classLoader class loader to read the resources with
	 */
	public JarInJarURLStreamHandlerFactory(final ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public URLStreamHandler createURLStreamHandler(final String protocol) {
		if ("rsrc".equals(protocol)) {
			return new JarInJarURLStreamHandler(classLoader);
		} else {
			return null;
		}
	}
}
