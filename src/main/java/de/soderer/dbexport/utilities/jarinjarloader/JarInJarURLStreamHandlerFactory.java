package de.soderer.dbexport.utilities.jarinjarloader;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * Creates a {@link JarInJarURLStreamHandler} for the "rsrc:" protocol and leaves all other protocols
 * to the default handlers.
 */
public class JarInJarURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private final ClassLoader classLoader;

	/**
	 * Creates the factory.
	 *
	 * @param classLoader class loader used to read the resources addressed by "rsrc:" URLs
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
