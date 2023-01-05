package de.soderer.utilities.jarinjarloader;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class JarInJarURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private final ClassLoader classLoader;

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
