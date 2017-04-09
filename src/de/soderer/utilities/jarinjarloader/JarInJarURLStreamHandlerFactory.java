package de.soderer.utilities.jarinjarloader;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class JarInJarURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private ClassLoader classLoader;
	private URLStreamHandlerFactory chainFac;

	public JarInJarURLStreamHandlerFactory(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public URLStreamHandler createURLStreamHandler(String protocol) {
		if ("rsrc".equals(protocol)) {
			return new JarInJarURLStreamHandler(classLoader);
		} else if (chainFac != null) {
			return chainFac.createURLStreamHandler(protocol);
		} else {
			return null;
		}
	}

	public void setURLStreamHandlerFactory(URLStreamHandlerFactory fac) {
		chainFac = fac;
	}
}
