package de.soderer.utilities.code;

import java.io.File;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

public class JavaSourceFromString extends SimpleJavaFileObject {
	private final String code;

	public JavaSourceFromString(final String name, final String code) {
		super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
		this.code = code;
	}

	@Override
	public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
		return code;
	}

	public static void main(final String[] args) {
		try {
			final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
			if (javaCompiler == null) {
				throw new Exception("Compiler unavailable");
			}

			final String code = "package de.soderer.test;\n" + "public class SimpleWorker {\n" + "public void runJob() throws Exception {\n" + "System.out.println(\"SimpleJobWorker worked\");\n" + "}\n"
					+ "}\n";

			final JavaSourceFromString javaSourceFromString = new JavaSourceFromString("de.soderer.test.SimpleWorker", code);

			final Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(javaSourceFromString);

			final String compilationPath = System.getProperty("user.home") + "/tmp/jitClasses";
			final File compilationPathFile = new File(compilationPath);
			compilationPathFile.mkdirs();

			final List<String> options = new ArrayList<>();
			options.add("-d");
			options.add(compilationPath);
			options.add("-classpath");
			try (URLClassLoader urlClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader()) {
				final StringBuilder stringBuilder = new StringBuilder();
				for (final URL url : urlClassLoader.getURLs()) {
					stringBuilder.append(url.getFile()).append(File.pathSeparator);
				}
				stringBuilder.append(compilationPath);
				options.add(stringBuilder.toString());

				final StringWriter output = new StringWriter();
				final boolean success = javaCompiler.getTask(output, null, null, options, null, fileObjects).call();
				if (success) {
					try (URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { new File(compilationPath).toURI().toURL() })) {
						final Class<?> clazz = Class.forName("de.soderer.test.SimpleWorker", true, classLoader);
						final Constructor<?> constructor = ClassUtilities.getConstructor(clazz);
						constructor.setAccessible(true);
						final Object instance = constructor.newInstance();
						final Method method = clazz.getMethod("runJob");
						method.invoke(instance);
					}
				} else {
					throw new Exception("Compilation failed :" + output);
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}