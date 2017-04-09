package de.soderer.utilities.code;

import java.io.File;
import java.io.StringWriter;
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

	public JavaSourceFromString(String name, String code) {
		super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
		this.code = code;
	}

	@Override
	public CharSequence getCharContent(boolean ignoreEncodingErrors) {
		return code;
	}

	public static void main(String[] args) {
		try {
			JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
			if (javaCompiler == null) {
				throw new Exception("Compiler unavailable");
			}

			String code = "package de.soderer.test;\n" + "public class SimpleWorker {\n" + "public void runJob() throws Exception {\n" + "System.out.println(\"SimpleJobWorker worked\");\n" + "}\n"
					+ "}\n";

			JavaSourceFromString javaSourceFromString = new JavaSourceFromString("de.soderer.test.SimpleWorker", code);

			Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(javaSourceFromString);

			String compilationPath = System.getProperty("user.home") + "/tmp/jitClasses";
			File compilationPathFile = new File(compilationPath);
			compilationPathFile.mkdirs();

			List<String> options = new ArrayList<String>();
			options.add("-d");
			options.add(compilationPath);
			options.add("-classpath");
			URLClassLoader urlClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
			StringBuilder stringBuilder = new StringBuilder();
			for (URL url : urlClassLoader.getURLs()) {
				stringBuilder.append(url.getFile()).append(File.pathSeparator);
			}
			stringBuilder.append(compilationPath);
			options.add(stringBuilder.toString());

			StringWriter output = new StringWriter();
			boolean success = javaCompiler.getTask(output, null, null, options, null, fileObjects).call();
			if (success) {
				URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { new File(compilationPath).toURI().toURL() });
				Class<?> clazz = Class.forName("de.soderer.test.SimpleWorker", true, classLoader);
				Object instance = clazz.newInstance();
				Method method = clazz.getMethod("runJob");
				method.invoke(instance);
			} else {
				throw new Exception("Compilation failed :" + output);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}