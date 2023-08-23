package de.soderer.utilities;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamWithOtherItemsToClose extends InputStream {
	private InputStream baseInputStream = null;
	private String name = null;
	private Closeable[] otherItemsToClose = null;

	public InputStreamWithOtherItemsToClose(final InputStream baseInputStream, final String name, final Closeable... otherItemsToClose) {
		this.baseInputStream = baseInputStream;
		this.name = name;
		this.otherItemsToClose = otherItemsToClose;
	}

	public InputStreamWithOtherItemsToClose(final InputStream baseInputStream, final Closeable... otherItemsToClose) {
		this.baseInputStream = baseInputStream;
		this.otherItemsToClose = otherItemsToClose;
	}

	public String getName() {
		return name;
	}

	@Override
	public int read() throws IOException {
		return baseInputStream.read();
	}

	@Override
	public void close() throws IOException {
		baseInputStream.close();
		if (otherItemsToClose != null) {
			for (final Closeable otherItemToClose : otherItemsToClose) {
				if (otherItemToClose != null) {
					otherItemToClose.close();
				}
			}
		}
	}
}
