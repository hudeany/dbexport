package de.soderer.utilities.console;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

import de.soderer.utilities.Utilities;

public class LinuxKeyStrokeReader {
	private static boolean initDone;
	private static boolean stdinIsConsole;
	private static boolean consoleModeAltered;

	private static Libc libc;
	private static TerminalFlags originalTerminalFlags;
	private static TerminalFlags rawTerminalFlags;
	private static TerminalFlags intermediateTerminalFlags;

	/**
	 * Read a character from the console without echo.
	 */
	public static int readKey(final boolean wait) throws Exception {
		init();
		if (!stdinIsConsole) {
			return readSingleCharFromByteStream(System.in);
		} else {
			consoleModeAltered = true;
			setTerminalAttrs(0, rawTerminalFlags);
			try {
				if (!wait && System.in.available() == 0) {
					return -2;
				} else {
					return readSingleCharFromByteStream(System.in);
				}
			} finally {
				setTerminalAttrs(0, intermediateTerminalFlags);
			}
		}
	}

	private static synchronized void init() throws IOException {
		if (!initDone) {
			libc = Native.load("c", Libc.class);
			stdinIsConsole = libc.isatty(0) == 1;

			if (stdinIsConsole) {
				originalTerminalFlags = getTerminalAttrs(0);
				rawTerminalFlags = new TerminalFlags(originalTerminalFlags);
				rawTerminalFlags.c_lflag &= ~(LibcDefinitions.ICANON | LibcDefinitions.ECHO | LibcDefinitions.ECHONL | LibcDefinitions.ISIG);
				intermediateTerminalFlags = new TerminalFlags(rawTerminalFlags);
				intermediateTerminalFlags.c_lflag |= LibcDefinitions.ICANON;

				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						try {
							if (initDone && stdinIsConsole && consoleModeAltered) {
								setTerminalAttrs(0, originalTerminalFlags);
								consoleModeAltered = false;
							}
						} catch (@SuppressWarnings("unused") final Exception e) {
							// do nothing
						}
					}
				});
			}
			initDone = true;
		}
	}

	private static TerminalFlags getTerminalAttrs(final int fd) throws IOException {
		final TerminalFlags termios = new TerminalFlags();
		try {
			if (libc.tcgetattr(fd, termios) != 0) {
				throw new RuntimeException("tcgetattr failed");
			}
		} catch (final LastErrorException e) {
			throw new IOException("tcgetattr failed", e);
		}
		return termios;
	}

	private static void setTerminalAttrs(final int fd, final TerminalFlags termios) throws IOException {
		try {
			if (libc.tcsetattr(fd, LibcDefinitions.TCSANOW, termios) != 0) {
				throw new RuntimeException("tcsetattr failed");
			}
		} catch (final LastErrorException e) {
			throw new IOException("tcsetattr failed", e);
		}
	}

	private static int readSingleCharFromByteStream(final InputStream inputStream) throws Exception {
		while (inputStream.available() == 0) {
			// wait for data
		}
		final byte[] buffer = new byte[4];
		int index = 3;
		while (inputStream.available() > 0) {
			if (index < 0) {
				break;
			} else {
				buffer[index--] = (byte) inputStream.read();
			}
		}
		return Utilities.convertByteArrayToInt(buffer);
	}

	protected static class TerminalFlags extends Structure {
		public int c_iflag;
		public int c_oflag;
		public int c_cflag;
		public int c_lflag;
		public byte c_line;
		public byte[] filler = new byte[64];

		@Override
		protected List<String> getFieldOrder() {
			return Arrays.asList("c_iflag", "c_oflag", "c_cflag", "c_lflag", "c_line", "filler");
		}

		TerminalFlags() {
		}

		TerminalFlags(final TerminalFlags t) {
			c_iflag = t.c_iflag;
			c_oflag = t.c_oflag;
			c_cflag = t.c_cflag;
			c_lflag = t.c_lflag;
			c_line = t.c_line;
			filler = t.filler.clone();
		}
	}

	private static class LibcDefinitions {
		static final int ISIG = 0000001;
		static final int ICANON = 0000002;
		static final int ECHO = 0000010;
		static final int ECHONL = 0000100;
		static final int TCSANOW = 0;
	}

	private interface Libc extends Library {
		int tcgetattr(int fd, TerminalFlags termios) throws LastErrorException;
		int tcsetattr(int fd, int opt, TerminalFlags termios) throws LastErrorException;
		int isatty(int fd);
	}
}
