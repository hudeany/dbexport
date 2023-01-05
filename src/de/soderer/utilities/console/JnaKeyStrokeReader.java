package de.soderer.utilities.console;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;

import de.soderer.utilities.SystemUtilities;
import de.soderer.utilities.Utilities;

public class JnaKeyStrokeReader {
	/**
	 * Detection of Ctrl-C only works on Windows systems, because it ends Linux Java VM immediatelly
	 */
	public static final int KeyCode_CtrlC = 3;
	public static final int KeyCode_CtrlV = 22;
	public static final int KeyCode_CtrlX = 24;

	/**
	 * Detection of contextmenu insert. Only works on Linux systems
	 */
	public static final int KeyCode_ContextMenu_Insert = 6513249;

	public static final int KeyCode_Enter;
	public static final int KeyCode_Backspace;
	public static final int KeyCode_Tab = 9;
	public static final int KeyCode_Escape = 27;

	public static final int KeyCode_Home;
	public static final int KeyCode_PageUp;
	public static final int KeyCode_End;
	public static final int KeyCode_Insert;
	public static final int KeyCode_Delete;
	public static final int KeyCode_PageDown;

	public static final int KeyCode_Up;
	public static final int KeyCode_Left;
	public static final int KeyCode_Right;
	public static final int KeyCode_Down;

	public static final int KeyCode_AUml_Lower;
	public static final int KeyCode_AUml_Upper;
	public static final int KeyCode_OUml_Lower;
	public static final int KeyCode_OUml_Upper;
	public static final int KeyCode_UUml_Lower;
	public static final int KeyCode_UUml_Upper;

	private static boolean initDone;
	private static boolean stdinIsConsole;
	private static boolean consoleModeAltered;

	// Windows
	private static Msvcrt msvcrt;
	private static Kernel32 kernel32;
	private static Pointer consoleHandle;
	private static int originalConsoleMode;

	// Linux
	private static Libc libc;
	private static TerminalFlags originalTerminalFlags;
	private static TerminalFlags rawTerminalFlags;
	private static TerminalFlags intermediateTerminalFlags;

	static {
		if (SystemUtilities.isWindowsSystem()) {
			KeyCode_Enter = 13;
			KeyCode_Backspace = 8;

			KeyCode_Insert = 57426;
			KeyCode_Home = 57415;
			KeyCode_PageUp = 57417;
			KeyCode_End = 57423;
			KeyCode_Delete = 57427;
			KeyCode_PageDown = 57425;

			KeyCode_Up = 57416;
			KeyCode_Left = 57419;
			KeyCode_Right = 57421;
			KeyCode_Down = 57424;

			KeyCode_AUml_Lower = 228;
			KeyCode_AUml_Upper = 196;
			KeyCode_OUml_Lower = 246;
			KeyCode_OUml_Upper = 214;
			KeyCode_UUml_Lower = 252;
			KeyCode_UUml_Upper = 220;
		} else {
			KeyCode_Enter = 10;
			KeyCode_Backspace = 127;

			KeyCode_Insert = 2117229339;
			KeyCode_Home = 4741915;
			KeyCode_PageUp = 2117425947;
			KeyCode_Delete = 2117294875;
			KeyCode_End = 4610843;
			KeyCode_PageDown = 2117491483;

			KeyCode_Up = 4283163;
			KeyCode_Left = 4479771;
			KeyCode_Right = 4414235;
			KeyCode_Down = 4348699;

			KeyCode_AUml_Lower = 42179;
			KeyCode_AUml_Upper = 33987;
			KeyCode_OUml_Lower = 46787;
			KeyCode_OUml_Upper = 38595;
			KeyCode_UUml_Lower = 48323;
			KeyCode_UUml_Upper = 40131;
		}
	}

	/**
	 * Read a character from the console without echo.
	 */
	public static int readKey(final boolean wait) throws Exception {
		if (SystemUtilities.isWindowsSystem()) {
			return readKeyWindows(wait);
		} else {
			return readKeyUnix(wait);
		}
	}

	/**
	 * Resets console mode to normal line mode
	 */
	public static void resetConsoleMode() throws IOException {
		if (initDone && stdinIsConsole && consoleModeAltered) {
			if (SystemUtilities.isWindowsSystem()) {
				setConsoleMode(consoleHandle, originalConsoleMode);
			} else {
				setTerminalAttrs(0, originalTerminalFlags);
			}
			consoleModeAltered = false;
		}
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdownHook();
			}
		});
	}

	private static void shutdownHook() {
		try {
			resetConsoleMode();
		} catch (@SuppressWarnings("unused") final Exception e) {
			// do nothing
		}
	}

	private static int readKeyWindows(final boolean wait) throws IOException {
		initWindows();
		if (stdinIsConsole) {
			consoleModeAltered = true;
			setConsoleMode(consoleHandle, originalConsoleMode & ~Kernel32Definitions.ENABLE_PROCESSED_INPUT);
			if (!wait && msvcrt._kbhit() == 0) {
				return -2;
			}
			return getwch();
		} else {
			int c = msvcrt.getwchar();
			if (c == 0xFFFF) {
				c = -1;
			}
			return c;
		}
	}

	private static int getwch() {
		int c = msvcrt._getwch();
		if (c == 0 || c == 0xE0) {
			c = msvcrt._getwch();
			if (c >= 0 && c <= 0x18FF) {
				return 0xE000 + c;
			} else {
				return 0xFFFE;
			}
		} else if (c < 0 || c > 0xFFFF) {
			return 0xFFFE;
		} else {
			return c;
		}
	}

	private static synchronized void initWindows() {
		if (initDone) {
			return;
		} else {
			msvcrt = Native.load("msvcrt", Msvcrt.class);
			kernel32 = Native.load("kernel32", Kernel32.class);
			try {
				consoleHandle = getStdInputHandle();
				originalConsoleMode = getConsoleMode(consoleHandle);
				stdinIsConsole = true;
			} catch (@SuppressWarnings("unused") final IOException e) {
				stdinIsConsole = false;
			}
			if (stdinIsConsole) {
				registerShutdownHook();
			}
			initDone = true;
		}
	}

	private static Pointer getStdInputHandle() throws IOException {
		final Pointer handle = kernel32.GetStdHandle(Kernel32Definitions.STD_INPUT_HANDLE);
		if (Pointer.nativeValue(handle) == 0 || Pointer.nativeValue(handle) == Kernel32Definitions.INVALID_HANDLE_VALUE) {
			throw new IOException("GetStdHandle(STD_INPUT_HANDLE) failed");
		} else {
			return handle;
		}
	}

	private static int getConsoleMode(final Pointer handle) throws IOException {
		final IntByReference mode = new IntByReference();
		if (kernel32.GetConsoleMode(handle, mode) == 0) {
			throw new IOException("GetConsoleMode failed");
		} else {
			return mode.getValue();
		}
	}

	private static void setConsoleMode(final Pointer handle, final int mode) throws IOException {
		if (kernel32.SetConsoleMode(handle, mode) == 0) {
			throw new IOException("SetConsoleMode failed");
		}
	}

	private interface Msvcrt extends Library {
		int _kbhit();
		int _getwch();
		int getwchar();
	}

	private static class Kernel32Definitions {
		static final int STD_INPUT_HANDLE = -10;
		static final long INVALID_HANDLE_VALUE = (Native.POINTER_SIZE == 8) ? -1 : 0xFFFFFFFFL;
		static final int ENABLE_PROCESSED_INPUT = 0x0001;
	}

	private interface Kernel32 extends Library {
		int GetConsoleMode(Pointer hConsoleHandle, IntByReference lpMode);
		int SetConsoleMode(Pointer hConsoleHandle, int dwMode);
		Pointer GetStdHandle(int nStdHandle);
	}

	private static int readKeyUnix(final boolean wait) throws Exception {
		initUnix();
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

		//			final byte[] inputBuffer = new byte[4];
		//			int inputBufferLength = 0;
		//			while (true) {
		//			if (inputBufferLength >= inputBuffer.length) {
		//				return 0xFFFE;
		//			} else {
		//				final int b = inputStream.read();
		//				if (b == -1) {
		//					return -1;
		//				} else {
		//					inputBuffer[inputBufferLength++] = (byte) b;
		//					final int c = decodeCharFromBytes(inputBuffer, inputBufferLength);
		//					if (c != -1) {
		//						return c;
		//					}
		//				}
		//			}
		//	}
	}

	//	private static synchronized int decodeCharFromBytes(final byte[] inBytes, final int inLen) {
	//		charsetDecoder.reset();
	//		charsetDecoder.onMalformedInput(CodingErrorAction.REPLACE);
	//		charsetDecoder.replaceWith(String.valueOf((char) 0xFFFE));
	//		final ByteBuffer in = ByteBuffer.wrap(inBytes, 0, inLen);
	//		final CharBuffer out = CharBuffer.allocate(1);
	//		charsetDecoder.decode(in, out, false);
	//		if (out.position() == 0) {
	//			return -1;
	//		} else {
	//			return out.get(0);
	//		}
	//	}

	private static synchronized void initUnix() throws IOException {
		if (!initDone) {
			libc = Native.load("c", Libc.class);
			stdinIsConsole = libc.isatty(0) == 1;
			//			charsetDecoder = Charset.defaultCharset().newDecoder();
			if (stdinIsConsole) {
				originalTerminalFlags = getTerminalAttrs(0);
				rawTerminalFlags = new TerminalFlags(originalTerminalFlags);
				rawTerminalFlags.c_lflag &= ~(LibcDefinitions.ICANON | LibcDefinitions.ECHO | LibcDefinitions.ECHONL | LibcDefinitions.ISIG);
				intermediateTerminalFlags = new TerminalFlags(rawTerminalFlags);
				intermediateTerminalFlags.c_lflag |= LibcDefinitions.ICANON;
				registerShutdownHook();
			}
			initDone = true;
		}
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
