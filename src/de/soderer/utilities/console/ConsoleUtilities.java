package de.soderer.utilities.console;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.Thread.State;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Queue;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.SystemUtilities;
import de.soderer.utilities.Utilities;

public class ConsoleUtilities {
	private static Queue<Character> consoleInputBuffer = null;

	private static ConsoleType consoleType = null;
	private static boolean linuxConsoleActivatedRawMode = false;

	public static final int KeyCode_CtrlC = 3;
	public static final int KeyCode_CtrlV = 22;
	public static final int KeyCode_CtrlX = 24;

	public static final int KeyCode_CtrlHome = 57463;
	public static final int KeyCode_CtrlEnd = 57461;
	public static final int KeyCode_CtrlArrowUp = 57485;
	public static final int KeyCode_CtrlArrowLeft = 57459;
	public static final int KeyCode_CtrlArrowRight = 57460;
	public static final int KeyCode_CtrlArrowDown = 57489;

	public static final int KeyCode_Enter = 13;
	public static final int KeyCode_Backspace = 8;
	public static final int KeyCode_Tab = 9;
	/**
	 * There is no Windows Code for ReverseTab
	 */
	public static final int KeyCode_ReverseTab = -1;
	public static final int KeyCode_Escape = 27;

	public static final int KeyCode_Home = 57415;
	public static final int KeyCode_PageUp = 57417;
	public static final int KeyCode_End = 57423;
	public static final int KeyCode_Insert = 57426;
	public static final int KeyCode_Delete = 57427;
	public static final int KeyCode_PageDown = 57425;

	public static final int KeyCode_ArrowUp = 57416;
	public static final int KeyCode_ArrowLeft = 57419;
	public static final int KeyCode_ArrowRight = 57421;
	public static final int KeyCode_ArrowDown = 57424;

	/**
	 * Additional attributes
		Bold 1
		Underline 4
		No underline 24
		Negative(reverse the foreground and background) 7
		Positive(no negative) 27
		Default 0
	 */
	public enum TextColor {
		Black(30, 40),
		Red(31, 41),
		Green(32, 42),
		Yellow(33, 43),
		Blue(34, 44),
		Magenta(35, 45),
		Cyan(36, 46),
		Light_gray(37, 47),
		Dark_gray(90, 100),
		Light_red(91, 101),
		Light_green(92, 102),
		Light_yellow(93, 103),
		Light_blue(94, 104),
		Light_magenta(95, 105),
		Light_cyan(96, 106),
		White(97, 107);

		private final int foreGroundColorCode;
		private final int backGroundColorCode;

		TextColor(final int foreGroundColorCode, final int backGroundColorCode) {
			this.foreGroundColorCode = foreGroundColorCode;
			this.backGroundColorCode = backGroundColorCode;
		}

		public int getForeGroundColorCode() {
			return foreGroundColorCode;
		}

		public int getBackGroundColorCode() {
			return backGroundColorCode;
		}
	}

	public enum TextAttribute {
		/**
		 * Reset Text decoration
		 */
		RESET("\033[0m"),

		HIGH_INTENSITY("\033[1m"),

		/**
		 * Not supported by Windows console
		 */
		LOW_INTENSITY("\033[2m"),

		/**
		 * Not supported by Windows console
		 */
		ITALIC("\033[3m"),

		UNDERLINE("\033[4m"),

		/**
		 * Not supported by Windows console
		 */
		BLINK("\033[5m"),

		/**
		 * Not supported by Windows console
		 */
		RAPID_BLINK("\033[6m"),

		REVERSE_VIDEO("\033[7m"),

		INVISIBLE_TEXT("\033[8m");

		private final String ansiCode;

		TextAttribute(final String ansiCode) {
			this.ansiCode = ansiCode;
		}

		public String getAnsiCode() {
			return ansiCode;
		}
	}

	public static ConsoleType getConsoleType() {
		if (consoleType == null) {
			if (SystemUtilities.isWindowsSystem() || (System.console() != null && System.getenv().get("TERM") != null)) {
				consoleType = ConsoleType.ANSI;
				try {
					getTerminalSize();
				} catch (final Exception e) {
					e.printStackTrace();
					consoleType = ConsoleType.TEST;
				}
			} else {
				consoleType = ConsoleType.TEST;
			}
		}
		return consoleType;
	}

	public static boolean activateLinuxConsoleRawMode() throws Exception {
		if (!linuxConsoleActivatedRawMode) {
			Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", "stty raw -echo < /dev/tty" }).waitFor();
			linuxConsoleActivatedRawMode = true;
			registerShutdownHook();
			return true;
		} else {
			return false;
		}
	}

	public static void deactivateLinuxConsoleRawMode() throws Exception {
		if (linuxConsoleActivatedRawMode) {
			Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", "stty -raw echo </dev/tty" }).waitFor();
			linuxConsoleActivatedRawMode = false;
		}
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					deactivateLinuxConsoleRawMode();
				} catch (@SuppressWarnings("unused") final Exception e) {
					// do nothing
				}
			}
		});
	}

	/**
	 * Create a progress string for terminal output e.g.: "65% [=================================>                   ] 103.234 200/s eta 5m"
	 *
	 * @param start
	 * @param itemsToDo
	 * @param itemsDone
	 * @return
	 */
	public static String getConsoleProgressString(final int lineLength, final LocalDateTime start, final long itemsToDo, final long itemsDone, final String itemUnitSign) {
		final LocalDateTime now = LocalDateTime.now();
		String itemsToDoString = "??";
		String percentageString = " 0%";
		String speedString = "???/s";
		String etaString = "eta ???";
		int percentageDone = 0;

		if (itemsToDo > 0) {
			itemsToDoString = Utilities.getHumanReadableNumber(itemsToDo, itemUnitSign, true, 5, true, Locale.ENGLISH);

			if (itemsDone > 0) {
				percentageDone = (int) (itemsDone * 100 / itemsToDo);
				percentageString = Utilities.leftPad(percentageDone + "%", 3);
				long elapsedSeconds = Duration.between(start, now).toSeconds();
				// Prevent division by zero, when start is fast
				if (elapsedSeconds == 0) {
					elapsedSeconds = 1;
				}
				final int speed = (int) (itemsDone / elapsedSeconds);
				speedString = Utilities.getHumanReadableNumber(speed, "", true, 5, true, Locale.ENGLISH) + (itemUnitSign == null ? "" : itemUnitSign) + "/s";
				final LocalDateTime estimatedEnd = DateUtilities.calculateETA(start, itemsToDo, itemsDone);
				etaString = "eta " + DateUtilities.getShortHumanReadableTimespan(Duration.between(now, estimatedEnd), false, true);
			}
		}

		final String leftPart = percentageString + " [";
		final String rightPart = "] " + itemsToDoString + " " + speedString + " " + etaString;
		final int barWith = lineLength - (leftPart.length() + rightPart.length());
		int barDone = barWith * percentageDone / 100;
		if (barDone < 1) {
			barDone = 1;
		} else if (barDone >= barWith) {
			barDone = barWith;
		}
		return leftPart + Utilities.repeat("=", barDone - 1) + ">" + Utilities.repeat(" ", barWith - barDone) + rightPart;
	}

	public static void printBoxed(final String text) {
		printBoxed(text, '*');
	}

	public static void printBoxed(final String text, final char boxChar) {
		if (text != null) {
			System.out.println(Utilities.repeat(boxChar, text.length() + 4));
			System.out.println(boxChar + " " + text + " " + boxChar);
			System.out.println(Utilities.repeat(boxChar, text.length() + 4));
		}
	}

	public static void clearScreen() {
		if (SystemUtilities.isWindowsSystem()) {
			try {
				new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
			} catch (@SuppressWarnings("unused") final Exception e) {
				System.err.println("Cannot clear Windows console");
			}
		} else {
			System.out.print("\033[H\033[2J");
			System.out.flush();
		}
	}

	public static String getAnsiColoredText(final String text, final TextColor foreGroundColor) {
		return getAnsiFormattedText(text, foreGroundColor, null);
	}

	public static String getAnsiColoredText(final TextColor foreGroundColor, final TextColor backGroundColor, final String text) {
		return getAnsiFormattedText(text, foreGroundColor, backGroundColor);
	}

	public static String getAnsiFormattedText(final String text, final TextColor foreGroundColor, final TextColor backGroundColor, final TextAttribute... attributes) {
		final StringBuilder builder = new StringBuilder();
		if (foreGroundColor != null) {
			builder.append("\033[" + foreGroundColor.getForeGroundColorCode() + "m");
		}
		if (backGroundColor != null) {
			builder.append("\033[" + backGroundColor.getBackGroundColorCode() + "m");
		}
		if (attributes != null) {
			for (final TextAttribute attribute : attributes) {
				builder.append(attribute.getAnsiCode());
			}
		}
		builder.append(text);
		builder.append(TextAttribute.RESET.getAnsiCode());
		return builder.toString();
	}

	public static void saveCurrentCursorPosition() {
		System.out.print("\033[s");
	}

	public static void moveCursorToPosition(final int line, final int column) {
		System.out.print("\033[" + line + ";" + column + "H");
	}

	public static void moveCursorToSavedPosition() {
		System.out.print("\033[u");
	}

	public static void clearLineContentAfterCursor() {
		System.out.print("\033[K");
	}

	public static void hideCursor() {
		System.out.print("\033[?25l");
	}

	public static void showCursor() {
		System.out.print("\033[?25h");
	}

	public static Position getCursorPosition() throws Exception {
		try {
			final StringBuilder response = new StringBuilder();
			saveCurrentCursorPosition();
			try {
				if (SystemUtilities.isWindowsSystem()) {
					// Request current cursor position
					System.out.print("\033[6n");

					int keyBuffer;
					while ((keyBuffer = WindowsKeyStrokeReader.readKey(false)) > 0) {
						response.append((char) keyBuffer);
					}
				} else {
					final boolean linuxConsoleMustBeReset = activateLinuxConsoleRawMode();
					try {
						// Request current cursor position
						System.out.print("\033[6n");

						try (Reader inputReader = System.console().reader()) {
							int byteBuffer;
							while ((byteBuffer = inputReader.read()) > -1) {
								if (byteBuffer == 3) {
									break;
								} else if (byteBuffer == 27) {
									response.append("\\033");
								} else {
									response.append((char) byteBuffer);
									if ('R' == byteBuffer) {
										break;
									}
								}
							}
						}
					} finally {
						if (linuxConsoleMustBeReset) {
							deactivateLinuxConsoleRawMode();
						}
					}
				}
			} finally {
				moveCursorToSavedPosition();
			}

			if (response.lastIndexOf("[") > -1) {
				final String lineString = response.substring(response.lastIndexOf("[") + 1, response.indexOf(";"));
				final int line = Integer.parseInt(lineString);
				final String columnString = response.substring(response.indexOf(";") + 1, response.indexOf("R"));
				final int column = Integer.parseInt(columnString);
				return new Position(line, column);
			} else {
				throw new Exception("Cannot detect terminal size");
			}
		} catch (final Exception e) {
			throw new Exception("Cannot detect terminal size", e);
		}
	}

	public static Size getTerminalSize() throws Exception {
		try {
			final StringBuilder response = new StringBuilder();
			saveCurrentCursorPosition();
			try {
				moveCursorToPosition(3000, 3000);

				if (SystemUtilities.isWindowsSystem()) {
					final WindowsConsoleWidthDetection consoleWidthDetection = new WindowsConsoleWidthDetection(response);
					final Thread consoleWithDetectionThread = new Thread(consoleWidthDetection, "ConsoleWidthDetection");
					consoleWithDetectionThread.start();
					Thread.sleep(1000);
					if (consoleWithDetectionThread.getState() != State.TERMINATED) {
						consoleWithDetectionThread.interrupt();
						consoleType = ConsoleType.TEST;
						return new Size(24, 80);
					}
				} else {
					final boolean linuxConsoleMustBeReset = activateLinuxConsoleRawMode();
					try {
						// Request current cursor position
						System.out.print("\033[6n");

						try (Reader inputReader = System.console().reader()) {
							int byteBuffer;
							while ((byteBuffer = inputReader.read()) > -1) {
								if (byteBuffer == 3) {
									break;
								} else if (byteBuffer == 27) {
									response.append("\\033");
								} else {
									response.append((char) byteBuffer);
									if ('R' == byteBuffer) {
										break;
									}
								}
							}
						}
					} finally {
						if (linuxConsoleMustBeReset) {
							deactivateLinuxConsoleRawMode();
						}
					}
				}
			} finally {
				moveCursorToSavedPosition();
			}

			if (response.lastIndexOf("[") > -1) {
				final String heightString = response.substring(response.lastIndexOf("[") + 1, response.indexOf(";"));
				final int height = Integer.parseInt(heightString);
				final String widthString = response.substring(response.indexOf(";") + 1, response.indexOf("R"));
				final int width = Integer.parseInt(widthString);
				return new Size(height, width);
			} else {
				throw new Exception("Cannot detect terminal size");
			}
		} catch (final Exception e) {
			throw new Exception("Cannot detect terminal size", e);
		}
	}

	public static Size getUnixTerminalSizeByTput() throws Exception {
		try {
			return new Size(Integer.parseInt(executeTputCommand("lines").trim()), Integer.parseInt(executeTputCommand("cols").trim()));
		} catch (final Exception e) {
			throw new Exception("Cannot detect terminal size", e);
		}
	}

	private static String executeTputCommand(final String command) throws IOException {
		final ProcessBuilder processBuilder = new ProcessBuilder("tput", command);
		try (InputStream inputStream = processBuilder.start().getInputStream()) {
			int readBuffer = 0;
			final StringBuffer buffer = new StringBuffer();
			while ((readBuffer = inputStream.read()) != -1) {
				buffer.append((char) readBuffer);
			}
			return buffer.toString();
		}
	}

	public static int readKey() throws Exception {
		try {
			if (SystemUtilities.isWindowsSystem()) {
				return WindowsKeyStrokeReader.readKey(true);
			} else {
				if (consoleInputBuffer != null) {
					final int returnValue = consoleInputBuffer.remove();
					if (consoleInputBuffer.size() == 0) {
						consoleInputBuffer = null;
					}
					return returnValue;
				} else {
					final boolean linuxConsoleMustBeReset = activateLinuxConsoleRawMode();
					try (Reader consoleReader = System.console().reader()) {
						int nextKeyCode = -1;

						final char[] inputReadBuffer = new char[256];
						int inputReadBytes;
						// Read method always waits for next key pressed, no matter how much data is still left in the console-reader
						inputReadBytes = consoleReader.read(inputReadBuffer);
						if (inputReadBytes > 1 && inputReadBuffer[0] != 27) {
							consoleInputBuffer = new LinkedList<>();
							for (int i = 0; i < inputReadBytes; i++) {
								consoleInputBuffer.add(inputReadBuffer[i]);
							}
							nextKeyCode = consoleInputBuffer.remove();
						} else {
							nextKeyCode = inputReadBuffer[0];
						}

						if (nextKeyCode == 3) {
							consoleInputBuffer = null;
							return KeyCode_CtrlC;
						} else if (nextKeyCode == 8) {
							consoleInputBuffer = null;
							return KeyCode_Backspace;
						} else if (nextKeyCode == 9) {
							consoleInputBuffer = null;
							return KeyCode_Tab;
						} else if (nextKeyCode == 13) {
							consoleInputBuffer = null;
							return KeyCode_Enter;
						} else if (nextKeyCode == 22) {
							consoleInputBuffer = null;
							return KeyCode_CtrlV;
						} else if (nextKeyCode == 24) {
							consoleInputBuffer = null;
							return KeyCode_CtrlX;
						} else if (nextKeyCode == 27) {
							consoleInputBuffer = null;
							// Escape sequence is entered
							if (inputReadBytes == 3) {
								if (inputReadBuffer[1] == 1 && inputReadBuffer[2] == 65) {
									return KeyCode_ArrowUp;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 66) {
									return KeyCode_ArrowDown;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 67) {
									return KeyCode_ArrowRight;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 68) {
									return KeyCode_ArrowLeft;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 70) {
									return KeyCode_End;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 72) {
									return KeyCode_Home;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 90) {
									return KeyCode_ReverseTab;
								} else {
									return KeyCode_Escape;
								}
							} else if (inputReadBytes == 4) {
								if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 50 && inputReadBuffer[3] == 126) {
									return KeyCode_Insert;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 51 && inputReadBuffer[3] == 126) {
									return KeyCode_Delete;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 53 && inputReadBuffer[3] == 126) {
									return KeyCode_PageUp;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 54 && inputReadBuffer[3] == 126) {
									return KeyCode_PageDown;
								} else {
									return KeyCode_Escape;
								}
							} else if (inputReadBytes == 6) {
								if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 70) {
									return KeyCode_CtrlEnd;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 72) {
									return KeyCode_CtrlHome;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 65) {
									return KeyCode_CtrlArrowUp;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 66) {
									return KeyCode_CtrlArrowDown;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 67) {
									return KeyCode_CtrlArrowRight;
								} else if (inputReadBuffer[1] == 91 && inputReadBuffer[2] == 49 && inputReadBuffer[3] == 59 && inputReadBuffer[4] == 53 && inputReadBuffer[5] == 68) {
									return KeyCode_CtrlArrowLeft;
								} else {
									return KeyCode_Escape;
								}
							} else {
								return KeyCode_Escape;
							}
						} else if (nextKeyCode == 127) {
							consoleInputBuffer = null;
							return KeyCode_Backspace;
						} else {
							return nextKeyCode;
						}
					} finally {
						if (linuxConsoleMustBeReset) {
							deactivateLinuxConsoleRawMode();
						}
					}
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Cannot readKey", e);
		}
	}

	public static int readNextRawKey() throws Exception {
		try {
			if (SystemUtilities.isWindowsSystem()) {
				return WindowsKeyStrokeReader.readKey(true);
			} else {
				final boolean linuxConsoleMustBeReset = activateLinuxConsoleRawMode();
				try (Reader consoleReader = System.console().reader()) {
					return consoleReader.read();
				} finally {
					if (linuxConsoleMustBeReset) {
						deactivateLinuxConsoleRawMode();
					}
				}
			}
		} catch (final Exception e) {
			throw new Exception("Cannot readNextRawKey", e);
		}
	}
}
