package de.soderer.utilities.console;

public class WindowsConsoleWidthDetection implements Runnable {
	private final StringBuilder response;
	private ConsoleType consoleType = null;

	public ConsoleType getConsoleType() {
		return consoleType;
	}

	public WindowsConsoleWidthDetection(final StringBuilder response) {
		this.response = response;
	}

	@Override
	public void run() {
		try {
			// Request current cursor position
			System.out.print("\033[6n");

			int keyBuffer;
			while ((keyBuffer = WindowsKeyStrokeReader.readKey(false)) > 0) {
				response.append((char) keyBuffer);
			}
			consoleType = ConsoleType.ANSI;
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Most probably an eclipse console
			consoleType = ConsoleType.TEST;
		}
	}
}
