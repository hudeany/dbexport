package de.soderer.utilities.console;

import java.awt.Toolkit;
import java.awt.datatransfer.DataFlavor;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleUtilities.TextAttribute;

public class PasswordConsoleInput {
	private boolean silent = false;
	private final char hideCharacter;
	private String allowedCharacters = null;
	private String prompt = "";
	protected String presetContent = "";

	public PasswordConsoleInput() {
		this('*');
	}

	public PasswordConsoleInput(final char hideCharacter) {
		this.hideCharacter = hideCharacter;
	}

	/**
	 * When silent = false a system beep occurs on input error
	 *
	 * @param silent
	 */
	public boolean isSilent() {
		return silent;
	}

	/**
	 * When silent = false a system beep occurs on input error
	 *
	 * @param silent
	 */
	public void setSilent(final boolean silent) {
		this.silent = silent;
	}

	/**
	 * For password inputs
	 *
	 * @return
	 */
	public char getHideCharacter() {
		return hideCharacter;
	}

	/**
	 * Get allowed Characters. Default is all chars of german keyboard layout
	 */
	public String getAllowedCharacters() {
		return allowedCharacters;
	}

	/**
	 * Set allowed Characters. Default is all chars of german keyboard layout
	 */
	public void setAllowedCharacters(final String allowedCharacters) {
		this.allowedCharacters = allowedCharacters;
	}

	public String getPrompt() {
		return prompt;
	}

	public PasswordConsoleInput setPrompt(final String prompt) {
		this.prompt = (prompt == null ? "" : prompt);
		return this;
	}

	public String getPresetContent() {
		return presetContent;
	}

	public PasswordConsoleInput setPresetContent(final String presetContent) {
		this.presetContent = (presetContent == null ? "" : presetContent);
		return this;
	}

	public char[] readInput() throws Exception {
		if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			boolean insertMode = true;
			char[] content = presetContent == null ? "".toCharArray() : presetContent.toCharArray();
			int cursorIndex = content.length;
			final int previousContentLength = content.length;

			try {
				ConsoleUtilities.saveCurrentCursorPosition();
				while (true) {
					ConsoleUtilities.hideCursor();
					ConsoleUtilities.moveCursorToSavedPosition();

					TextAttribute[] cursorTextAttributes;
					if (insertMode) {
						cursorTextAttributes = new TextAttribute[] {TextAttribute.UNDERLINE, TextAttribute.HIGH_INTENSITY};
					} else {
						cursorTextAttributes = new TextAttribute[] {TextAttribute.REVERSE_VIDEO};
					}

					System.out.print(prompt);
					System.out.print(Utilities.repeat(hideCharacter, cursorIndex));
					if (content.length > cursorIndex) {
						System.out.print(ConsoleUtilities.getAnsiFormattedText(Character.toString(hideCharacter), null, null, cursorTextAttributes) + Utilities.repeat(hideCharacter, content.length - cursorIndex - 1));
					} else {
						System.out.print(ConsoleUtilities.getAnsiFormattedText(" ", null, null, cursorTextAttributes));
					}

					// Linux needs some extra cleared chars
					if (content.length - previousContentLength > 0) {
						System.out.print(Utilities.repeat(" ", content.length - previousContentLength));
					}

					ConsoleUtilities.clearLineContentAfterCursor();

					ConsoleUtilities.moveCursorToSavedPosition();

					final int keyCode = ConsoleUtilities.readKey();

					if (keyCode == ConsoleUtilities.KeyCode_CtrlC) {
						ConsoleUtilities.showCursor();
						System.exit(1);
					} else if (keyCode == ConsoleUtilities.KeyCode_CtrlV) {
						final String data = (String) Toolkit.getDefaultToolkit().getSystemClipboard().getData(DataFlavor.stringFlavor);
						if (insertMode || cursorIndex == content.length) {
							final char[] newContent = new char[content.length + data.length()];
							for (int i = 0; i < cursorIndex; i++) {
								newContent[i] = content[i];
							}
							final char[] dataArray = data.toCharArray();
							for (int i = 0; i < data.length(); i++) {
								newContent[cursorIndex + i] = dataArray[i];
							}
							for (int i = cursorIndex; i < content.length; i++) {
								newContent[dataArray.length + i] = content[i];
							}
							Utilities.clear(content);
							content = newContent;
						} else {
							final int restLength = Math.max(content.length - cursorIndex - data.length(), 0);
							final char[] newContent = new char[content.length + data.length() + restLength];
							for (int i = 0; i < cursorIndex; i++) {
								newContent[i] = content[i];
							}
							final char[] dataArray = data.toCharArray();
							for (int i = 0; i < data.length(); i++) {
								newContent[cursorIndex + i] = dataArray[i];
							}
							if (restLength > 0) {
								for (int i = cursorIndex + data.length(); i < content.length; i++) {
									newContent[i] = content[i];
								}
							}
							Utilities.clear(content);
							content = newContent;
						}
						cursorIndex += data.length();
					} else if (keyCode == ConsoleUtilities.KeyCode_Escape) {
						// do nothing
					} else if (keyCode == ConsoleUtilities.KeyCode_Enter) {
						// Remove special displayed cursor
						ConsoleUtilities.moveCursorToSavedPosition();
						System.out.print(prompt);
						System.out.println(Utilities.repeat(hideCharacter, content.length) + " ");
						ConsoleUtilities.showCursor();
						return content;
					} else if (keyCode == ConsoleUtilities.KeyCode_Backspace) {
						if (cursorIndex > 0) {
							final char[] newContent = new char[content.length - 1];
							for (int i = 0; i < cursorIndex - 1; i++) {
								newContent[i] = content[i];
							}
							for (int i = cursorIndex; i < content.length; i++) {
								newContent[i - 1] = content[i];
							}
							Utilities.clear(content);
							content = newContent;
							cursorIndex--;
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_Delete) {
						if (cursorIndex < content.length) {
							final char[] newContent = new char[content.length - 1];
							for (int i = 0; i < cursorIndex; i++) {
								newContent[i] = content[i];
							}
							for (int i = cursorIndex + 1; i < content.length; i++) {
								newContent[i - 1] = content[i];
							}
							Utilities.clear(content);
							content = newContent;
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowLeft) {
						if (cursorIndex > 0) {
							cursorIndex--;
						} else {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowRight) {
						if (cursorIndex < content.length) {
							cursorIndex++;
						} else {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowDown) {
						// do nothing
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowUp) {
						// do nothing
					} else if (keyCode == ConsoleUtilities.KeyCode_PageDown) {
						if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_PageUp) {
						if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_Home) {
						cursorIndex = 0;
					} else if (keyCode == ConsoleUtilities.KeyCode_End) {
						cursorIndex = content.length;
					} else if (keyCode == ConsoleUtilities.KeyCode_Insert) {
						insertMode = !insertMode;
					} else if (keyCode == ConsoleUtilities.KeyCode_Tab) {
						if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else {
						// "keyCode < 1000" to prevent Chinese symbols on some systems, that occur without intention
						if ((allowedCharacters != null && allowedCharacters.contains("" + (char) keyCode)) || keyCode < 1000) {
							if (insertMode || cursorIndex == content.length) {
								final char[] newContent = new char[content.length + 1];
								for (int i = 0; i < cursorIndex; i++) {
									newContent[i] = content[i];
								}
								newContent[cursorIndex] = (char) keyCode;
								for (int i = cursorIndex; i < content.length; i++) {
									newContent[1 + i] = content[i];
								}
								Utilities.clear(content);
								content = newContent;
								cursorIndex++;
							} else {
								content[cursorIndex] = (char) keyCode;
								cursorIndex++;
							}
						}
					}
				}
			} finally {
				ConsoleUtilities.showCursor();
			}
		} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
			return System.console().readPassword(prompt);
		} else {
			return System.console().readPassword(prompt);
		}
	}
}
