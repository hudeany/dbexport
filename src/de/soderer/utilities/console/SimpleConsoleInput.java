package de.soderer.utilities.console;

import java.awt.Toolkit;
import java.awt.datatransfer.DataFlavor;
import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleUtilities.TextAttribute;

public class SimpleConsoleInput {
	private boolean silent = false;
	private Character hideCharacter = null;
	private String allowedCharacters = null;
	private String prompt = "";
	protected String presetContent = "";
	protected List<String> autoCompletionStrings = null;

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
	public Character getHideCharacter() {
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

	/**
	 * For password inputs
	 *
	 * @param hideCharacter
	 * @return
	 */
	public SimpleConsoleInput setHideCharacter(final Character hideCharacter) {
		this.hideCharacter = hideCharacter;
		return this;
	}

	public String getPrompt() {
		return prompt;
	}

	public SimpleConsoleInput setPrompt(final String prompt) {
		this.prompt = (prompt == null ? "" : prompt);
		return this;
	}

	public String getPresetContent() {
		return presetContent;
	}

	public SimpleConsoleInput setPresetContent(final String presetContent) {
		this.presetContent = (presetContent == null ? "" : presetContent);
		return this;
	}

	public List<String> getAutoCompletionStrings() {
		return autoCompletionStrings;
	}

	public SimpleConsoleInput setAutoCompletionStrings(final List<String> autoCompletionStrings) {
		this.autoCompletionStrings = autoCompletionStrings;
		return this;
	}

	public SimpleConsoleInput addAutoCompletionString(final String autoCompletionString) {
		if (autoCompletionStrings == null) {
			autoCompletionStrings = new ArrayList<>();
		}
		autoCompletionStrings.add(autoCompletionString);
		return this;
	}

	public String readInput() throws Exception {
		if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			boolean insertMode = true;
			String content = presetContent == null ? "" : presetContent;
			int cursorIndex = content.length();
			final int previousContentLength = content.length();

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
					if (hideCharacter == null) {
						System.out.print(content.substring(0, cursorIndex));
						if (content.length() > cursorIndex) {
							System.out.print(ConsoleUtilities.getAnsiFormattedText(content.substring(cursorIndex, cursorIndex + 1), null, null, cursorTextAttributes) + content.substring(cursorIndex + 1));
						} else {
							System.out.print(ConsoleUtilities.getAnsiFormattedText(" ", null, null, cursorTextAttributes));
						}
					} else {
						System.out.print(Utilities.repeat(hideCharacter, cursorIndex));
						if (content.length() > cursorIndex) {
							System.out.print(ConsoleUtilities.getAnsiFormattedText(hideCharacter.toString(), null, null, cursorTextAttributes) + Utilities.repeat(hideCharacter, content.length() - cursorIndex - 1));
						} else {
							System.out.print(ConsoleUtilities.getAnsiFormattedText(" ", null, null, cursorTextAttributes));
						}
					}

					// Linux needs some extra cleared chars
					if (content.length() - previousContentLength > 0) {
						System.out.print(Utilities.repeat(" ", content.length() - previousContentLength));
					}

					ConsoleUtilities.clearLineContentAfterCursor();

					ConsoleUtilities.moveCursorToSavedPosition();

					final int keyCode = ConsoleUtilities.readKey();

					if (keyCode == ConsoleUtilities.KeyCode_CtrlC) {
						ConsoleUtilities.showCursor();
						System.exit(1);
					} else if (keyCode == ConsoleUtilities.KeyCode_CtrlV) {
						final String data = (String) Toolkit.getDefaultToolkit().getSystemClipboard().getData(DataFlavor.stringFlavor);
						if (insertMode || cursorIndex == content.length()) {
							content = content.substring(0, cursorIndex) + data + content.substring(cursorIndex);
						} else {
							final String rest = content.substring(cursorIndex);
							content = content.substring(0, cursorIndex) + data + (rest.length() > data.length() ? rest.substring(data.length()) : "");
						}
						cursorIndex += data.length();
					} else if (keyCode == ConsoleUtilities.KeyCode_Escape) {
						// do nothing
					} else if (keyCode == ConsoleUtilities.KeyCode_Enter) {
						// Remove special displayed cursor
						ConsoleUtilities.moveCursorToSavedPosition();
						System.out.print(prompt);
						System.out.println(content + " ");
						ConsoleUtilities.showCursor();
						return content;
					} else if (keyCode == ConsoleUtilities.KeyCode_Backspace) {
						if (cursorIndex > 0) {
							content = content.substring(0, cursorIndex - 1) + content.substring(cursorIndex);
							cursorIndex--;
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_Delete) {
						if (cursorIndex < content.length()) {
							content = content.substring(0, cursorIndex) + content.substring(cursorIndex + 1);
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
						if (cursorIndex < content.length()) {
							cursorIndex++;
						} else {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowDown) {
						final List<String> autoCompletionStringsToCheck = getAutoCompletionStrings(content, keyCode);
						if (autoCompletionStringsToCheck != null && autoCompletionStringsToCheck.size() > 0) {
							int currentIndexOfAutocompletion = -1;
							for (int i = 0; i < autoCompletionStringsToCheck.size(); i++) {
								if (autoCompletionStringsToCheck.get(i) != null && autoCompletionStringsToCheck.get(i).equals(content)) {
									currentIndexOfAutocompletion = i;
									break;
								}
							}

							if (currentIndexOfAutocompletion == -1) {
								currentIndexOfAutocompletion = autoCompletionStringsToCheck.size() - 1;
							}

							int nextIndexOfAutocompletion;
							if (currentIndexOfAutocompletion < (autoCompletionStringsToCheck.size() - 1)) {
								nextIndexOfAutocompletion = currentIndexOfAutocompletion + 1;
							} else {
								nextIndexOfAutocompletion = 0;
							}

							content = autoCompletionStringsToCheck.get(nextIndexOfAutocompletion);
							cursorIndex = content.length();
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_ArrowUp) {
						final List<String> autoCompletionStringsToCheck = getAutoCompletionStrings(content, keyCode);
						if (autoCompletionStringsToCheck != null && autoCompletionStringsToCheck.size() > 0) {
							int currentIndexOfAutocompletion = -1;
							for (int i = 0; i < autoCompletionStringsToCheck.size(); i++) {
								if (autoCompletionStringsToCheck.get(i) != null && autoCompletionStringsToCheck.get(i).equals(content)) {
									currentIndexOfAutocompletion = i;
									break;
								}
							}

							if (currentIndexOfAutocompletion == -1) {
								currentIndexOfAutocompletion = 0;
							}

							int nextIndexOfAutocompletion;
							if (currentIndexOfAutocompletion > 0) {
								nextIndexOfAutocompletion = currentIndexOfAutocompletion - 1;
							} else {
								nextIndexOfAutocompletion = autoCompletionStringsToCheck.size() - 1;
							}

							content = autoCompletionStringsToCheck.get(nextIndexOfAutocompletion);
							cursorIndex = content.length();
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_PageDown) {
						final List<String> autoCompletionStringsToCheck = getAutoCompletionStrings(content, keyCode);
						if (autoCompletionStringsToCheck != null && autoCompletionStringsToCheck.size() > 0) {
							int autoCompletionIndex = -1;
							for (int i = 0; i < autoCompletionStringsToCheck.size(); i++) {
								if (content.equals(autoCompletionStringsToCheck.get(i))) {
									autoCompletionIndex = i;
									break;
								}
							}
							if (autoCompletionIndex == autoCompletionStringsToCheck.size() - 1) {
								content = autoCompletionStringsToCheck.get(0);
								cursorIndex = content.length();
							} else if (autoCompletionIndex >= 0) {
								content = autoCompletionStringsToCheck.get(autoCompletionIndex + 1);
								cursorIndex = content.length();
							} else if (!silent) {
								Toolkit.getDefaultToolkit().beep();
							}
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_PageUp) {
						final List<String> autoCompletionStringsToCheck = getAutoCompletionStrings(content, keyCode);
						if (autoCompletionStringsToCheck != null && autoCompletionStringsToCheck.size() > 0) {
							int autoCompletionIndex = -1;
							for (int i = 0; i < autoCompletionStringsToCheck.size(); i++) {
								if (content.equals(autoCompletionStringsToCheck.get(i))) {
									autoCompletionIndex = i;
									break;
								}
							}
							if (autoCompletionIndex == 0) {
								content = autoCompletionStringsToCheck.get(autoCompletionStringsToCheck.size() - 1);
								cursorIndex = content.length();
							} else if (autoCompletionIndex > 0) {
								content = autoCompletionStringsToCheck.get(autoCompletionIndex - 1);
								cursorIndex = content.length();
							} else if (!silent) {
								Toolkit.getDefaultToolkit().beep();
							}
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else if (keyCode == ConsoleUtilities.KeyCode_Home) {
						cursorIndex = 0;
					} else if (keyCode == ConsoleUtilities.KeyCode_End) {
						cursorIndex = content.length();
					} else if (keyCode == ConsoleUtilities.KeyCode_Insert) {
						insertMode = !insertMode;
					} else if (keyCode == ConsoleUtilities.KeyCode_Tab) {
						final String prefix = content.substring(0, cursorIndex);
						final List<String> autoCompletionStringsToCheck = getAutoCompletionStrings(prefix, keyCode);
						if (autoCompletionStringsToCheck != null) {
							String autoCompletionStringCandidate = null;
							for (final String autoCompletionString : autoCompletionStringsToCheck) {
								if (autoCompletionString != null && autoCompletionString.startsWith(prefix)) {
									if (autoCompletionStringCandidate == null) {
										autoCompletionStringCandidate = autoCompletionString;
									} else {
										autoCompletionStringCandidate = null;
										if (!silent) {
											Toolkit.getDefaultToolkit().beep();
										}
										break;
									}
								}
							}
							if (autoCompletionStringCandidate != null) {
								content = autoCompletionStringCandidate + content.substring(cursorIndex);
								cursorIndex = autoCompletionStringCandidate.length();
							} else if (!silent) {
								Toolkit.getDefaultToolkit().beep();
							}
						} else if (!silent) {
							Toolkit.getDefaultToolkit().beep();
						}
					} else {
						// "keyCode < 1000" to prevent Chinese symbols on some systems, that occur without intention
						if ((allowedCharacters != null && allowedCharacters.contains("" + (char) keyCode)) || keyCode < 1000) {
							if (insertMode || cursorIndex == content.length()) {
								content = content.substring(0, cursorIndex) + ((char) keyCode) + content.substring(cursorIndex);
								cursorIndex++;
							} else {
								content = content.substring(0, cursorIndex) + ((char) keyCode) + content.substring(cursorIndex + 1);
								cursorIndex++;
							}
						}
					}
				}
			} finally {
				ConsoleUtilities.showCursor();
			}
		} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
			if (hideCharacter != null) {
				return new String(System.console().readPassword(prompt));
			} else {
				return System.console().readLine(prompt);
			}
		} else {
			if (hideCharacter != null) {
				return new String(System.console().readPassword(prompt));
			} else {
				return System.console().readLine(prompt);
			}
		}
	}

	/**
	 * Override hook for other autoComplete functions
	 *
	 * @param checkContent
	 * @param keyCode
	 */
	protected List<String> getAutoCompletionStrings(final String checkContent, final int keyCode) {
		return autoCompletionStrings;
	}
}
