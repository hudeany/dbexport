package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.util.Arrays;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;

import de.soderer.utilities.Credentials;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.SecureDataStore;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WrongPasswordException;

public class SecurePreferencesDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = -2855354859482098916L;

	private final File securePreferencesFile;
	private SecureDataStore secureDataStore = null;
	private Object currentSecureDataEntry;

	private final String text_Username;
	private final String text_PasswordText;
	private final String text_Password;
	private final String text_OK;
	private final String text_Cancel;

	private final JButton loadButton;
	private final JButton createButton;
	private final JButton updateButton;
	private final JButton deleteButton;

	/**
	 * Store latestPassword as String instead of char[], so it may be used even after the Credentialdialogs JTextField password cleanup
	 */
	private char[] latestPassword = null;

	private final JTextField newNameField;
	private final JTable preferencesTable;

	public SecurePreferencesDialog(final Window parent, final String title, final String text, final File securePreferencesFile,
			final String textButton_Load, final String textButton_Create, final String textButton_Update, final String textButton_Delete, final String textButton_Save, final String textButton_Cancel, final String text_PasswordText,
			final String text_Username, final String text_Password, final String text_OK, final String text_Cancel) {
		super(parent, title);

		this.text_Username = text_Username;
		this.text_PasswordText = text_PasswordText;
		this.text_Password = text_Password;
		this.text_OK = text_OK;
		this.text_Cancel = text_Cancel;

		this.securePreferencesFile = securePreferencesFile;

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel interimPanel = new JPanel();
		interimPanel.setLayout(new BoxLayout(interimPanel, BoxLayout.LINE_AXIS));

		interimPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JPanel comboPanel = new JPanel();
		comboPanel.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		comboPanel.setLayout(new BoxLayout(comboPanel, BoxLayout.PAGE_AXIS));

		newNameField = new JTextField();
		newNameField.setToolTipText(text);
		newNameField.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void insertUpdate(final DocumentEvent event) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}

			@Override
			public void removeUpdate(final DocumentEvent event) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}

			@Override
			public void changedUpdate(final DocumentEvent event) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}
		});
		comboPanel.add(newNameField);

		preferencesTable = new JTable(new DefaultTableModel(0, 1));
		preferencesTable.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		preferencesTable.setToolTipText(text);
		preferencesTable.getSelectionModel().addListSelectionListener(new ListSelectionListener(){
			@Override
			public void valueChanged(final ListSelectionEvent event) {
				loadButton.setEnabled(preferencesTable.getSelectedRows().length == 1);
				updateButton.setEnabled(preferencesTable.getSelectedRows().length == 1);
				deleteButton.setEnabled(preferencesTable.getSelectedRows().length > 0);
			}
		});
		preferencesTable.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(final MouseEvent event) {
				if (event.getClickCount() >= 2) {
					final int rowIndex = preferencesTable.rowAtPoint(event.getPoint());
					if (rowIndex >= 0) {
						final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
						final String entryName = (String) tableModel.getValueAt(rowIndex, 0);
						currentSecureDataEntry = secureDataStore.getEntry(currentSecureDataEntry.getClass(), entryName);
						dispose();
					}
				}
			}
		});
		comboPanel.add(preferencesTable);

		interimPanel.add(comboPanel);

		interimPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(interimPanel);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		// Button Panel
		final JPanel buttonPanel1 = new JPanel();
		buttonPanel1.setLayout(new BoxLayout(buttonPanel1, BoxLayout.LINE_AXIS));

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		loadButton = new JButton(textButton_Load);
		loadButton.setEnabled(false);
		loadButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (preferencesTable.getSelectedRows().length == 1) {
					final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					final String entryName = (String) tableModel.getValueAt(preferencesTable.getSelectedRows()[0], 0);
					currentSecureDataEntry = secureDataStore.getEntry(currentSecureDataEntry.getClass(), entryName);
					dispose();
				}
			}
		});
		buttonPanel1.add(loadButton);

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		createButton = new JButton(textButton_Create);
		createButton.setEnabled(false);
		createButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (secureDataStore == null) {
					secureDataStore = new SecureDataStore();
				}

				if (Utilities.isNotBlank(newNameField.getText())) {
					secureDataStore.addEntry(newNameField.getText(), currentSecureDataEntry);
					if (getPassword() == null) {
						final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						final Credentials credentials = credentialsDialog.open();
						if (credentials != null) {
							setPassword(credentials.getPassword());
						} else {
							setPassword(null);
						}
					}
					if (getPassword() != null) {
						try {
							secureDataStore.save(securePreferencesFile, getPassword());
						} catch (final Exception e) {
							e.printStackTrace();
							new QuestionDialog((Window) getParent(), getTitle(), LangResources.get("cannotSaveKeystore"), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
						}
						currentSecureDataEntry = null;
						dispose();
					}
				}
			}
		});
		buttonPanel1.add(createButton);

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		updateButton = new JButton(textButton_Update);
		updateButton.setEnabled(false);
		updateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (preferencesTable.getSelectedRows().length == 1) {
					final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (final int rowNum : preferencesTable.getSelectedRows()) {
						final String entryName = (String) tableModel.getValueAt(rowNum, 0);
						secureDataStore.addEntry(entryName, currentSecureDataEntry);
						if (getPassword() == null) {
							final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
							final Credentials credentials = credentialsDialog.open();
							if (credentials != null) {
								setPassword(credentials.getPassword());
							} else {
								setPassword(null);
							}
						}
						if (getPassword() != null) {
							try {
								secureDataStore.save(securePreferencesFile, getPassword());
							} catch (final Exception e) {
								e.printStackTrace();
								new QuestionDialog((Window) getParent(), getTitle(), LangResources.get("cannotSaveKeystore"), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
							}
							currentSecureDataEntry = null;
							dispose();
						}
					}
				}
			}
		});
		buttonPanel1.add(updateButton);

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		deleteButton = new JButton(textButton_Delete);
		deleteButton.setEnabled(false);
		deleteButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (preferencesTable.getSelectedRows().length > 0) {
					final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (int i = preferencesTable.getSelectedRows().length - 1; i >= 0; i--) {
						final int rowNum = preferencesTable.getSelectedRows()[i];
						final String entryName = (String) tableModel.getValueAt(rowNum, 0);
						secureDataStore.removeEntry(currentSecureDataEntry.getClass(), entryName);
						tableModel.removeRow(rowNum);
					}
					if (getPassword() == null) {
						final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						final Credentials credentials = credentialsDialog.open();
						if (credentials != null) {
							setPassword(credentials.getPassword());
						} else {
							setPassword(null);
						}
					}
					if (getPassword() != null) {
						try {
							secureDataStore.save(securePreferencesFile, getPassword());
						} catch (final Exception e) {
							e.printStackTrace();
							new QuestionDialog((Window) getParent(), getTitle(), LangResources.get("cannotSaveKeystore"), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
						}
						currentSecureDataEntry = null;
						dispose();
					}
				}
			}
		});
		buttonPanel1.add(deleteButton);

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		final JPanel buttonPanel2 = new JPanel();
		buttonPanel2.setLayout(new BoxLayout(buttonPanel2, BoxLayout.LINE_AXIS));

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton saveButton = new JButton(textButton_Save);
		saveButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (secureDataStore == null) {
					secureDataStore = new SecureDataStore();
				}

				if (Utilities.isNotBlank(newNameField.getText())) {
					secureDataStore.addEntry(newNameField.getText(), currentSecureDataEntry);
				} else if (preferencesTable.getSelectedRows().length == 1) {
					final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (final int rowNum : preferencesTable.getSelectedRows()) {
						final String entryName = (String) tableModel.getValueAt(rowNum, 0);
						secureDataStore.addEntry(entryName, currentSecureDataEntry);
						if (getPassword() == null) {
							final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
							final Credentials credentials = credentialsDialog.open();
							if (credentials != null) {
								setPassword(credentials.getPassword());
							} else {
								setPassword(null);
							}
						}
						if (getPassword() != null) {
							try {
								secureDataStore.save(securePreferencesFile, getPassword());
							} catch (final Exception e) {
								e.printStackTrace();
								new QuestionDialog((Window) getParent(), getTitle(), LangResources.get("cannotSaveKeystore"), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
							}
							currentSecureDataEntry = null;
							dispose();
						}
					}
				}

				final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
				final Credentials credentials = credentialsDialog.open();
				if (credentials != null) {
					setPassword(credentials.getPassword());
					try {
						secureDataStore.save(securePreferencesFile, getPassword());
					} catch (final Exception e) {
						e.printStackTrace();
						new QuestionDialog((Window) getParent(), getTitle(), LangResources.get("cannotSaveKeystore"), LangResources.get("ok")).setBackgroundColor(SwingColor.LightRed).open();
					}
					currentSecureDataEntry = null;
					dispose();
				}
			}
		});
		buttonPanel2.add(saveButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton cancelButton = new JButton(textButton_Cancel);
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				currentSecureDataEntry = null;
				dispose();
			}
		});
		buttonPanel2.add(cancelButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(buttonPanel1);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		panel.add(buttonPanel2);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		add(panel);

		pack();

		setLocationRelativeTo(parent);

		newNameField.requestFocus();
	}

	public void setCurrentSecureDataEntry(final Object secureDataEntry) {
		currentSecureDataEntry = secureDataEntry;
	}

	public Object getCurrentSecureDataEntry() {
		return currentSecureDataEntry;
	}

	public char[] getPassword() {
		if (latestPassword == null) {
			return null;
		} else {
			return Arrays.copyOf(latestPassword, latestPassword.length);
		}
	}

	public void setPassword(final char[] password) {
		latestPassword = password;
	}

	@Override
	public Boolean open() {
		try {
			if (securePreferencesFile.exists()) {
				boolean retry = true;
				while (secureDataStore == null && retry) {
					try {
						secureDataStore = new SecureDataStore();
						secureDataStore.load(securePreferencesFile, getPassword());
					} catch (@SuppressWarnings("unused") final WrongPasswordException e) {
						secureDataStore = null;
						final CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						final Credentials credentials = credentialsDialog.open();
						if (credentials != null) {
							setPassword(credentials.getPassword());
						} else {
							setPassword(null);
							retry = false;
						}
					}
				}

				if (secureDataStore != null) {
					final DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (final String entryName : secureDataStore.getEntryNames(currentSecureDataEntry.getClass())) {
						tableModel.addRow(new Object[] { entryName });
					}

					pack();

					setVisible(true);
				}
			} else {
				setVisible(true);
			}
		} catch (final Exception e) {
			setPassword(null);
			new QuestionDialog((Window) getParent(), "ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
		}
		return true;
	}
}
