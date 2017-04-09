package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;

import de.soderer.utilities.LangResources;
import de.soderer.utilities.SecureDataEntry;
import de.soderer.utilities.SecureDataKeyStore;
import de.soderer.utilities.Utilities;

public class SecurePreferencesDialog extends JDialog {
	private static final long serialVersionUID = -2855354859482098916L;
	
	private File securePreferencesFile;
	private SecureDataKeyStore secureDataKeyStore;
	private SecureDataEntry currentSecureDataEntry;
	
	// I18N texts for CredentialsDialog 
	private String text_Username;
	private String text_PasswordText;
	private String text_Password;
	private String text_OK;
	private String text_Cancel;
	
	private JButton loadButton;
	private JButton createButton;
	private JButton updateButton;
	private JButton deleteButton;

	/**
	 * Store latestPassword as String instead of char[], so it may be used even after the Credentialdialogs JTextField password cleanup
	 */
	private String latestPassword = null;
	
	private JTextField newNameField;
	private JTable preferencesTable;

	public SecurePreferencesDialog(Window parent, String title, String text, File securePreferencesFile) {
		this(parent, title, text, securePreferencesFile,
			"Load", "Create", "Update", "Delete", "Save with new password", "Cancel", "Please enter password for secured preferences",
			"Username", "Password", "OK", "Cancel");
	}
	
	public SecurePreferencesDialog(Window parent, String title, String text, File securePreferencesFile,
			String textButton_Load, String textButton_Create, String textButton_Update, String textButton_Delete, String textButton_Save, String textButton_Cancel, final String text_PasswordText,
			final String text_Username, final String text_Password, final String text_OK, final String text_Cancel) {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);

		this.text_Username = text_Username;
		this.text_PasswordText = text_PasswordText;
		this.text_Password = text_Password;
		this.text_OK = text_OK;
		this.text_Cancel = text_Cancel;
		
		this.securePreferencesFile = securePreferencesFile;

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
		
		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		JPanel interimPanel = new JPanel();
		interimPanel.setLayout(new BoxLayout(interimPanel, BoxLayout.LINE_AXIS));
		
		interimPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		
		JPanel comboPanel = new JPanel();
		comboPanel.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		comboPanel.setLayout(new BoxLayout(comboPanel, BoxLayout.PAGE_AXIS));
		
		newNameField = new JTextField();
		newNameField.setToolTipText(text);
		newNameField.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void insertUpdate(DocumentEvent e) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}

			@Override
			public void changedUpdate(DocumentEvent e) {
				createButton.setEnabled(newNameField.getText().length() > 0);
			}
		});
		comboPanel.add(newNameField);
		
		preferencesTable = new JTable(new DefaultTableModel(0, 1));
		preferencesTable.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		preferencesTable.setToolTipText(text);
		preferencesTable.getSelectionModel().addListSelectionListener(new ListSelectionListener(){
	        public void valueChanged(ListSelectionEvent event) {
	        	loadButton.setEnabled(preferencesTable.getSelectedRows().length == 1);
	        	updateButton.setEnabled(preferencesTable.getSelectedRows().length == 1);
	        	deleteButton.setEnabled(preferencesTable.getSelectedRows().length > 0);
	        }
	    });
		preferencesTable.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent event) {
				if (event.getClickCount() >= 2) {
			        int rowIndex = preferencesTable.rowAtPoint(event.getPoint());
			        if (rowIndex >= 0) {
			        	DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
						String entryName = (String) tableModel.getValueAt(rowIndex, 0);
						currentSecureDataEntry = secureDataKeyStore.getEntry(currentSecureDataEntry.getClass(), entryName);
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
		JPanel buttonPanel1 = new JPanel();
		buttonPanel1.setLayout(new BoxLayout(buttonPanel1, BoxLayout.LINE_AXIS));
		
		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));
		
		loadButton = new JButton(textButton_Load);
		loadButton.setEnabled(false);
		loadButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				if (preferencesTable.getSelectedRows().length == 1) {
					DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					String entryName = (String) tableModel.getValueAt(preferencesTable.getSelectedRows()[0], 0);
					currentSecureDataEntry = secureDataKeyStore.getEntry(currentSecureDataEntry.getClass(), entryName);
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
			public void actionPerformed(ActionEvent event) {
				if (Utilities.isNotBlank(newNameField.getText())) {
					currentSecureDataEntry.setEntryName(newNameField.getText());
					secureDataKeyStore.addEntry(currentSecureDataEntry);
					if (getPassword() == null) {
						CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						credentialsDialog.setVisible(true);
						if (credentialsDialog.getCredentials() != null) {
							setPassword(credentialsDialog.getCredentials().getPassword());
						} else {
							setPassword(null);
						}
					}
					if (getPassword() != null) {
						secureDataKeyStore.saveKeyStore(getPassword());
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
			public void actionPerformed(ActionEvent event) {
				if (preferencesTable.getSelectedRows().length == 1) {
					DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (int rowNum : preferencesTable.getSelectedRows()) {
						String entryName = (String) tableModel.getValueAt(rowNum, 0);
						currentSecureDataEntry.setEntryName(entryName);
						secureDataKeyStore.addEntry(currentSecureDataEntry);
						if (getPassword() == null) {
							CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
							credentialsDialog.setVisible(true);
							if (credentialsDialog.getCredentials() != null) {
								setPassword(credentialsDialog.getCredentials().getPassword());
							} else {
								setPassword(null);
							}
						}
						if (getPassword() != null) {
							secureDataKeyStore.saveKeyStore(getPassword());
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
			public void actionPerformed(ActionEvent event) {
				if (preferencesTable.getSelectedRows().length > 0) {
					DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (int i = preferencesTable.getSelectedRows().length - 1; i >= 0; i--) {
						int rowNum = preferencesTable.getSelectedRows()[i];
						String entryName = (String) tableModel.getValueAt(rowNum, 0);
						secureDataKeyStore.remove(currentSecureDataEntry.getClass(), entryName);
						tableModel.removeRow(rowNum);
					}
					if (getPassword() == null) {
						CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						credentialsDialog.setVisible(true);
						if (credentialsDialog.getCredentials() != null) {
							setPassword(credentialsDialog.getCredentials().getPassword());
						} else {
							setPassword(null);
						}
					}
					if (getPassword() != null) {
						secureDataKeyStore.saveKeyStore(getPassword());
						currentSecureDataEntry = null;
						dispose();
					}
				}
			}
		});
		buttonPanel1.add(deleteButton);
		
		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));
		
		JPanel buttonPanel2 = new JPanel();
		buttonPanel2.setLayout(new BoxLayout(buttonPanel2, BoxLayout.LINE_AXIS));
		
		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));
		
		JButton saveButton = new JButton(textButton_Save);
		saveButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				if (Utilities.isNotBlank(newNameField.getText())) {
					currentSecureDataEntry.setEntryName(newNameField.getText());
					secureDataKeyStore.addEntry(currentSecureDataEntry);
				} else if (preferencesTable.getSelectedRows().length == 1) {
					DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (int rowNum : preferencesTable.getSelectedRows()) {
						String entryName = (String) tableModel.getValueAt(rowNum, 0);
						currentSecureDataEntry.setEntryName(entryName);
						secureDataKeyStore.addEntry(currentSecureDataEntry);
						if (getPassword() == null) {
							CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
							credentialsDialog.setVisible(true);
							if (credentialsDialog.getCredentials() != null) {
								setPassword(credentialsDialog.getCredentials().getPassword());
							} else {
								setPassword(null);
							}
						}
						if (getPassword() != null) {
							secureDataKeyStore.saveKeyStore(getPassword());
							currentSecureDataEntry = null;
							dispose();
						}
					}
				}
				
				CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
				credentialsDialog.setVisible(true);
				if (credentialsDialog.getCredentials() != null) {
					setPassword(credentialsDialog.getCredentials().getPassword());
					secureDataKeyStore.saveKeyStore(getPassword());
					currentSecureDataEntry = null;
					dispose();
				}
			}
		});
		buttonPanel2.add(saveButton);
		
		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));
		
		JButton cancelButton = new JButton(textButton_Cancel);
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
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
	
	public void setCurrentSecureDataEntry(SecureDataEntry secureDataEntry) {
		this.currentSecureDataEntry = secureDataEntry;
	}
	
	public SecureDataEntry getCurrentSecureDataEntry() {
		return currentSecureDataEntry;
	}
	
	public char[] getPassword() {
		return latestPassword == null ? null : latestPassword.toCharArray();
	}
	
	public void setPassword(char[] password) {
		this.latestPassword = password == null ? null : new String(password);
	}
	
	public void showDialog() {
		try {
			secureDataKeyStore = new SecureDataKeyStore(securePreferencesFile);
			
			if (securePreferencesFile.exists()) {
				if (getPassword() == null) {
					try {
						// Try with insecure empty password
						secureDataKeyStore.loadKeyStore("".toCharArray());
						setPassword("".toCharArray());
					} catch (Exception e) {
						// Do nothing
					}
					
					if (getPassword() == null) {
						CredentialsDialog credentialsDialog = new CredentialsDialog((Window) getParent(), getTitle(), text_PasswordText, false, true, text_Username, text_Password, text_OK, text_Cancel);
						credentialsDialog.setVisible(true);
						if (credentialsDialog.getCredentials() != null) {
							setPassword(credentialsDialog.getCredentials().getPassword());
						} else {
							setPassword(null);
						}
					}
				}
				
				if (getPassword() != null) {
					secureDataKeyStore.loadKeyStore(getPassword());
					DefaultTableModel tableModel = (DefaultTableModel) preferencesTable.getModel();
					for (String entryName : secureDataKeyStore.getEntryNames(currentSecureDataEntry.getClass())) {
						tableModel.addRow(new Object[] { entryName });
					}
					
					pack();
					
					setVisible(true);
				}
			} else {
				setVisible(true);
			}
		} catch (Exception e) {
			setPassword(null);
			TextDialog textDialog = new TextDialog((Window) getParent(), "ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED);
			textDialog.setVisible(true);
		}
	}
}
