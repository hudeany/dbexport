package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import de.soderer.utilities.Credentials;

public class CredentialsDialog extends JDialog {
	private static final long serialVersionUID = 2020023796052296409L;
	
	private JTextField userField;
	private JTextField passwordField;
	
	private Credentials credentials = null;

	public CredentialsDialog(Window parent, String title, String text, boolean aquireUsername, boolean aquirePassword) {
		this(parent, title, text, aquireUsername, aquirePassword, "Username", "Password", "OK", "Cancel");
	}
	
	public CredentialsDialog(Window parent, String title, String text, boolean aquireUsername, boolean aquirePassword,
			String text_Username, String text_Password, String text_OK, String text_Cancel) {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);

		final CredentialsDialog credentialsDialog = this;

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		JTextArea textField = new JTextArea();
		textField.setText(text);
		textField.setEditable(false);
		textField.setBackground(Color.LIGHT_GRAY);

		JScrollPane textScrollpane = new JScrollPane(textField);
		textScrollpane.setPreferredSize(new Dimension(textField.getPreferredSize().width + 5, textField.getPreferredSize().height + 5));
		
		// Text Panel
		JPanel textPanel = new JPanel();
		textPanel.setLayout(new BoxLayout(textPanel, BoxLayout.LINE_AXIS));

		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		textPanel.add(textScrollpane);
		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		panel.add(textPanel);

		if (aquireUsername) {
			// User Panel
			JPanel userPanel = new JPanel();
			userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
			JLabel userLabel = new JLabel(text_Username);
			userPanel.add(userLabel);
			userField = new JTextField();
			userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
			userPanel.add(userField);
			panel.add(userPanel);
		}

		if (aquirePassword) {
			// Password Panel
			JPanel passwordPanel = new JPanel();
			passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
			JLabel passwordLabel = new JLabel(text_Password);
			passwordPanel.add(passwordLabel);
			passwordField = new JPasswordField();
			passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
			passwordPanel.add(passwordField);
			panel.add(passwordPanel);
		}
		
		// Button Panel
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// ok Button
		JButton okButton = new JButton(text_OK);
		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				if (userField != null && passwordField != null) {
					credentialsDialog.setCredentials(new Credentials(userField.getText(), passwordField.getText().toCharArray()));
				} else if (userField != null) {
					credentialsDialog.setCredentials(new Credentials(userField.getText()));
				} else if (passwordField != null) {
					credentialsDialog.setCredentials(new Credentials(passwordField.getText().toCharArray()));
				}
				dispose();
			}
		});
		buttonPanel.add(okButton);
		
		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Close Button
		JButton closeButton = new JButton(text_Cancel);
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(closeButton);

		panel.add(Box.createRigidArea(new Dimension(5, 0)));
		panel.add(buttonPanel);
		getRootPane().setDefaultButton(okButton);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		add(panel);

		pack();

		setLocationRelativeTo(parent);
		
		if (userField != null) {
			userField.requestFocus();
		} else if (passwordField != null) {
			passwordField.requestFocus();
		}
	}

	public Credentials getCredentials() {
		return credentials;
	}

	private void setCredentials(Credentials credentials) {
		this.credentials = credentials;
	}
}
