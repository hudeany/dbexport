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
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class TextDialog extends JDialog {
	private static final long serialVersionUID = 2020023796052296409L;
	
	public TextDialog(Window parent, String title, String text, String closeButtonText) {
		this(parent, title, text, closeButtonText, true, Color.WHITE);
	}
	
	public TextDialog(Window parent, String title, String text, String closeButtonText, Color backgroundcolor) {
		this(parent, title, text, closeButtonText, true, backgroundcolor);
	}
	
	public TextDialog(Window parent, String title, String text, String closeButtonText, boolean wrapLines) {
		this(parent, title, text, closeButtonText, wrapLines, Color.WHITE);
	}

	public TextDialog(Window parent, String title, String text, String closeButtonText, boolean wrapLines, Color backgroundcolor) {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
		
		JTextArea textField = new JTextArea();
		textField.setEditable(false);
		textField.setTabSize(4);
		textField.setText(text);
		
		if (wrapLines) {
			textField.setLineWrap(true);
			textField.setWrapStyleWord(true);
		}
		
		if (backgroundcolor != null) {
			textField.setBackground(backgroundcolor);
		}
		
		JScrollPane textScrollpane = new JScrollPane(textField);
		textScrollpane.setPreferredSize(new Dimension(400, 200));
		
		// Text Panel
		JPanel textPanel = new JPanel();
		textPanel.setLayout(new BoxLayout(textPanel, BoxLayout.LINE_AXIS));

		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		textPanel.add(textScrollpane);
		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		JButton cancelButton = new JButton(closeButtonText);
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(cancelButton);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		panel.add(textPanel);
		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(parent);
		
		getRootPane().setDefaultButton(cancelButton);
	}
}
