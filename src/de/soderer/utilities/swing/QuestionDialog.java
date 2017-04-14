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

public class QuestionDialog extends JDialog {
	private static final long serialVersionUID = 2020023796052296409L;
	
	private int answerButtonIndex = -1;

	public QuestionDialog(Window parent, String title, String text, String... buttonTexts) {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);

		final QuestionDialog questionDialog = this;

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

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

		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		
		JButton focusButton = null;
		
		if (buttonTexts != null && buttonTexts.length > 0) {
			for (int i = 0; i < buttonTexts.length; i++) {
				if (i == 0) {
					buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));
				}
				
				final int answerButtonIndex = i;
				
				JButton someButton = new JButton(buttonTexts[i]);
				someButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent event) {
						questionDialog.setAnswerButtonIndex(answerButtonIndex);
						questionDialog.dispose();
					}
				});
				buttonPanel.add(someButton);
				
				if (i == 0) {
					focusButton = someButton;
				}
			}
		} else {
			JButton cancelButton = new JButton("Close");
			cancelButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent event) {
					questionDialog.dispose();
				}
			});
			buttonPanel.add(cancelButton);
			
			focusButton = cancelButton;
		}

		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		panel.add(textPanel);
		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(parent);
		
		if (focusButton != null) {
			getRootPane().setDefaultButton(focusButton);
		}
	}

	public int getAnswerButtonIndex() {
		return answerButtonIndex;
	}

	private void setAnswerButtonIndex(int answerButtonIndex) {
		this.answerButtonIndex = answerButtonIndex;
	}
}