package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.MessageFormat;
import java.util.Locale;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;

public class QuestionDialog extends ModalDialog<Integer> {
	private static final long serialVersionUID = 2020023796052296409L;

	private final String text;
	private Color backgroundcolor = null;
	private final String[] buttonTexts;
	private String checkboxText = null;
	private boolean checkboxStatus = false;

	private JPanel panel;

	public QuestionDialog(final Window parent, final String title, final String text, final String... buttonTexts) {
		super(parent, title);

		this.text = text;
		this.buttonTexts = buttonTexts;

		setResizable(false);

		createComponents(this);
	}

	private void createComponents(final QuestionDialog questionDialog) {
		panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JTextArea textField = new JTextArea();
		textField.setText(text);
		textField.setEditable(false);
		if (backgroundcolor != null) {
			textField.setBackground(backgroundcolor);
		}

		final JScrollPane textScrollpane = new JScrollPane(textField);
		textScrollpane.setPreferredSize(new Dimension(Math.min(textField.getPreferredSize().width + 5, 600), Math.min(textField.getPreferredSize().height + 5, 400)));

		// Text Panel
		final JPanel textPanel = new JPanel();
		textPanel.setLayout(new BoxLayout(textPanel, BoxLayout.LINE_AXIS));

		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		textPanel.add(textScrollpane);
		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(textPanel);

		if (Utilities.isNotBlank(checkboxText)) {
			// Checkbox Panel
			final JPanel checkboxPanel = new JPanel();
			checkboxPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
			final JCheckBox checkboxCheckBox = new JCheckBox(checkboxText);
			checkboxCheckBox.setSelected(checkboxStatus);
			checkboxCheckBox.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(final ActionEvent event) {
					checkboxStatus = checkboxCheckBox.isSelected();
				}
			});
			checkboxPanel.add(checkboxCheckBox);

			panel.add(checkboxPanel);
		}

		// Button Panel
		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));

		JButton buttonToSelect = null;

		if (buttonTexts != null && buttonTexts.length > 0) {
			for (int i = 0; i < buttonTexts.length; i++) {
				if (i == 0) {
					buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));
				}

				final int answerButtonIndexFinal = i;

				final JButton nextButton = new JButton(buttonTexts[i]);
				nextButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(final ActionEvent event) {
						returnValue = answerButtonIndexFinal;
						questionDialog.dispose();
					}
				});
				buttonPanel.add(nextButton);

				if (i == 0) {
					buttonToSelect = nextButton;
				}
			}
		} else {
			final JButton cancelButton = new JButton(getI18NString("close"));
			cancelButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(final ActionEvent event) {
					questionDialog.dispose();
				}
			});
			buttonPanel.add(cancelButton);

			buttonToSelect = cancelButton;
		}

		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(getOwner());

		if (buttonToSelect != null) {
			getRootPane().setDefaultButton(buttonToSelect);
		}
	}

	public QuestionDialog setBackgroundColor(final Color backgroundcolor) {
		this.backgroundcolor = backgroundcolor;
		remove(panel);
		createComponents(this);
		return this;
	}

	public QuestionDialog setCheckboxText(final String checkboxText) {
		this.checkboxText = checkboxText;
		remove(panel);
		createComponents(this);
		return this;
	}

	public QuestionDialog setCheckboxStatus(final boolean checkboxStatus) {
		this.checkboxStatus = checkboxStatus;
		remove(panel);
		createComponents(this);
		return this;
	}

	public boolean getCheckboxStatus() {
		return checkboxStatus;
	}

	private static String getI18NString(final String resourceKey, final Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "close": pattern = "Schließen"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + (arguments != null && arguments.length > 0 ? " Argumente: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "close": pattern = "Close"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + (arguments != null && arguments.length > 0 ? " arguments: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}