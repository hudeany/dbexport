package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.DefaultTableModel;

import de.soderer.utilities.Utilities;

public class TableDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = -718203702968699038L;

	private JTable table;

	public TableDialog(final Window parent, final String title, final String text, final List<String> headers, final List<List<String>> items, final String closeButtonText) {
		super(parent, title);

		final TableDialog dialog = this;

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		if (Utilities.isNotBlank(text)) {
			final JTextArea textField = new JTextArea();
			textField.setText(text);
			textField.setEditable(false);
			textField.setBackground(Color.LIGHT_GRAY);

			final JScrollPane textScrollpane = new JScrollPane(textField);
			textScrollpane.setPreferredSize(new Dimension(textField.getPreferredSize().width + 5, textField.getPreferredSize().height + 5));

			// Text Panel
			final JPanel textPanel = new JPanel();
			textPanel.setLayout(new BoxLayout(textPanel, BoxLayout.LINE_AXIS));

			textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
			textPanel.add(textScrollpane);
			textPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			panel.add(Box.createRigidArea(new Dimension(0, 5)));
			panel.add(textPanel);
		}

		// table area
		final Object[] headerArray = new Object[headers.size()];
		for (int i = 0; i < headers.size(); i++) {
			headerArray[i] = headers.get(i);
		}

		final Object[][] itemsArray = new Object[items.size()][];
		for (int i = 0; i < items.size(); i++) {
			final Object[] itemArray = new Object[items.get(i).size()];
			for (int j = 0; j < items.get(i).size(); j++) {
				itemArray[j] = items.get(i).get(j);
			}
			itemsArray[i] = itemArray;
		}

		final DefaultTableModel tableModel = new DefaultTableModel(itemsArray, headerArray) {
			private static final long serialVersionUID = -372185535503984868L;

			@Override
			public Class<?> getColumnClass(final int columnIndex) {
				return String.class;
			}
		};
		table = new JTable(tableModel);
		table.setBackground(Color.LIGHT_GRAY);

		final JScrollPane tableScrollpane = new JScrollPane(table);
		tableScrollpane.setPreferredSize(new Dimension(table.getPreferredSize().width + 5, 100));

		final JPanel tablePanel = new JPanel();
		tablePanel.setLayout(new BoxLayout(tablePanel, BoxLayout.LINE_AXIS));

		tablePanel.add(Box.createRigidArea(new Dimension(5, 0)));
		tablePanel.add(tableScrollpane);
		tablePanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Button Panel
		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton closeButton = new JButton(closeButtonText);
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				dialog.dispose();
			}
		});
		buttonPanel.add(closeButton);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		panel.add(tablePanel);
		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(parent);getRootPane().setDefaultButton(closeButton);
	}
}