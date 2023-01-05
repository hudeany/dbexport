package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;

import de.soderer.utilities.Utilities;

public class SelectableTableDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = -718203702968699038L;

	private JTable table;

	private List<List<String>> selectedItems;

	public SelectableTableDialog(final Window parent, final String title, final String text, final List<String> headers, final List<List<String>> items, final boolean defaultState, final String selectAllButtonText, final String selectNoneButtonText, final String selectButtonText, final String cancelButtonText) {
		super(parent, title);

		final SelectableTableDialog dialog = this;

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
		final Object[] headerArray = new Object[headers.size() + 1];
		headerArray[0] = "";
		for (int i = 0; i < headers.size(); i++) {
			headerArray[i + 1] = headers.get(i);
		}

		final Object[][] itemsArray = new Object[items.size()][];
		for (int i = 0; i < items.size(); i++) {
			final Object[] itemArray = new Object[items.get(i).size() + 1];
			itemArray[0] = defaultState;
			for (int j = 0; j < items.get(i).size(); j++) {
				itemArray[j + 1] = items.get(i).get(j);
			}
			itemsArray[i] = itemArray;
		}

		final DefaultTableModel tableModel = new DefaultTableModel(itemsArray, headerArray) {
			private static final long serialVersionUID = -372185535503984868L;

			@Override
			public Class<?> getColumnClass(final int columnIndex) {
				if (columnIndex == 0) {
					return Boolean.class;
				} else {
					return String.class;
				}
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

		// Button Panel 1
		final JPanel buttonPanel1 = new JPanel();
		buttonPanel1.setLayout(new BoxLayout(buttonPanel1, BoxLayout.LINE_AXIS));

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		if (Utilities.isNotBlank(selectAllButtonText)) {
			final JButton selectAllButton = new JButton(selectAllButtonText);
			selectAllButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(final ActionEvent event) {
					for (int rowIndex = 0; rowIndex < table.getRowCount(); rowIndex++) {
						for (int searchIndex = 0; searchIndex < table.getColumnCount(); searchIndex++) {
							final TableColumn column = table.getColumnModel().getColumn(searchIndex);
							if ("".equals(column.getHeaderValue())) {
								table.getModel().setValueAt(true, rowIndex, searchIndex);
							}
						}
					}
				}
			});
			buttonPanel1.add(selectAllButton);

			buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));
		}

		if (Utilities.isNotBlank(selectNoneButtonText)) {
			final JButton selectNoneButton = new JButton(selectNoneButtonText);
			selectNoneButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(final ActionEvent event) {
					for (int rowIndex = 0; rowIndex < table.getRowCount(); rowIndex++) {
						for (int searchIndex = 0; searchIndex < table.getColumnCount(); searchIndex++) {
							final TableColumn column = table.getColumnModel().getColumn(searchIndex);
							if ("".equals(column.getHeaderValue())) {
								table.getModel().setValueAt(false, rowIndex, searchIndex);
							}
						}
					}
				}
			});
			buttonPanel1.add(selectNoneButton);

			buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));
		}

		// Button Panel 2
		final JPanel buttonPanel2 = new JPanel();
		buttonPanel2.setLayout(new BoxLayout(buttonPanel2, BoxLayout.LINE_AXIS));

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton selectButton = new JButton(selectButtonText);
		selectButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				selectedItems = new ArrayList<>();
				for (int rowIndex = 0; rowIndex < table.getRowCount(); rowIndex++) {
					boolean selected = false;
					for (int searchIndex = 0; searchIndex < table.getColumnCount(); searchIndex++) {
						final TableColumn column = table.getColumnModel().getColumn(searchIndex);
						if ("".equals(column.getHeaderValue())) {
							selected = (Boolean) table.getModel().getValueAt(rowIndex, searchIndex);
						}
					}
					if (selected) {
						final List<String> item = new ArrayList<>();
						for (final String columnName : headers) {
							int columnIndex = 0;
							for (int searchIndex = 0; searchIndex < table.getColumnCount(); searchIndex++) {
								final TableColumn column = table.getColumnModel().getColumn(searchIndex);
								if (columnName.equals(column.getHeaderValue())) {
									columnIndex = searchIndex;
									break;
								}
							}
							if (columnIndex > 0) {
								item.add((String) table.getModel().getValueAt(rowIndex, columnIndex));
							}
						}
						selectedItems.add(item);
					}
				}

				dialog.dispose();
			}
		});
		buttonPanel2.add(selectButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton cancelButton = new JButton(cancelButtonText);
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				selectedItems = null;
				dialog.dispose();
			}
		});
		buttonPanel2.add(cancelButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		panel.add(tablePanel);
		if (Utilities.isNotBlank(selectAllButtonText) || Utilities.isNotBlank(selectNoneButtonText)) {
			panel.add(buttonPanel1);
			add(Box.createRigidArea(new Dimension(0, 5)));
		}
		panel.add(buttonPanel2);

		add(panel);

		pack();

		setLocationRelativeTo(parent);

		getRootPane().setDefaultButton(selectButton);
	}

	public List<List<String>> getSelectedItems() {
		return selectedItems;
	}
}