package de.soderer.utilities.swing;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;

public class DualProgressDialog<T extends WorkerDual<?>> extends SimpleProgressDialog<T> implements WorkerParentDual {
	private static final long serialVersionUID = 1891862599169713021L;
	
	private JLabel label;
	private JProgressBar subItemProgressBar;

	public DualProgressDialog(WorkerParentDual parent, String title) {
		this((Window) parent, title, null);
	}

	public DualProgressDialog(Window parent, String title, T worker) {
		super(parent);
		
		setTitle(title);
		
		this.worker = worker;
		if (worker != null) {
			worker.setParent(this);
		}

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		JPanel itemPanel = new JPanel();
		itemPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		progressBar = new JProgressBar(0, 100);
		progressBar.setPreferredSize(new Dimension(380, 19));
		itemPanel.add(progressBar);
		panel.add(itemPanel);

		JPanel labelPanel = new JPanel();
		labelPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		label = new JLabel("Item");
		labelPanel.add(label);
		panel.add(labelPanel);

		JPanel subItemPanel = new JPanel();
		subItemPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		subItemProgressBar = new JProgressBar(0, 100);
		subItemProgressBar.setPreferredSize(new Dimension(380, 19));
		subItemPanel.add(subItemProgressBar);
		panel.add(subItemPanel);

		// Cancel Button
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		JButton cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				cancel();
			}
		});
		buttonPanel.add(cancelButton);
		panel.add(buttonPanel);

		add(panel);

		pack();

        setLocationRelativeTo((Window) parent);
		
		if (cancelButton != null) {
			getRootPane().setDefaultButton(cancelButton);
		}
	}

	@Override
	public void showUnlimitedSubProgress() {
		if (SwingUtilities.isEventDispatchThread()) {
			subItemProgressBar.setIndeterminate(true);
			subItemProgressBar.setStringPainted(false);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showUnlimitedSubProgress();
				}
			});
		}
	}

	@Override
	public void showItemStart(final String itemName) {
		if (SwingUtilities.isEventDispatchThread()) {
			label.setText(itemName);
			
			int value = 0;
			subItemProgressBar.setIndeterminate(false);
			subItemProgressBar.setValue(value);
			subItemProgressBar.setString(value + "%");
			subItemProgressBar.setStringPainted(true);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showItemStart(itemName);
				}
			});
		}
	}

	@Override
	public void showItemProgress(final Date itemStart, final long subItemToDo, final long subItemDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			updateProgressBar(subItemProgressBar, itemStart, subItemToDo, subItemDone);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showItemProgress(itemStart, subItemToDo, subItemDone);
				}
			});
		}
	}

	@Override
	public void showItemDone(final Date itemStart, final Date itemEnd, final long subItemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			int value = 100;
			subItemProgressBar.setIndeterminate(false);
			subItemProgressBar.setValue(value);
			subItemProgressBar.setString(value + "%");
			subItemProgressBar.setStringPainted(true);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showItemDone(itemStart, itemEnd, subItemsDone);
				}
			});
		}
	}
}
