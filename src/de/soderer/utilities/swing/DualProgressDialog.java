package de.soderer.utilities.swing;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.time.LocalDateTime;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import de.soderer.utilities.worker.WorkerDual;
import de.soderer.utilities.worker.WorkerParentDual;

public class DualProgressDialog<T extends WorkerDual<?>> extends ProgressDialog<T> implements WorkerParentDual {
	private static final long serialVersionUID = 1891862599169713021L;

	private JLabel subItemLabel;
	private String subCommentStringFormat;
	private JProgressBar subItemProgressBar;

	public DualProgressDialog(final WorkerParentDual parent, final String title, final String commentStringFormat) {
		this((Window) parent, title, commentStringFormat, null);
	}

	public DualProgressDialog(final Window parent, final String title, final String commentStringFormat, final T worker) {
		super(parent, title, commentStringFormat, worker);
	}

	@Override
	protected void createAdditionalComponents(final JPanel panel) {
		final JPanel subCommentLabelPanel = new JPanel();
		subCommentLabelPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		subCommentStringFormat = "Item";
		subItemLabel = new JLabel("Item");
		subCommentLabelPanel.add(subItemLabel);
		panel.add(subCommentLabelPanel);

		final JPanel subItemPanel = new JPanel();
		subItemPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		subItemProgressBar = new JProgressBar(0, 100);
		subItemProgressBar.setPreferredSize(new Dimension(380, 19));
		subItemPanel.add(subItemProgressBar);
		panel.add(subItemPanel);
	}

	@Override
	public void showUnlimitedSubProgress() {
		if (SwingUtilities.isEventDispatchThread()) {
			subItemProgressBar.setIndeterminate(true);
			subItemProgressBar.setStringPainted(false);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					showUnlimitedSubProgress();
				}
			});
		}
	}

	@Override
	public void showItemStart(final String itemName) {
		if (SwingUtilities.isEventDispatchThread()) {
			subCommentStringFormat = itemName;
			subItemLabel.setText(itemName);

			updateProgressBar(subItemProgressBar, LocalDateTime.now(), 100, 0);
			final String labelText = subCommentStringFormat.replace("{0}", Long.toString(0)).replace("{1}", Long.toString(100)).replace("{2}", Long.toString(0));
			subItemLabel.setText(labelText);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					showItemStart(itemName);
				}
			});
		}
	}

	@Override
	public void showItemProgress(final LocalDateTime itemStart, final long subItemsToDo, final long subItemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			updateProgressBar(subItemProgressBar, itemStart, subItemsToDo, subItemsDone);
			final String labelText = subCommentStringFormat.replace("{0}", Long.toString(subItemsDone)).replace("{1}", Long.toString(subItemsToDo)).replace("{2}", Long.toString(subItemsDone * 100 / subItemsToDo));
			subItemLabel.setText(labelText);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					showItemProgress(itemStart, subItemsToDo, subItemsDone);
				}
			});
		}
	}

	@Override
	public void showItemDone(final LocalDateTime itemStart, final LocalDateTime itemEnd, final long subItemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			updateProgressBar(subItemProgressBar, itemStart, subItemsDone, subItemsDone);
			final String labelText = subCommentStringFormat.replace("{0}", Long.toString(subItemsDone)).replace("{1}", Long.toString(subItemsDone)).replace("{2}", Long.toString(100));
			subItemLabel.setText(labelText);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					showItemDone(itemStart, itemEnd, subItemsDone);
				}
			});
		}
	}
}
