package de.soderer.utilities.swing;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Locale;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Result;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;

public class ProgressDialog<T extends WorkerSimple<?>> extends ModalDialog<Result> implements WorkerParentSimple {
	private static final long serialVersionUID = 4039220978715450890L;

	protected JLabel commentLabel;
	protected String commentStringFormat;

	protected T worker;

	private JProgressBar progressBar;
	protected JButton buttonClose;

	protected boolean canceledByUser = false;

	public ProgressDialog(final Window parent, final String title, final String commentStringFormat) {
		this(parent, title, commentStringFormat, (T) null);
	}

	public ProgressDialog(final Window parent, final String title, final String commentStringFormat, final T worker) {
		super(parent, title);

		setTitle(title);

		this.commentStringFormat = commentStringFormat;

		this.worker = worker;
		if (worker != null) {
			worker.setParent(this);
		}

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		if (Utilities.isNotBlank(commentStringFormat) && commentLabel != null) {
			final JPanel commentLabelPanel = new JPanel();
			commentLabelPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
			final String labelText = commentStringFormat.replace("{0}", Long.toString(0)).replace("{1}", Long.toString(0)).replace("{2}", Long.toString(0));
			commentLabel.setText(labelText);
			commentLabelPanel.add(commentLabel);
			panel.add(commentLabelPanel);
		}

		final JPanel itemPanel = new JPanel();
		itemPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		progressBar = new JProgressBar(0, 100);
		progressBar.setPreferredSize(new Dimension(380, 19));
		itemPanel.add(progressBar);
		panel.add(itemPanel);

		createAdditionalComponents(panel);

		// Cancel Button
		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		buttonClose = new JButton(LangResources.get("cancel"));
		buttonClose.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (new QuestionDialog(parent, getI18NString("cancel"), getI18NString("sureQuestion"), getI18NString("no"), getI18NString("yes")).open() == 1) {
					cancel();
				}
			}
		});
		buttonPanel.add(buttonClose);
		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(parent);

		getRootPane().setDefaultButton(buttonClose);
	}

	protected void createAdditionalComponents(@SuppressWarnings("unused") final JPanel panel) {
		// May be filled in sub classes
	}

	public T getWorker() {
		return worker;
	}

	@Override
	public Result open() {
		if (worker != null) {
			new Thread(worker).start();
		}

		setVisible(true);

		if (canceledByUser) {
			return Result.CANCELED;
		} else if (worker == null || worker.getError() == null) {
			return Result.OK;
		} else {
			return Result.ERROR;
		}
	}

	@Override
	public void receiveUnlimitedProgressSignal() {
		if (SwingUtilities.isEventDispatchThread()) {
			progressBar.setIndeterminate(true);
			progressBar.setStringPainted(false);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					receiveUnlimitedProgressSignal();
				}
			});
		}
	}

	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDo, final long itemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			updateProgressBar(progressBar, start, itemsToDo, itemsDone);
			if (commentLabel != null) {
				final String labelText = commentStringFormat.replace("{0}", Long.toString(itemsDone)).replace("{1}", Long.toString(itemsToDo)).replace("{2}", Long.toString(itemsDone * 100 / itemsToDo));
				commentLabel.setText(labelText);
			}
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					receiveProgressSignal(start, itemsToDo, itemsDone);
				}
			});
		}
	}

	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			dispose();
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					receiveDoneSignal(start, end, itemsDone);
				}
			});
		}
	}

	@Override
	public boolean cancel() {
		if (SwingUtilities.isEventDispatchThread()) {
			if (!canceledByUser) {
				canceledByUser = true;
				buttonClose.setEnabled(false);
				if (worker != null) {
					worker.cancel();
				}
				if (progressBar.isIndeterminate()) {
					progressBar.setIndeterminate(false);
				}
				if (getParent() != null && getParent() instanceof WorkerParentSimple) {
					((WorkerParentSimple) getParent()).cancel();
				}
			}
		} else {
			if (!canceledByUser) {
				SwingUtilities.invokeLater(new Runnable() {
					@Override
					public void run() {
						cancel();
					}
				});
			}
		}
		return true;
	}

	protected static void updateProgressBar(final JProgressBar progressBar, final LocalDateTime start, final long itemsToDo, final long itemsDone) {
		int value;
		if (itemsToDo > 0) {
			value = (int) (itemsDone * 100 / itemsToDo);
		} else {
			value = 0;
		}

		if (value < 0) {
			value = 0;
		} else if (value > 100) {
			value = 100;
		}

		if (progressBar.isIndeterminate()) {
			progressBar.setIndeterminate(false);
		}
		progressBar.setValue(value);
		progressBar.setString(value + "%");
		progressBar.setStringPainted(true);

		final LocalDateTime estimatedEnd = DateUtilities.calculateETA(start, itemsToDo, itemsDone);
		if (estimatedEnd != null) {
			progressBar.setToolTipText("ETA: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), estimatedEnd));
		} else {
			progressBar.setToolTipText(null);
		}
	}

	@Override
	public void changeTitle(final String newTitle) {
		setTitle(newTitle);
	}

	private static String getI18NString(final String resourceKey, final Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "cancel": pattern = "Abbrechen"; break;
				case "yes": pattern = "Ja"; break;
				case "no": pattern = "Nein"; break;
				case "sureQuestion": pattern = "Sind sie sich sicher?"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + (arguments != null && arguments.length > 0 ? " Argumente: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "cancel": pattern = "Cancel"; break;
				case "yes": pattern = "Yes"; break;
				case "no": pattern = "No"; break;
				case "sureQuestion": pattern = "Are you sure?"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + (arguments != null && arguments.length > 0 ? " arguments: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}
