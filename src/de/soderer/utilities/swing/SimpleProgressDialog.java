package de.soderer.utilities.swing;

import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.WorkerSimple;

public class SimpleProgressDialog<T extends WorkerSimple<?>> extends JDialog implements WorkerParentSimple {
	private static final long serialVersionUID = 4039220978715450890L;

	public enum Result {
		OK,
		ERROR,
		CANCELED
	}

	protected T worker;
	protected JProgressBar progressBar;
	protected boolean canceledByUser = false;
	
	protected SimpleProgressDialog(Window parent) {
		super(parent, Dialog.ModalityType.DOCUMENT_MODAL);
	}
	
	public SimpleProgressDialog(WorkerParentSimple parent, String title) {
		this((Window) parent, title, (T) null);
	}

	public SimpleProgressDialog(Window parent, String title, T worker) {
		this(parent);
		
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
	
	public T getWorker() {
		return worker;
	}

    public Result showDialog() {
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

    /**
     * @deprecated use method "showDialog()" instead
     */
	@Override
	@Deprecated
    public void setVisible(boolean value) {
        super.setVisible(value);
    }

	@Override
	public void showUnlimitedProgress() {
		if (SwingUtilities.isEventDispatchThread()) {
			progressBar.setIndeterminate(true);
			progressBar.setStringPainted(false);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showUnlimitedProgress();
				}
			});
		}
	}

	@Override
	public void showProgress(final Date start, final long itemsToDo, final long itemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			updateProgressBar(progressBar, start, itemsToDo, itemsDone);
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showProgress(start, itemsToDo, itemsDone);
				}
			});
		}
	}

	@Override
	public void showDone(final Date start, final Date end, final long itemsDone) {
		if (SwingUtilities.isEventDispatchThread()) {
			dispose();
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					showDone(start, end, itemsDone);
				}
			});
		}
	}

	@Override
	public void cancel() {
		if (SwingUtilities.isEventDispatchThread()) {
			dispose();
			canceledByUser = true;
			if (worker != null) {
				worker.cancel();
			}
			if (getParent() != null && getParent() instanceof WorkerParentSimple) {
				((WorkerParentSimple) getParent()).cancel();
			}
		} else {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					cancel();
				}
			});
		}
	}

	protected void updateProgressBar(JProgressBar progressBar, Date start, long itemsToDo, long itemsDone) {
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
		
		progressBar.setIndeterminate(false);
		progressBar.setValue(value);
		progressBar.setString(value + "%");
		progressBar.setStringPainted(true);
		
		Date estimatedEnd = DateUtilities.calculateETA(start, itemsToDo, itemsDone);
		if (estimatedEnd != null) {
			progressBar.setToolTipText(estimatedEnd.toString());
		} else {
			progressBar.setToolTipText(null);
		}
	}

	@Override
	public void changeTitle(String newTitle) {
		setTitle(newTitle);
	}
}
