package de.soderer.utilities.swing;

import java.awt.Dialog;
import java.awt.Window;

import javax.swing.JDialog;

public abstract class ModalDialog<T> extends JDialog {
	private static final long serialVersionUID = -528812797227988224L;

	protected T returnValue;

	public ModalDialog(final Window owner, final String title) {
		super(owner, title, Dialog.ModalityType.DOCUMENT_MODAL);
	}

	public T open() {
		setVisible(true);

		if (returnValue == null) {
			return getDefaultReturnValue();
		} else {
			return returnValue;
		}
	}

	protected T getDefaultReturnValue() {
		return null;
	}
}
