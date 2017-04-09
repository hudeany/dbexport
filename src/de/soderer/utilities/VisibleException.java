package de.soderer.utilities;

public class VisibleException extends Exception {
	private static final long serialVersionUID = -5425442516236660858L;

	private String visibleMessage = LangResources.get("error_occurred");

	public VisibleException() {
		super();
	}

	public VisibleException(String exceptionMessage) {
		super(exceptionMessage);
	}

	public VisibleException(String exceptionMessage, String visibleMessage) {
		super(exceptionMessage);
		this.visibleMessage = visibleMessage;
	}

	public VisibleException(Throwable throwable) {
		super(throwable);
	}

	public VisibleException(String exceptionMessage, Throwable throwable) {
		super(exceptionMessage, throwable);
	}

	public VisibleException(String exceptionMessage, Throwable throwable, String visibleMessage) {
		super(exceptionMessage, throwable);
		this.visibleMessage = visibleMessage;
	}

	public VisibleException(Throwable throwable, String visibleMessage) {
		super(throwable);
		this.visibleMessage = visibleMessage;
	}

	public String getVisibleMessage() {
		return visibleMessage;
	}
}
