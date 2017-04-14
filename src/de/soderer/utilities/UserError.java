package de.soderer.utilities;

public class UserError extends Exception {
	private static final long serialVersionUID = 3676716731550742292L;

	public enum Reason {
		Unknown, Unauthenticated, Unauthorized, UnauthenticatedOrUnauthorized
	}

	private Reason reason;

	public UserError(String message) {
		super(LangResources.existsKey(message) ? LangResources.get(message) : message);

		reason = Reason.Unknown;
	}

	public UserError(String message, Reason reason) {
		super(LangResources.existsKey(message) ? LangResources.get(message) : message);

		this.reason = reason;
	}

	public UserError(String message, Throwable cause) {
		super(LangResources.existsKey(message) ? LangResources.get(message) : message, cause);

		reason = Reason.Unknown;
	}

	public Reason getReason() {
		return reason;
	}
}
