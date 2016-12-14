package org.xeslite.common;

public class XESLiteException extends RuntimeException {
	
	public final static class StringPoolException extends XESLiteException {

		private static final long serialVersionUID = 4845709116730684529L;

		public StringPoolException() {
			super();
		}

		public StringPoolException(String message, Throwable cause, boolean enableSuppression,
				boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}

		public StringPoolException(String message, Throwable cause) {
			super(message, cause);
		}

		public StringPoolException(String message) {
			super(message);
		}

		public StringPoolException(Throwable cause) {
			super(cause);
		}
		
	}

	private static final long serialVersionUID = -1904462818496947919L;

	public XESLiteException() {
		super();
	}

	public XESLiteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public XESLiteException(String message, Throwable cause) {
		super(message, cause);
	}

	public XESLiteException(String message) {
		super(message);
	}

	public XESLiteException(Throwable cause) {
		super(cause);
	}
}