package org.xeslite.external;

final class CompositeKey {

	private final long externalId;
	private final int attributeKey;

	public CompositeKey(long externalId, int attributeKey) {
		this.externalId = externalId;
		this.attributeKey = attributeKey;
	}

	public long getCompositeKey() {
		return externalId + attributeKey;
	}

	public int getAttributeKey() {
		return attributeKey;
	}

	public long getExternalId() {
		return externalId;
	}

	public static CompositeKey valueOf(long externalId, int attributeKey) {
		return new CompositeKey(externalId, attributeKey);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + attributeKey;
		result = prime * result + (int) (externalId ^ (externalId >>> 32));
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof CompositeKey))
			return false;
		CompositeKey other = (CompositeKey) obj;
		if (attributeKey != other.attributeKey)
			return false;
		if (externalId != other.externalId)
			return false;
		return true;
	}

}