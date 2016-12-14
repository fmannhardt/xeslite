package org.xeslite.external;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.model.impl.XAttributeMapImpl;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;

final class XLogExternalImpl extends ForwardingList<XTrace> implements XLog {

	private ExternalStore store;

	private List<XTrace> traces;

	private XAttributeMap attributes;

	private Set<XExtension> extensions;
	private List<XEventClassifier> classifiers;
	private List<XAttribute> globalTraceAttributes;
	private List<XAttribute> globalEventAttributes;

	private XEventClassifier cachedClassifier;
	private SoftReference<XLogInfo> cachedInfo; // retain only when enough memory

	private final boolean closeStoreOnFinalize;

	/*
	 * Try to dispose the store on finalize(), yet it is always better to
	 * dispose the factory!
	 */
	protected void finalize() throws Throwable {
		try {
			if (closeStoreOnFinalize) {
				store.dispose();
			}
		} finally {
			super.finalize();
		}
	}

	XLogExternalImpl(ExternalStore attributeStore) {
		this(null, attributeStore);
	}

	XLogExternalImpl(ExternalStore attributeStore, Collection<XTrace> events) {
		this(null, attributeStore, events);
	}

	XLogExternalImpl(XAttributeMap attributeMap, ExternalStore attributeStore, Collection<XTrace> events) {
		this(attributeStore.getIdFactory().nextId(), attributeMap, attributeStore, events, false);
	}

	XLogExternalImpl(XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(attributeStore.getIdFactory().nextId(), attributeMap, attributeStore, ImmutableList.<XTrace>of(), false);
	}

	XLogExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(id, attributeMap, attributeStore, ImmutableList.<XTrace>of(), false);
	}

	XLogExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore, Collection<XTrace> events,
			boolean closeStoreOnFinalize) {
		this.store = attributeStore;
		this.closeStoreOnFinalize = closeStoreOnFinalize;
		this.traces = new ArrayList<>(events);
		if (attributeMap != null) {
			this.attributes = attributeMap;	
		} else {
			this.attributes = new XAttributeMapImpl();
		}

		// Transient fields
		this.extensions = new HashSet<>();
		this.classifiers = new ArrayList<>();
		this.globalTraceAttributes = new ArrayList<>();
		this.globalEventAttributes = new ArrayList<>();
		this.cachedClassifier = null;
		this.cachedInfo = null;
	}

	@Override
	public XAttributeMap getAttributes() {
		return attributes;
	}

	@Override
	public void setAttributes(XAttributeMap attributes) {
		this.attributes = attributes;
	}

	@Override
	public boolean hasAttributes() {
		return !attributes.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XAttributable#getExtensions()
	 */
	public Set<XExtension> getExtensions() {
		return extensions;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#getClassifiers()
	 */
	public List<XEventClassifier> getClassifiers() {
		return classifiers;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#getGlobalEventAttributes()
	 */
	public List<XAttribute> getGlobalEventAttributes() {
		return globalEventAttributes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#getGlobalTraceAttributes()
	 */
	public List<XAttribute> getGlobalTraceAttributes() {
		return globalTraceAttributes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#getInfo(org.deckfour.xes.classification.
	 * XEventClassifier)
	 */
	public XLogInfo getInfo(XEventClassifier classifier) {
		if (classifier.equals(cachedClassifier)) {
			return getInfo();
		}
		return null;
	}

	public XLogInfo getInfo() {
		XLogInfo info = cachedInfo.get();
		if (info != null) {
			return info;
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#setInfo(org.deckfour.xes.classification.
	 * XEventClassifier, org.deckfour.xes.info.XLogInfo)
	 */
	public void setInfo(XEventClassifier classifier, XLogInfo info) {
		cachedClassifier = classifier;
		cachedInfo = new SoftReference<XLogInfo>(info);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		XLogExternalImpl clone = new XLogExternalImpl((XAttributeMap) getAttributes().clone(), store);
		clone.extensions = new HashSet<>(extensions);
		clone.classifiers = new ArrayList<>(classifiers);
		clone.globalTraceAttributes = new ArrayList<>(globalTraceAttributes);
		clone.globalEventAttributes = new ArrayList<>(globalEventAttributes);
		clone.cachedClassifier = null;
		clone.cachedInfo = null;
		for (XTrace trace : this) {
			clone.add((XTrace) trace.clone());
		}
		return clone;
	}

	/*
	 * Runs the given visitor on this log.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XLog#accept(org.deckfour.xes.model.XVisitor)
	 */
	public boolean accept(XVisitor visitor) {
		/*
		 * Check whether the visitor may run.
		 */
		if (visitor.precondition()) {
			/*
			 * Yes, it may. Now initialize.
			 */
			visitor.init(this);
			/*
			 * First call.
			 */
			visitor.visitLogPre(this);
			/*
			 * Visit the extensions.
			 */
			for (XExtension extension : extensions) {
				extension.accept(visitor, this);
			}
			/*
			 * Visit the classifiers.
			 */
			for (XEventClassifier classifier : classifiers) {
				classifier.accept(visitor, this);
			}
			/*
			 * Visit the attributes.
			 */
			if (hasAttributes()) {
				for (XAttribute attribute : getAttributes().values()) {
					attribute.accept(visitor, this);
				}
			}
			/*
			 * Visit the traces.
			 */
			for (XTrace trace : this) {
				trace.accept(visitor, this);
			}
			/*
			 * Last call.
			 */
			visitor.visitLogPost(this);
			return true;
		}
		return false;
	}

	protected List<XTrace> delegate() {
		return traces;
	}

}
