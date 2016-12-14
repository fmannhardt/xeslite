package org.xeslite.dfa;

import java.util.AbstractList;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.deckfour.xes.model.impl.XAttributeMapImpl;
import org.deckfour.xes.model.impl.XEventImpl;
import org.deckfour.xes.model.impl.XLogImpl;
import org.deckfour.xes.model.impl.XTraceImpl;
import org.xeslite.common.ImmutableXAttributeMap;

import com.google.common.collect.BiMap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

import eu.danieldk.dictomaton.PerfectHashDictionary;

final class XLogDFA extends AbstractList<XTrace> implements XLog {

	private static final class TraceEventId extends XID {

		private final long traceId;
		private final long eventId;

		public TraceEventId(long traceId, long eventId) {
			this.traceId = traceId;
			this.eventId = eventId;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TraceEventId) {
				TraceEventId other = (TraceEventId) obj;
				return traceId == other.traceId && eventId == other.eventId;
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return Long.toString(traceId) + "+" + Long.toString(eventId);
		}

		@Override
		public Object clone() {
			return new TraceEventId(traceId, eventId);
		}

		@Override
		public int hashCode() {
			int result = 1;
			result = 31 * result + Longs.hashCode(traceId);
			result = 31 * result + Longs.hashCode(eventId);
			return result;
		}

		@Override
		public int compareTo(XID o) {
			if (o instanceof TraceEventId) {
				return ComparisonChain.start().compare(this.traceId, ((TraceEventId) o).traceId)
						.compare(this.eventId, ((TraceEventId) o).eventId).result();
			} else {
				return -1; //TODO correct?
			}
		}

	}

	private final static class XEventCharacterImpl implements XEvent {

		private final long traceId;
		private final long eventId;

		private XAttribute identityAttribute;

		public XEventCharacterImpl(long traceId, long eventId, XAttribute identityAttribute) {
			this.traceId = traceId;
			this.eventId = eventId;
			this.identityAttribute = identityAttribute;
		}

		public XAttributeMap getAttributes() {
			return new ImmutableXAttributeMap(identityAttribute);
		}

		public boolean hasAttributes() {
			return true;
		}

		public Set<XExtension> getExtensions() {
			return ImmutableSet.<XExtension>of(XConceptExtension.instance());
		}

		public XID getID() {
			return new TraceEventId(traceId, eventId);
		}

		public void accept(XVisitor visitor, XTrace trace) {
			visitor.visitEventPre(this, trace);
			for (XAttribute attribute : getAttributes().values()) {
				attribute.accept(visitor, this);
			}
			visitor.visitEventPost(this, trace);
		}

		public Object clone() {
			return new XEventImpl(getAttributes());
		}

		public void setAttributes(XAttributeMap attributes) {
			throw new UnsupportedOperationException();
		}

	}

	private final class XTraceSequenceImpl extends AbstractList<XEvent> implements XTrace {

		private final long traceId;
		private final String sequence;

		public XTraceSequenceImpl(long traceId, String sequence) {
			this.traceId = traceId;
			this.sequence = sequence;
		}

		public XEvent get(int index) {
			char character = sequence.charAt(index);
			return charToEvent(traceId, index, character);
		}

		public int size() {
			return sequence.length();
		}

		public XAttributeMap getAttributes() {
			return new XAttributeMapImpl();
		}

		public void setAttributes(XAttributeMap attributes) {
			throw new UnsupportedOperationException();
		}

		public boolean hasAttributes() {
			return false;
		}

		public Set<XExtension> getExtensions() {
			return ImmutableSet.of();
		}

		public void accept(XVisitor visitor, XLog log) {
			visitor.visitTracePre(this, log);
			for (XAttribute attribute : getAttributes().values()) {
				attribute.accept(visitor, this);
			}
			for (XEvent event : this) {
				event.accept(visitor, this);
			}
			visitor.visitTracePost(this, log);
		}

		public int insertOrdered(XEvent event) {
			throw new UnsupportedOperationException();
		}

		public Object clone() {
			XTrace clone = new XTraceImpl(getAttributes());
			for (XEvent event : this) {
				clone.add((XEvent) event.clone());
			}
			return clone;
		}

	}

	private final BiMap<String, Character> eventClasses;
	private final PerfectHashDictionary dictionary;
	private final int[] frequencies;
	private final int size;

	private String name;

	public XLogDFA(BiMap<String, Character> eventClasses, PerfectHashDictionary dictionary, int[] frequencies, int totalCount) {
		this.eventClasses = eventClasses;
		this.dictionary = dictionary;
		this.frequencies = frequencies;
		this.size = totalCount;
	}

	public XAttribute charToIdentifier(char character) {
		String identity = eventClasses.inverse().get(Character.valueOf(character));
		return new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, identity, XConceptExtension.instance());
	}

	private XEvent charToEvent(long traceId, long eventId, char character) {
		return new XEventCharacterImpl(traceId, eventId, charToIdentifier(character));
	}

	private int indexToHash(int index) {
		int hash = 1, count = 0;
		for (int frequency : frequencies) {
			count += frequency;
			if (count >= index) {
				return hash;
			}
			hash++;
		}
		return -1;
	}

	public XTrace get(int index) {
		int hash = indexToHash(index);
		String sequence = dictionary.sequence(hash);
		if (sequence != null) {
			return sequenceToTrace(index, sequence);
		} else {
			return null;
		}
	}

	private XTrace sequenceToTrace(long traceId, String sequence) {
		return new XTraceSequenceImpl(traceId, sequence);
	}

	public int size() {
		return size;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public XAttributeMap getAttributes() {
		return new ImmutableXAttributeMap(
				new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, name, XConceptExtension.instance()));
	}

	public boolean hasAttributes() {
		return name != null;
	}

	public Set<XExtension> getExtensions() {
		return ImmutableSet.<XExtension>of(XConceptExtension.instance());
	}

	public List<XEventClassifier> getClassifiers() {
		return ImmutableList.<XEventClassifier>of(new XEventNameClassifier());
	}

	public List<XAttribute> getGlobalTraceAttributes() {
		return ImmutableList.<XAttribute>of();
	}

	public List<XAttribute> getGlobalEventAttributes() {
		return ImmutableList.<XAttribute>of(XConceptExtension.ATTR_NAME);
	}

	public boolean accept(XVisitor visitor) {
		if (visitor.precondition()) {
			visitor.init(this);
			visitor.visitLogPre(this);
			for (XExtension extension : getExtensions()) {
				extension.accept(visitor, this);
			}
			for (XEventClassifier classifier : getClassifiers()) {
				classifier.accept(visitor, this);
			}
			if (hasAttributes()) {
				for (XAttribute attribute : getAttributes().values()) {
					attribute.accept(visitor, this);
				}
			}
			for (XTrace trace : this) {
				trace.accept(visitor, this);
			}
			visitor.visitLogPost(this);
			return true;
		}
		return false;
	}

	public Object clone() {
		XLogImpl clone = new XLogImpl((XAttributeMap) getAttributes().clone());
		for (XTrace trace : this) {
			clone.add((XTrace) trace.clone());
		}
		return clone;
	}
	
	// We do not cache
	public XLogInfo getInfo(XEventClassifier classifier) {
		return null;
	}

	public void setInfo(XEventClassifier classifier, XLogInfo info) {
	}	

	// Mutation not allowed
	public void setAttributes(XAttributeMap attributes) {
		throw new UnsupportedOperationException();
	}

}