package org.xeslite.external;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.mapdb.Serializer;
import org.xeslite.common.XESLiteException;

public final class MapDBLogSerializer extends Serializer<XLog> {

	private static final int VERSION = 3;

	private final XFactoryExternalStore factory;

	public MapDBLogSerializer(XFactoryExternalStore factory) {
		this.factory = factory;
	}

	public void serialize(DataOutput out, XLog log) throws IOException {
		assert log instanceof ExternalIdentifyable : "Only supports logs stored in a MapDB database";
		out.writeInt(VERSION);
		out.writeLong(((ExternalIdentifyable) log).getExternalId());
		//Classifier
		out.writeInt(log.getClassifiers().size());
		for (XEventClassifier classifier : log.getClassifiers()) {
			out.writeUTF(classifier.name());
			String[] attributeKeys = classifier.getDefiningAttributeKeys();
			out.writeInt(attributeKeys.length);
			for (String key : attributeKeys) {
				out.writeUTF(key);
			}
		}
		//Extensions
		out.writeInt(log.getExtensions().size());
		for (XExtension extensions : log.getExtensions()) {
			URI uri = extensions.getUri();
			out.writeUTF(uri.toString());
		}
		//Globals
		writeGlobalAttributes(out, log.getGlobalEventAttributes());
		writeGlobalAttributes(out, log.getGlobalTraceAttributes());
		out.writeInt(log.size());
		for (XTrace t : log) {
			assert t instanceof ExternalIdentifyable : "Only supports traces stored in a MapDB database";
			out.writeInt(t.size());
			out.writeLong(((ExternalIdentifyable) t).getExternalId());
			for (XEvent e : t) {
				assert e instanceof ExternalIdentifyable : "Only supports events stored in a MapDB database";
				if (e instanceof AttributesCacheable) {
					//TODO store in MapDB
				}
				out.writeLong(((ExternalIdentifyable) e).getExternalId());
			}
		}
	}

	private void writeGlobalAttributes(DataOutput out, List<XAttribute> globalAttributes) throws IOException {
		out.writeInt(globalAttributes.size());
		for (XAttribute globalAttribute : globalAttributes) {
			ByteArrayOutputStream bs = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bs);
			os.writeObject(globalAttribute);
			byte[] byteArray = bs.toByteArray();
			out.writeInt(byteArray.length);
			out.write(byteArray);
		}
	}

	public XLog deserialize(DataInput in, int available) throws IOException {
		int version = in.readInt();
		if (version != VERSION) {
			throw new RuntimeException("Invalid XESLite database. Expected database version " + VERSION);
		}
		XLog log = factory.openLog(in.readLong());
		int classifierSize = in.readInt();
		for (int classIndex = 0; classIndex < classifierSize; classIndex++) {
			String name = in.readUTF();
			String[] keys = new String[in.readInt()];
			for (int keysIndex = 0; keysIndex < keys.length; keysIndex++) {
				keys[keysIndex] = in.readUTF();
			}
			log.getClassifiers().add(new XEventAttributeClassifier(name, keys));
		}
		int extensionsSize = in.readInt();
		for (int extIndex = 0; extIndex < extensionsSize; extIndex++) {
			XExtension extension = XExtensionManager.instance().getByUri(URI.create(in.readUTF()));
			log.getExtensions().add(extension);
		}
		readGlobals(in, log.getGlobalEventAttributes());
		readGlobals(in, log.getGlobalTraceAttributes());
		int logSize = in.readInt();
		ArrayList<XEvent> events = new ArrayList<>();
		for (int traceIndex = 0; traceIndex < logSize; traceIndex++) {
			events.clear();
			int traceSize = in.readInt();
			events.ensureCapacity(traceSize);
			long traceId = in.readLong();
			for (int eventIndex = 0; eventIndex < traceSize; eventIndex++) {
				XEvent event = factory.openEvent(in.readLong());
				if (event instanceof AttributesCacheable) {
					XAttributeMap attributes = event.getAttributes();
					AttributesCacheable cacheable = (AttributesCacheable) event;
					for (Iterator<XAttribute> iterator = attributes.values().iterator(); iterator.hasNext();) {
						XAttribute a = iterator.next();
						Integer cacheIndex = cacheable.getCacheIndex(a.getKey());
						if (cacheIndex != null) {
							cacheable.setCacheValue(cacheIndex, a);
						}
					}
				}
				events.add(event);
			}
			XTrace trace = factory.openTrace(traceId, events);
			log.add(trace);
		}
		return log;
	}

	private void readGlobals(DataInput in, List<XAttribute> globals) throws IOException {
		int globalsSize = in.readInt();
		for (int globalsIndex = 0; globalsIndex < globalsSize; globalsIndex++) {
			byte[] buffer = new byte[in.readInt()];
			in.readFully(buffer, 0, buffer.length);
			try {
				XAttribute attribute = (XAttribute) new ObjectInputStream(new ByteArrayInputStream(buffer))
						.readObject();
				globals.add(attribute);
			} catch (ClassNotFoundException e) {
				throw new XESLiteException("Could not read global attribute from XESLite database.", e);
			}
		}
	}

}
