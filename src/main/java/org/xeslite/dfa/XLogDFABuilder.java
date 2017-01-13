package org.xeslite.dfa;

import java.util.List;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.TreeMultiset;

import eu.danieldk.dictomaton.DictionaryBuilderException;
import eu.danieldk.dictomaton.DictionaryBuilderIterative;
import eu.danieldk.dictomaton.PerfectHashDictionary;

public class XLogDFABuilder {

	private final TreeMultiset<String> sortingMultiset = TreeMultiset.create();
	private final BiMap<String, Character> eventClasses = HashBiMap.create();

	private final XEventClassifier classifier;
	private final boolean stateSuffixes;

	public XLogDFABuilder() {
		this(true);
	}

	public XLogDFABuilder(boolean stateSuffixes) {
		this(new XEventNameClassifier(), stateSuffixes);
	}

	public XLogDFABuilder(XEventClassifier classifier, boolean stateSuffixes) {
		super();
		this.classifier = classifier;
		this.stateSuffixes = stateSuffixes;
	}

	public void addLog(XLog log) {
		for (XTrace t : log) {
			addTrace(t);
		}
	}

	public void addTrace(XTrace trace) {
		addTrace(Lists.transform(trace, new Function<XEvent, String>() {

			public String apply(XEvent event) {
				return classifier.getClassIdentity(event);
			}
		}));
	}

	public void addTrace(List<String> trace) {
		StringBuilder sb = new StringBuilder(trace.size());
		for (String event : trace) {
			sb.append((char) addIdentity(event));
		}
		sortingMultiset.add(sb.toString());
	}

	private int addIdentity(String identity) {
		Character index = eventClasses.get(identity);
		if (index == null) {
			index = Character.valueOf((char) eventClasses.size());
			eventClasses.put(identity, index);
		}
		return index;
	}

	public XLogDFA build() {
		try {

			DictionaryBuilderIterative builder = new DictionaryBuilderIterative();

			for (Entry<String> entry : sortingMultiset.entrySet()) {
				builder.add(entry.getElement());
			}

			PerfectHashDictionary hashDictionary = builder.buildPerfectHash(stateSuffixes);

			int totalCount = 0;

			int[] frequencies = new int[sortingMultiset.elementSet().size()];
			for (String trace : hashDictionary) {
				int index = hashDictionary.number(trace);
				int frequency = sortingMultiset.count(trace);
				frequencies[index - 1] = frequency;
				totalCount += frequency;
			}

			return new XLogDFA(eventClasses, hashDictionary, frequencies, totalCount);

		} catch (DictionaryBuilderException e) {
			throw new RuntimeException("Could not build automaton!", e);
		}
	}

}