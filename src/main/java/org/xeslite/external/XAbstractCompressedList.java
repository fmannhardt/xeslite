package org.xeslite.external;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import com.google.common.primitives.Longs;

import net.jpountz.lz4.LZ4Factory;

abstract class XAbstractCompressedList<E> extends AbstractList<E> {

	interface Compressor {

		int decompress(byte[] compressedData, byte[] uncompressedBuffer, int srcOff, int destOff, int destLen);

		byte[] decompress(byte[] compressedData, int uncompressedSize);

		int compress(byte[] data, int srcOff, int srcLen, byte[] dest, int destOff);

		byte[] compress(byte[] compressedData);

		int maxCompressedSize(int length);

	}

	static final class EventData {
		long[] ids;

		EventData() {
			this(0);
		}

		EventData(int size) {
			super();
			ids = new long[size];
		}
	}

	//TODO these iterators are not fast failing!
	private class CompressedIterator implements Iterator<E> {

		protected EventData eventData;

		protected int index = 0;
		protected int last = -1;

		CompressedIterator(EventData eventData) {
			this.eventData = eventData;
		}

		public boolean hasNext() {
			return index != size();
		}

		public E next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			E element = doGet(index, eventData);
			last = index;
			index++;
			return element;
		}

		public void remove() {
			if (last < 0) {
				throw new IllegalStateException();
			}

			// Remove from list			
			setEventData(doRemove(last, eventData));

			if (last < index) {
				index--;
			}
			last = -1;
		}

	}

	//TODO these iterators are not fast failing!
	private class CompressedListIterator extends CompressedIterator implements ListIterator<E> {

		CompressedListIterator(int index, EventData eventData) {
			super(eventData);
			this.index = index;
		}

		public boolean hasPrevious() {
			return index > 0;
		}

		public E previous() {
			try {
				int prevIndex = index - 1;
				E previous = doGet(prevIndex, eventData);
				index = prevIndex;
				last = prevIndex;
				return previous;
			} catch (IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}
		}

		public int nextIndex() {
			return index;
		}

		public int previousIndex() {
			return index - 1;
		}

		public void set(E e) {
			if (last == -1) {
				throw new IllegalStateException();
			}
			setEventData(doSet(last, e, eventData));
		}

		public void add(E e) {
			eventData = resizeIfNeeded(size + 1, eventData);
			setEventData(doAdd(index++, e, eventData));
			last = -1;
		}

	}

	private static final Compressor COMPRESSOR = new Compressor() {

		private final LZ4Factory factory = LZ4Factory.fastestJavaInstance();

		public int decompress(byte[] compressedData, byte[] uncompressedBuffer, int srcOff, int destOff, int destLen) {
			return factory.fastDecompressor().decompress(compressedData, srcOff, uncompressedBuffer, destOff, destLen);
		}

		public int compress(byte[] data, int srcOff, int srcLen, byte[] dest, int destOff) {
			return factory.fastCompressor().compress(data, srcOff, srcLen, dest, destOff);
		}

		public int maxCompressedSize(int length) {
			return factory.fastCompressor().maxCompressedLength(length);
		}

		public byte[] decompress(byte[] compressedData, int uncompressedSize) {
			return factory.fastDecompressor().decompress(compressedData, uncompressedSize);
		}

		public byte[] compress(byte[] data) {
			return factory.fastCompressor().compress(data);
		}

	};

	private byte[] compressedData;
	private int size;

	public XAbstractCompressedList() {
		this.compressedData = null;
		this.size = 0;
	}

	abstract protected E newInstance(int index, long id);
	
	abstract protected E convertElement(E e);

	abstract protected long getExternalId(E e);

	abstract protected int getIdShift();

	protected ByteBuffer getCompressedIds() {
		return ByteBuffer.wrap(compressedData);
	}

	public E get(int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException();
		}
		EventData eventData = getEventData();
		return doGet(index, eventData);
	}

	private E doGet(int index, EventData eventData) {
		return newInstance(index, eventData.ids[index]);
	}

	public E set(int index, E element) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException();
		}
		E newElement = convertElement(element);
		
		EventData eventData = getEventData();

		E oldElement = doGet(index, eventData);

		setEventData(doSet(index, newElement, eventData));

		return oldElement;
	}

	private EventData doSet(int index, E newElement, EventData eventData) {
		eventData.ids[index] = getExternalId(newElement);
		return eventData;
	}

	public void add(int index, E element) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}

		EventData eventData = resizeIfNeeded(size + 1, getEventData());

		eventData = doAdd(index, element, eventData);

		setEventData(eventData);
	}

	private EventData doAdd(int index, E element, EventData eventData) {
		E newElement = convertElement(element);
		System.arraycopy(eventData.ids, index, eventData.ids, index + 1, size - index);
		eventData = doSet(index, newElement, eventData);
		size++;
		return eventData;
	}

	public boolean addAll(Collection<? extends E> c) {
		return addAll(size, c);
	}

	public boolean addAll(int index, Collection<? extends E> c) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}
		int collectionSize = c.size();

		EventData eventData = resizeIfNeeded(size + collectionSize, getEventData());

		System.arraycopy(eventData.ids, index, eventData.ids, index + collectionSize, size - index);

		int i = 0;
		for (Iterator<? extends E> iterator = c.iterator(); iterator.hasNext(); i++) {
			E element = iterator.next();
			E newElement = convertElement(element);
			eventData.ids[index + i] = getExternalId(newElement);
		}
		size += collectionSize;

		setEventData(eventData);

		return collectionSize != 0;
	}

	public Iterator<E> iterator() {
		return new CompressedIterator(getEventData());
	}

	public ListIterator<E> listIterator(int index) {
		return new CompressedListIterator(index, getEventData());
	}

	public E remove(int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException();
		}

		EventData eventData = getEventData();

		E oldEvent = doGet(index, eventData);

		setEventData(doRemove(index, eventData));

		return oldEvent;
	}

	private EventData doRemove(int index, EventData eventData) {
		// Move subsequent events
		int tailLength = size - index - 1;
		if (tailLength > 0) {
			System.arraycopy(eventData.ids, index + 1, eventData.ids, index, tailLength);
		}

		// Change size and set last entry to zero
		--size;
		eventData.ids[size] = 0;

		return eventData;
	}

	public final int size() {
		return size;
	}

	protected final EventData getEventData() {
		if (compressedData == null) {
			return new EventData();
		} else {
			int uncompressedSize = uncompressedSize();
			byte[] uncompressedData = COMPRESSOR.decompress(compressedData, uncompressedSize);
			return decodeFromBytes(ByteBuffer.wrap(uncompressedData));
		}
	}

	protected final void setEventData(EventData eventIds) {
		byte[] uncompressedData = encodeToBytes(eventIds).array();
		compressedData = COMPRESSOR.compress(uncompressedData);
	}

	private final ByteBuffer encodeToBytes(EventData eventData) {
		ByteBuffer buffer = ByteBuffer.allocate(uncompressedSize());

		long lastId = 0;
		for (int i = 0; i < size; i++) {
			long id = eventData.ids[i] >> getIdShift();

			// Delta encode
			long delta = id - lastId;
			lastId = id;

			buffer.putLong(delta);
		}
		buffer.flip();
		return buffer;
	}

	private final int uncompressedSize() {
		return size * Longs.BYTES;
	}

	private final EventData decodeFromBytes(ByteBuffer uncompressedData) {
		EventData eventData = new EventData(size);

		long lastId = 0;

		for (int i = 0; i < size; i++) {
			// Delta decode
			long newId = uncompressedData.getLong() + lastId;
			eventData.ids[i] = newId << getIdShift();
			lastId = newId;
		}

		return eventData;
	}

	private static EventData resizeIfNeeded(int requiredCapacity, EventData data) {
		data.ids = resizeIfNeeded(requiredCapacity, data.ids);
		return data;
	}

	private static long[] resizeIfNeeded(int requiredCapacity, long[] array) {
		if (requiredCapacity > array.length) {
			int newCapacity = Math.max(10, 2 * array.length);
			if (newCapacity < requiredCapacity) {
				newCapacity = requiredCapacity;
			}
			array = Arrays.copyOf(array, newCapacity);
		}
		return array;
	}

}
