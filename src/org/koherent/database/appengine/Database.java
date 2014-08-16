package org.koherent.database.appengine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.koherent.database.DuplicateIdException;
import org.koherent.database.IdNotFoundException;
import org.koherent.database.InterruptedTransactionException;
import org.koherent.database.Value;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

public abstract class Database<I, V extends Value<I>> implements
		org.koherent.database.Database<I, V> {
	protected static final int NUMBER_OF_MAX_RETRIES = 3;

	public boolean exists(I id) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return exists(datastore, id);
	}

	protected boolean exists(DatastoreService datastore, I id)
			throws InterruptedTransactionException {
		return exists(datastore, null, id);
	}

	protected boolean exists(DatastoreService datastore,
			Transaction transaction, I id)
			throws InterruptedTransactionException {
		try {
			datastore.get(transaction, toKey(id));

			return true;
		} catch (EntityNotFoundException e) {
			return false;
		}
	}

	public Set<I> exists(Iterable<I> ids)
			throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return exists(datastore, ids);
	}

	protected Set<I> exists(DatastoreService datastore, Iterable<I> ids)
			throws InterruptedTransactionException {
		return exists(datastore, null, ids);
	}

	protected Set<I> exists(DatastoreService datastore,
			Transaction transaction, Iterable<I> ids)
			throws InterruptedTransactionException {
		return keyExists(datastore, transaction, idsToKeys(ids));
	}

	protected Set<I> keyExists(DatastoreService datastore,
			Transaction transaction, Iterable<Key> keys) {
		Set<I> existingIds = new HashSet<I>();

		Map<Key, Entity> keysToEntities = datastore.get(transaction, keys);
		for (Key key : keysToEntities.keySet()) {
			existingIds.add(toId(key));
		}

		return existingIds;
	}

	@Override
	public List<I> getIds(int limit) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return getIds(datastore, limit);
	}

	protected List<I> getIds(DatastoreService datastore, int limit)
			throws InterruptedTransactionException {
		return getIds(datastore, null, limit);
	}

	protected List<I> getIds(DatastoreService datastore,
			Transaction transaction, int limit)
			throws InterruptedTransactionException {
		return getIds(datastore, transaction, limit, 0);
	}

	@Override
	public List<I> getIds(int limit, int offset)
			throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return getIds(datastore, limit, offset);
	}

	protected List<I> getIds(DatastoreService datastore, int limit, int offset) {
		return getIds(datastore, null, limit, offset);
	}

	protected List<I> getIds(DatastoreService datastore,
			Transaction transaction, int limit, int offset) {
		Query query = new Query(getKind());
		query.setKeysOnly();

		return searchIds(datastore, transaction, query, offset, limit);
	}

	@Override
	public Iterator<I> getAllIds() throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return getAllIds(datastore);
	}

	protected Iterator<I> getAllIds(DatastoreService datastore)
			throws InterruptedTransactionException {
		return getAllIds(datastore, null);
	}

	protected Iterator<I> getAllIds(DatastoreService datastore,
			Transaction transaction) throws InterruptedTransactionException {
		Query query = new Query(getKind());
		query.setKeysOnly();

		return searchIds(datastore, transaction, query);
	}

	@Override
	public V get(I id) throws InterruptedTransactionException,
			IdNotFoundException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return get(datastore, id);
	}

	protected V get(DatastoreService datastore, I id)
			throws InterruptedTransactionException, IdNotFoundException {
		return get(datastore, null, id);
	}

	protected V get(DatastoreService datastore, Transaction transaction, I id)
			throws InterruptedTransactionException, IdNotFoundException {
		try {
			Entity entity = datastore.get(transaction, toKey(id));
			return toValue(entity);
		} catch (EntityNotFoundException e) {
			throw new IdNotFoundException(id, e);
		}
	}

	@Override
	public List<V> get(I... ids) throws InterruptedTransactionException,
			IdNotFoundException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return get(datastore, ids);
	}

	protected List<V> get(DatastoreService datastore, I... ids)
			throws InterruptedTransactionException, IdNotFoundException {
		return get(datastore, null, ids);
	}

	protected List<V> get(DatastoreService datastore, Transaction transaction,
			I... ids) throws InterruptedTransactionException,
			IdNotFoundException {
		return getByKeys(datastore, transaction, idsToKeys(ids));
	}

	@Override
	public List<V> get(Iterable<I> ids) throws InterruptedTransactionException,
			IdNotFoundException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return get(datastore, ids);
	}

	protected List<V> get(DatastoreService datastore, Iterable<I> ids)
			throws InterruptedTransactionException, IdNotFoundException {
		return get(datastore, null, ids);
	}

	protected List<V> get(DatastoreService datastore, Transaction transaction,
			Iterable<I> ids) throws InterruptedTransactionException,
			IdNotFoundException {
		return getByKeys(datastore, transaction, idsToKeys(ids));
	}

	protected List<V> getByKeys(DatastoreService datastore,
			Transaction transaction, Iterable<Key> keys)
			throws InterruptedTransactionException, IdNotFoundException {
		try {
			Map<Key, Entity> entities = datastore.get(transaction, keys);

			List<V> values = new ArrayList<V>();
			for (Key key : keys) {
				values.add(toValue(entities.get(key)));
			}

			return values;
		} catch (IllegalArgumentException e) {
			throw new IdNotFoundException(null, e);
		}
	}

	@Override
	public List<V> get(int limit) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return get(datastore, limit);
	}

	protected List<V> get(DatastoreService datastore, int limit)
			throws InterruptedTransactionException {
		return get(datastore, null, limit);
	}

	protected List<V> get(DatastoreService datastore, Transaction transaction,
			int limit) throws InterruptedTransactionException {
		return get(datastore, transaction, limit, 0);
	}

	@Override
	public List<V> get(int limit, int offset)
			throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return get(datastore, limit, offset);
	}

	protected List<V> get(DatastoreService datastore, int limit, int offset)
			throws InterruptedTransactionException {
		return get(datastore, null, limit, offset);
	}

	protected List<V> get(DatastoreService datastore, Transaction transaction,
			int limit, int offset) throws InterruptedTransactionException {
		return search(datastore, transaction, new Query(getKind()), offset,
				limit);
	}

	@Override
	public Iterator<V> getAll() throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		return getAll(datastore);
	}

	protected Iterator<V> getAll(DatastoreService datastore)
			throws InterruptedTransactionException {
		return getAll(datastore, null);
	}

	protected Iterator<V> getAll(DatastoreService datastore,
			Transaction transaction) throws InterruptedTransactionException {
		return search(datastore, transaction, new Query(getKind()));
	}

	@Override
	public I add(V value) throws InterruptedTransactionException,
			DuplicateIdException {
		return add(value, 0);
	}

	protected I add(V value, int retryCount)
			throws InterruptedTransactionException, DuplicateIdException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		Transaction transaction = datastore.beginTransaction();
		try {
			I id = add(datastore, transaction, value);
			transaction.commit();

			return id;
		} catch (DatastoreFailureException e) {
			if (retryCount < NUMBER_OF_MAX_RETRIES) {
				return add(value, retryCount + 1);
			}

			throw new InterruptedTransactionException(e);
		} catch (InterruptedTransactionException e) {
			transaction.rollback();
			throw e;
		} catch (DuplicateIdException e) {
			transaction.rollback();
			throw e;
		}
	}

	protected I add(DatastoreService datastore, V value)
			throws InterruptedTransactionException, DuplicateIdException {
		return add(datastore, null, value);
	}

	protected I add(DatastoreService datastore, Transaction transaction, V value)
			throws InterruptedTransactionException, DuplicateIdException {
		I id = value.getId();
		if (id == null) {
			return toId(datastore.put(transaction, toEntity(value)));
		}

		try {
			datastore.get(transaction, toKey(id));

			throw new DuplicateIdException(id);
		} catch (EntityNotFoundException e) {
			return toId(datastore.put(transaction, toEntity(value)));
		}
	}

	@Override
	public void put(V value) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		put(datastore, value);
	}

	protected void put(DatastoreService datastore, V value)
			throws InterruptedTransactionException {
		put(datastore, null, value);
	}

	protected void put(DatastoreService datastore, Transaction transaction,
			V value) throws InterruptedTransactionException {
		datastore.put(transaction, toEntity(value));
	}

	@Override
	public void put(Iterable<V> values) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		put(datastore, values);
	}

	protected void put(DatastoreService datastore, Iterable<V> values)
			throws InterruptedTransactionException {
		put(datastore, null, values);
	}

	protected void put(DatastoreService datastore, Transaction transaction,
			Iterable<V> values) throws InterruptedTransactionException {
		List<Entity> entities = new ArrayList<Entity>();
		for (V value : values) {
			entities.add(toEntity(value));
		}

		datastore.put(transaction, entities);
	}

	@Override
	public void remove(I id) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		remove(datastore, id);
	}

	protected void remove(DatastoreService datastore, I id)
			throws InterruptedTransactionException {
		remove(datastore, null, id);
	}

	protected void remove(DatastoreService datastore, Transaction transaction,
			I id) throws InterruptedTransactionException {
		datastore.delete(transaction, toKey(id));
	}

	@Override
	public void remove(Iterable<I> ids) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		remove(datastore, ids);
	}

	protected void remove(DatastoreService datastore, Iterable<I> ids)
			throws InterruptedTransactionException {
		remove(datastore, null, ids);
	}

	protected void remove(DatastoreService datastore, Transaction transaction,
			Iterable<I> ids) throws InterruptedTransactionException {
		List<Key> keys = new ArrayList<Key>();
		for (I id : ids) {
			keys.add(toKey(id));
		}

		datastore.delete(transaction, keys);
	}

	@Override
	public void remove(int limit) throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		remove(datastore, limit);
	}

	protected void remove(DatastoreService datastore, int limit)
			throws InterruptedTransactionException {
		remove(datastore, null, limit);
	}

	protected void remove(DatastoreService datastore, Transaction transaction,
			int limit) throws InterruptedTransactionException {
		Query query = new Query(getKind());
		query.setKeysOnly();

		List<Entity> entities = searchEntities(datastore, transaction, query,
				0, limit);

		List<Key> keys = new ArrayList<Key>();
		for (Entity entity : entities) {
			keys.add(entity.getKey());
		}

		datastore.delete(transaction, keys);
	}

	@Override
	public void removeAll() throws InterruptedTransactionException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		removeAll(datastore);
	}

	protected void removeAll(DatastoreService datastore)
			throws InterruptedTransactionException {
		removeAll(datastore, null);
	}

	protected void removeAll(DatastoreService datastore, Transaction transaction)
			throws InterruptedTransactionException {
		Query query = new Query(getKind());
		query.setKeysOnly();

		Iterator<Entity> iterator = searchEntities(datastore, transaction,
				query);

		List<Key> keys = new ArrayList<Key>();
		while (iterator.hasNext()) {
			keys.add(iterator.next().getKey());
		}

		datastore.delete(transaction, keys);
	}

	protected List<Entity> searchEntities(DatastoreService datastore,
			Transaction transaction, Query query, int offset, int limit) {
		FetchOptions fetchOptions = FetchOptions.Builder.withLimit(limit);
		if (offset > 0) {
			fetchOptions.offset(offset);
		}

		return datastore.prepare(transaction, query).asList(fetchOptions);
	}

	protected List<I> searchIds(DatastoreService datastore,
			Transaction transaction, Query query, int offset, int limit) {
		return entitiesToIds(searchEntities(datastore, transaction, query,
				offset, limit));
	}

	protected List<V> search(DatastoreService datastore,
			Transaction transaction, Query query, int offset, int limit) {
		return entitiesToValues(searchEntities(datastore, transaction, query,
				offset, limit));
	}

	protected Iterator<Entity> searchEntities(DatastoreService datastore,
			Transaction transaction, Query query) {
		return datastore.prepare(transaction, query).asIterator();
	}

	protected Iterator<I> searchIds(DatastoreService datastore,
			Transaction transaction, Query query) {
		return new IdIterator(searchEntities(datastore, transaction, query));
	}

	protected Iterator<V> search(DatastoreService datastore,
			Transaction transaction, Query query) {
		return new ValueIterator(searchEntities(datastore, transaction, query));
	}

	protected abstract String getKind();

	protected abstract I toId(Key key);

	protected abstract Key toKey(I id);

	protected abstract V toValue(Entity entity);

	protected abstract Entity toEntity(V value);

	protected List<I> entitiesToIds(Iterable<Entity> entities) {
		List<I> ids = new ArrayList<I>();

		for (Entity entity : entities) {
			ids.add(toId(entity.getKey()));
		}

		return ids;
	}

	protected List<V> entitiesToValues(List<Entity> entities) {
		List<V> values = new ArrayList<V>(entities.size());
		for (Entity entity : entities) {
			values.add(toValue(entity));
		}

		return values;
	}

	protected List<Key> idsToKeys(I... ids) {
		List<Key> keys = new ArrayList<Key>();

		for (I id : ids) {
			keys.add(toKey(id));
		}

		return keys;
	}

	protected List<Key> idsToKeys(Iterable<I> ids) {
		List<Key> keys = new ArrayList<Key>();

		for (I id : ids) {
			keys.add(toKey(id));
		}

		return keys;
	}

	protected static void setPropertyIfNotNull(Entity entity,
			String propertyName, Object value) throws IllegalArgumentException {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' cannot be null.");
		}
		if (propertyName == null) {
			throw new IllegalArgumentException("'propertyName' cannot be null.");
		}

		if (value != null) {
			entity.setProperty(propertyName, value);
		}
	}

	protected static void setUnindexedPropertyIfNotNull(Entity entity,
			String propertyName, Object value) throws IllegalArgumentException {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' cannot be null.");
		}
		if (propertyName == null) {
			throw new IllegalArgumentException("'propertyName' cannot be null.");
		}

		if (value != null) {
			entity.setUnindexedProperty(propertyName, value);
		}
	}

	protected class IdIterator implements Iterator<I> {
		private Iterator<Entity> entityIterator;

		public IdIterator(Iterator<Entity> entityIterator) {
			super();
			this.entityIterator = entityIterator;
		}

		@Override
		public boolean hasNext() {
			return entityIterator.hasNext();
		}

		@Override
		public I next() {
			Entity entity = entityIterator.next();
			if (entity == null) {
				return null;
			}

			Key key = entity.getKey();
			if (key == null) {
				return null;
			}

			return toId(key);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	protected class ValueIterator implements Iterator<V> {
		private Iterator<Entity> entityIterator;

		public ValueIterator(Iterator<Entity> entityIterator) {
			super();
			this.entityIterator = entityIterator;
		}

		@Override
		public boolean hasNext() {
			return entityIterator.hasNext();
		}

		@Override
		public V next() {
			Entity entity = entityIterator.next();
			if (entity == null) {
				return null;
			}

			return toValue(entity);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
