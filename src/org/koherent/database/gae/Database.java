package org.koherent.database.gae;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.koherent.database.AbstractDatabase;
import org.koherent.database.DatabaseException;
import org.koherent.database.DuplicateIdException;
import org.koherent.database.IdNotFoundException;
import org.koherent.database.Value;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Transaction;

public abstract class Database<I, V extends Value<I>> extends
		AbstractDatabase<I, V> implements org.koherent.database.Database<I, V> {
	protected static final int NUMBER_OF_MAX_RETRIES = 3;

	public boolean exists(I id) throws DatabaseException {
		return exists(DatastoreServiceFactory.getDatastoreService(), id);
	}

	protected boolean exists(DatastoreService datastore, I id)
			throws DatabaseException {
		return exists(datastore, null, id);
	}

	protected boolean exists(DatastoreService datastore,
			Transaction transaction, I id) throws DatabaseException {
		return getExistingIds(datastore, transaction, Collections.singleton(id))
				.iterator().hasNext();
	}

	@Override
	public Iterable<? extends I> getExistingIds(Iterable<? extends I> ids)
			throws DatabaseException {
		return getExistingIds(DatastoreServiceFactory.getDatastoreService(),
				ids);
	}

	protected Iterable<? extends I> getExistingIds(DatastoreService datastore,
			Iterable<? extends I> ids) throws DatabaseException {
		return getExistingIds(datastore, null, ids);
	}

	protected Iterable<? extends I> getExistingIds(DatastoreService datastore,
			Transaction transaction, Iterable<? extends I> ids)
			throws DatabaseException {
		Query query = new Query(getKind());
		query.setFilter(new Query.FilterPredicate(Entity.KEY_RESERVED_PROPERTY,
				FilterOperator.IN, idsToKeys(ids)));

		return searchIds(datastore, transaction, query);
	}

	protected Set<? extends I> keyExists(DatastoreService datastore,
			Transaction transaction, Iterable<Key> keys) {
		Set<I> existingIds = new HashSet<I>();

		Map<Key, Entity> keysToEntities = datastore.get(transaction, keys);
		for (Key key : keysToEntities.keySet()) {
			existingIds.add(toId(key));
		}

		return existingIds;
	}

	@Override
	public Iterable<? extends I> getIds(int limit) throws DatabaseException {
		return getIds(DatastoreServiceFactory.getDatastoreService(), limit);
	}

	protected Iterable<? extends I> getIds(DatastoreService datastore, int limit)
			throws DatabaseException {
		return getIds(datastore, null, limit);
	}

	protected Iterable<? extends I> getIds(DatastoreService datastore,
			Transaction transaction, int limit) throws DatabaseException {
		return getIds(datastore, transaction, limit, 0);
	}

	@Override
	public Iterable<? extends I> getIds(int limit, int offset)
			throws DatabaseException {
		return getIds(DatastoreServiceFactory.getDatastoreService(), limit,
				offset);
	}

	protected Iterable<? extends I> getIds(DatastoreService datastore,
			int limit, int offset) {
		return getIds(datastore, null, limit, offset);
	}

	protected Iterable<? extends I> getIds(DatastoreService datastore,
			Transaction transaction, int limit, int offset) {
		Query query = new Query(getKind());

		return searchIds(datastore, transaction, query,
				limitAndOffset(limit, offset));
	}

	@Override
	public Iterable<? extends I> getAllIds() throws DatabaseException {
		return getAllIds(DatastoreServiceFactory.getDatastoreService());
	}

	protected Iterable<? extends I> getAllIds(DatastoreService datastore)
			throws DatabaseException {
		return getAllIds(datastore, null);
	}

	protected Iterable<? extends I> getAllIds(DatastoreService datastore,
			Transaction transaction) throws DatabaseException {
		Query query = new Query(getKind());

		return searchIds(datastore, transaction, query);
	}

	@Override
	public V get(I id) throws IdNotFoundException, DatabaseException {
		return get(DatastoreServiceFactory.getDatastoreService(), id);
	}

	protected V get(DatastoreService datastore, I id)
			throws IdNotFoundException, DatabaseException {
		return get(datastore, null, id);
	}

	protected V get(DatastoreService datastore, Transaction transaction, I id)
			throws IdNotFoundException, DatabaseException {
		try {
			Entity entity = datastore.get(transaction, toKey(id));
			return toValue(entity);
		} catch (EntityNotFoundException e) {
			throw new IdNotFoundException(id, e);
		}
	}

	@Override
	public Iterable<? extends V> get(Iterable<? extends I> ids)
			throws DatabaseException {
		return get(DatastoreServiceFactory.getDatastoreService(), ids);
	}

	protected Iterable<? extends V> get(DatastoreService datastore,
			Iterable<? extends I> ids) throws DatabaseException {
		return get(datastore, null, ids);
	}

	protected Iterable<? extends V> get(DatastoreService datastore,
			Transaction transaction, Iterable<? extends I> ids)
			throws DatabaseException {
		return getByKeys(datastore, transaction, idsToKeys(ids));
	}

	protected Iterable<? extends V> getByKeys(DatastoreService datastore,
			Transaction transaction, Iterable<Key> keys)
			throws DatabaseException {
		Map<Key, Entity> entities = datastore.get(transaction, keys);

		List<V> values = new ArrayList<V>();
		for (Key key : keys) {
			values.add(toValue(entities.get(key)));
		}

		return values;
	}

	@Override
	public Iterable<? extends V> get(int limit) throws DatabaseException {
		return get(DatastoreServiceFactory.getDatastoreService(), limit);
	}

	protected Iterable<? extends V> get(DatastoreService datastore, int limit)
			throws DatabaseException {
		return get(datastore, null, limit);
	}

	protected Iterable<? extends V> get(DatastoreService datastore,
			Transaction transaction, int limit) throws DatabaseException {
		return get(datastore, transaction, limit, 0);
	}

	@Override
	public Iterable<? extends V> get(int limit, int offset)
			throws DatabaseException {
		return get(DatastoreServiceFactory.getDatastoreService(), limit, offset);
	}

	protected Iterable<? extends V> get(DatastoreService datastore, int limit,
			int offset) throws DatabaseException {
		return get(datastore, null, limit, offset);
	}

	protected Iterable<? extends V> get(DatastoreService datastore,
			Transaction transaction, int limit, int offset)
			throws DatabaseException {
		return search(datastore, transaction, new Query(getKind()),
				limitAndOffset(limit, offset));
	}

	@Override
	public Iterable<? extends V> getAll() throws DatabaseException {
		return getAll(DatastoreServiceFactory.getDatastoreService());
	}

	protected Iterable<? extends V> getAll(DatastoreService datastore)
			throws DatabaseException {
		return getAll(datastore, null);
	}

	protected Iterable<? extends V> getAll(DatastoreService datastore,
			Transaction transaction) throws DatabaseException {
		return search(datastore, transaction, new Query(getKind()));
	}

	@Override
	public I add(V value) throws DuplicateIdException, DatabaseException {
		return add(DatastoreServiceFactory.getDatastoreService(), value);
	}

	protected I add(DatastoreService datastore, V value)
			throws DuplicateIdException, DatabaseException {
		for (int retryCount = 0; true; retryCount++) {
			Transaction transaction = datastore.beginTransaction();
			try {
				I id = add(datastore, transaction, value);
				transaction.commit();

				return id;
			} catch (DatastoreFailureException e) {
				throw new DatabaseException(e);
			} catch (ConcurrentModificationException e) {
				if (retryCount < NUMBER_OF_MAX_RETRIES) {
					continue;
				}

				throw new DatabaseException(e);
			} finally {
				if (transaction.isActive()) {
					transaction.rollback();
				}
			}
		}
	}

	protected I add(DatastoreService datastore, Transaction transaction, V value)
			throws DuplicateIdException, DatabaseException {
		I id = value.getId();
		if (id != null) {
			try {
				get(datastore, transaction, id);
				throw new DuplicateIdException(id);
			} catch (IdNotFoundException e) {
			}
		}

		return toId(datastore.put(transaction, toEntity(value)));
	}

	@Override
	public void put(V value) throws DatabaseException {
		put(DatastoreServiceFactory.getDatastoreService(), value);
	}

	protected void put(DatastoreService datastore, V value)
			throws DatabaseException {
		put(datastore, null, value);
	}

	protected void put(DatastoreService datastore, Transaction transaction,
			V value) throws DatabaseException {
		datastore.put(transaction, toEntity(value));
	}

	@Override
	public void put(Iterable<? extends V> values) throws DatabaseException {
		put(DatastoreServiceFactory.getDatastoreService(), values);
	}

	protected void put(DatastoreService datastore, Iterable<? extends V> values)
			throws DatabaseException {
		put(datastore, null, values);
	}

	protected void put(DatastoreService datastore, Transaction transaction,
			Iterable<? extends V> values) throws DatabaseException {
		List<Entity> entities = new ArrayList<Entity>();
		for (V value : values) {
			entities.add(toEntity(value));
		}

		datastore.put(transaction, entities);
	}

	@Override
	public void remove(I id) throws DatabaseException {
		remove(DatastoreServiceFactory.getDatastoreService(), id);
	}

	protected void remove(DatastoreService datastore, I id)
			throws DatabaseException {
		remove(datastore, null, id);
	}

	protected void remove(DatastoreService datastore, Transaction transaction,
			I id) throws DatabaseException {
		datastore.delete(transaction, toKey(id));
	}

	@Override
	public void remove(Iterable<? extends I> ids) throws DatabaseException {
		remove(DatastoreServiceFactory.getDatastoreService(), ids);
	}

	protected void remove(DatastoreService datastore, Iterable<? extends I> ids)
			throws DatabaseException {
		remove(datastore, null, ids);
	}

	protected void remove(DatastoreService datastore, Transaction transaction,
			Iterable<? extends I> ids) throws DatabaseException {
		try {
			datastore.delete(transaction, idsToKeys(ids));
		} catch (IllegalArgumentException | ConcurrentModificationException
				| DatastoreFailureException e) {
			throw new DatabaseException(e);
		}
	}

	@Override
	public void removeAll() throws DatabaseException {
		removeAll(DatastoreServiceFactory.getDatastoreService());
	}

	protected void removeAll(DatastoreService datastore)
			throws DatabaseException {
		removeAll(datastore, null);
	}

	protected void removeAll(DatastoreService datastore, Transaction transaction)
			throws DatabaseException {
		Query query = new Query(getKind());
		query.setKeysOnly();

		List<Key> keys = new ArrayList<Key>();
		for (Entity entity : searchEntities(datastore, transaction, query, null)) {
			keys.add(entity.getKey());
		}

		datastore.delete(transaction, keys);
	}

	protected FetchOptions limitAndOffset(int limit, int offset) {
		return limitAndOffset(null, limit, offset);
	}

	protected FetchOptions limitAndOffset(FetchOptions options, int limit,
			int offset) {
		if (options == null) {
			options = FetchOptions.Builder.withLimit(limit);
		} else {
			options.limit(limit);
		}
		if (offset > 0) {
			options.offset(offset);
		}

		return options;
	}

	protected Iterable<? extends Entity> searchEntities(
			DatastoreService datastore, Transaction transaction, Query query,
			FetchOptions fetchOptions) {
		return datastore.prepare(transaction, query).asIterable(fetchOptions);
	}

	protected Iterable<? extends I> searchIds(DatastoreService datastore,
			Transaction transaction, Query query, FetchOptions options) {
		query.setKeysOnly();
		final Iterable<? extends Entity> entities = searchEntities(datastore,
				transaction, query, options);

		return new Iterable<I>() {
			@Override
			public Iterator<I> iterator() {
				return new IdIterator(entities.iterator());
			}
		};
	}

	protected Iterable<? extends V> search(DatastoreService datastore,
			Transaction transaction, Query query, FetchOptions options) {
		final Iterable<? extends Entity> entities = searchEntities(datastore,
				transaction, query, options);

		return new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				return new ValueIterator(entities.iterator());
			}
		};
	}

	protected Iterable<? extends I> searchIds(DatastoreService datastore,
			Transaction transaction, Query query) {
		query.setKeysOnly();
		return searchIds(datastore, transaction, query,
				FetchOptions.Builder.withDefaults());
	}

	protected Iterable<? extends V> search(DatastoreService datastore,
			Transaction transaction, Query query) {
		return search(datastore, transaction, query,
				FetchOptions.Builder.withDefaults());
	}

	protected abstract String getKind();

	protected abstract I toId(Key key);

	protected abstract Key toKey(I id);

	protected abstract V toValue(Entity entity);

	protected abstract Entity toEntity(V value);

	protected Iterable<Key> idsToKeys(Iterable<? extends I> ids) {
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

		if (value == null) {
			entity.removeProperty(propertyName);
		} else {
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

		if (value == null) {
			entity.removeProperty(propertyName);
		} else {
			entity.setUnindexedProperty(propertyName, value);
		}
	}

	protected class IdIterator implements Iterator<I> {
		private Iterator<? extends Entity> entityIterator;

		public IdIterator(Iterator<? extends Entity> entityIterator) {
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
		private Iterator<? extends Entity> entityIterator;

		public ValueIterator(Iterator<? extends Entity> entityIterator) {
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
