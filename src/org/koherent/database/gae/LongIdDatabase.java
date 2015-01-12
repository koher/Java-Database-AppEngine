package org.koherent.database.gae;

import org.koherent.database.Value;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

public abstract class LongIdDatabase<V extends Value<Long>> extends
		Database<Long, V> {
	@Override
	protected Long toId(Key key) {
		return key.getId();
	}

	@Override
	protected Key toKey(Long id) {
		return KeyFactory.createKey(getKind(), id);
	}

	protected Entity createNewEntity() {
		return new Entity(getKind());
	}

	protected Entity createEntity(Long id) {
		return id == null ? createNewEntity() : new Entity(toKey(id));
	}
}
