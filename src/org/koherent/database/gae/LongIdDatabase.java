package org.koherent.database.gae;

import org.koherent.database.Value;

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

	public static Long keyToId(LongIdDatabase<Value<Long>> database, Key key) {
		return database.toId(key);
	}

	public static Key idToKey(LongIdDatabase<Value<Long>> database, Long id) {
		return database.toKey(id);
	}
}
