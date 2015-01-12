package org.koherent.database.gae;

import org.koherent.database.Value;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

public abstract class StringIdDatabase<V extends Value<String>> extends
		Database<String, V> {
	@Override
	protected String toId(Key key) {
		return key.getName();
	}

	@Override
	protected Key toKey(String id) {
		return KeyFactory.createKey(getKind(), id);
	}

	public static <V extends Value<String>> String keyToId(
			StringIdDatabase<V> database, Key key) {
		return database.toId(key);
	}

	public static <V extends Value<String>> Key idToKey(
			StringIdDatabase<V> database, String id) {
		return database.toKey(id);
	}
}
