package org.koherent.database.gae;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.koherent.database.DatabaseException;
import org.koherent.database.DuplicateIdException;
import org.koherent.database.IdNotFoundException;
import org.koherent.database.Value;

public abstract class LongIdDatabaseTest<V extends Value<Long>, D extends LongIdDatabase<V>>
		extends DatabaseTest<Long, V, D> {
	protected abstract V createNewValue();

	protected abstract boolean equalsWithoutId(V value1, V value2);

	@Test
	public void testAdd() {
		D database = getDatabase();
		V newValue = createNewValue();

		try {
			Long id = database.add(newValue);
			V value = database.get(id);
			assertTrue(equalsWithoutId(newValue, value));
		} catch (DuplicateIdException | DatabaseException | IdNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
