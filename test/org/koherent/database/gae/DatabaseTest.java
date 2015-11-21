package org.koherent.database.gae;

import org.koherent.database.Value;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public abstract class DatabaseTest<I, V extends Value<I>, D extends Database<I, V>>
		extends org.koherent.database.DatabaseTest<I, V, D> {
	private final LocalServiceTestHelper helper = createLocalServiceTestHelper();

	protected LocalServiceTestHelper createLocalServiceTestHelper() {
		return new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
	}

	@Override
	protected void setUpDatabase() {
		helper.setUp();
	}

	@Override
	protected void tearDownDatabase() {
		helper.tearDown();
	}
}
