package org.koherent.database.gae;

import static org.junit.Assert.assertFalse;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public abstract class SymbolicTest {
	protected abstract Symbolic[] getSymbolics();

	@Test
	public void testDuplicateSymbol() {
		Set<String> symbols = new HashSet<String>();

		for (Symbolic symbolic : getSymbolics()) {
			String symbol = symbolic.getSymbol();
			assertFalse(symbols.contains(symbol));
			symbols.add(symbol);
		}
	}
}
