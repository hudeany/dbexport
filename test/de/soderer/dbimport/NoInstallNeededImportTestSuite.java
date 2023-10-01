package de.soderer.dbimport;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({
	DbImportTest_Derby.class,
	DbImportTest_HSQL.class,
	DbImportTest_SQLite.class
})

public class NoInstallNeededImportTestSuite {

}
