package de.soderer.dbexport;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({
	DbExportTest_Derby.class,
	DbExportTest_HSQL.class,
	DbExportTest_SQLite.class
})

public class NoInstallNeededExportTestSuite {

}
