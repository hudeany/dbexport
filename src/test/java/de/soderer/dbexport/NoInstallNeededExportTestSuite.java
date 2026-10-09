package de.soderer.dbexport;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
	DbExportTest_Derby.class,
	DbExportTest_HSQL.class,
	DbExportTest_SQLite.class
})
public class NoInstallNeededExportTestSuite {
	// Only the annotations are used
}
