# Java tool to import data in db from csv, json, xml, vcf, xls, xlsx, ods and kdbx files
## Commandline usage
	Usage: java -jar DbImport.jar [optional parameters] dbtype hostname[:port] dbname, username -table tablename -import importfilepathOrData [password]

## Mandatory parameters for db import
    dbtype: mysql | mariadb | oracle | postgresql | firebird | sqlite | derby | hsql | cassandra
    hostname: With optional port (Not needed for sqlite and derby)
    dbname: Dbname or filepath for sqlite db or derby db
    username: Username (Not needed for sqlite and derby)
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite and derby)
    
    -table tablename: Table to import to (* for name by filename)
    -import importfilepathOrData: File to import (?, * as wildcards, automapping only), maybe zipped (.zip, .tar.gz, .tgz, .gz)
                                or data as text (See optional parameter '-data')

## Optional parameters for db import
	-data: Declare importfilepathOrData explicitly as inline data (No filepath)
	-x importDataFormat: Data import format, default format is detected by file extension or, if it cannot be detected, default is CSV
		importDataFormat: CSV | JSON | XML | SQL | EXCEL | ODS | VCF | KDBX
	-dp <datapath>: Optional datapath within the XML-Data (XPath), JSON-Data or EXCEL/ODS-Data (Sheetname)
	-sp <schemaFilePath>: Optional path to schema file for XML (XSD) or JSON
	-m: Column mappings (Separated by semicolon or linebreak): When not configured a simple mapping by column names is used (Automapping)
		Mapping entry format: dbcolumnname="data column name" <formatinfo>
		<formatinfo> may be decimal delimiter (Default .), date pattern (Default dd.MM.yyyy HH:mm:ss) or file or lc (Lowercase) or uc (Uppercase) or email
		Example: 'db1="def 1" ,;db2="def 2" .;db3="def 3" dd.MM.yyyy HH:mm:ss;db4="def 4" file'
	-mf: Column mapping file, containing the mapping entries of -m
	-n 'NULL': Set a string for null values (Only for csv and xml, default is '')
	-l: Log import information in .log files
	-v: Progress and e.t.a. import in terminal
	-e: Encoding for CSV and JSON data files and clob files (Default UTF-8)
	-s: Separator character, default ';', encapsulate by ' (for CSV)
	-q: String quote character, default '"', encapsulate by ', 'null' or 'none' for empty (for CSV)
	-qe: String quote escape character, default '"', encapsulate by ' (for CSV)
	-noheaders: First csv line is data and not headers
	-c: Complete commit only (Takes more time and makes rollback on any error)
	-nonewindex: Do not create new indexes on destination table, even if they are helpful (By default new indexes are created, which saves time)
	-deactivatefk: Deactivate foreign key constraints during import
	-deactivatetriggers: Deactivate triggers during import
	-a: Allow underfilled lines (for CSV)
	-r: Allow lines with surplus empty trailing columns (for CSV)
	-t: Trim data values
	-i 'importmode':
		CLEARINSERT: Deletes all existing data before insert. If key columns are set, duplicates are prevented
		INSERT: (Default) Inserts all data. If key columns are set, inserts only not already existing data
		UPDATE: Updates only exiting data, needs key columns
		UPSERT: Updates and inserts, needs key columns
	-d 'duplicatemode':
		NO_CHECK: No duplicate check. Only insert
		CKECK_SOURCE_ONLY_DROP: Check duplicates in import data only. Drop duplicates
		CKECK_SOURCE_ONLY_JOIN: Check duplicates in import data only. Join duplicates
		UPDATE_FIRST_DROP: Check duplicates in import data and db data. Only update the first occurrence in db. Drop duplicates
		UPDATE_FIRST_JOIN: Check duplicates in import data and db data. Only update the first occurrence in db. Join duplicates
		UPDATE_ALL_DROP: Check duplicates in import data and db data. Update all occurrences in db. Drop duplicates
		UPDATE_ALL_JOIN: (Default) Check duplicates in import data and db data. Update all occurrences in db. Join duplicates
		MAKE_UNIQUE_DROP: Check duplicates in import data and db data. Remove duplicate occurrences in db. Drop duplicates
		MAKE_UNIQUE_JOIN: Check duplicates in import data and db data. Remove duplicate occurrences in db. Join duplicates
	-u: Don't update with null values from import data
	-k 'keycolumnslist': Keycolumns list comma separated
	-insvalues 'valuelist': Value list semicolon separated: Sometimes values not included in the data file are needed for inserts. E.g.: id=test_seq.NEXTVAL;flag='abc'
	-updvalues 'valuelist': Value list semicolon separated: Sometimes values not included in the data file are needed for updates. E.g.: create=current_timestamp;flag='abc'
	-create: Scan data and create suitable table, if not exists. Also creates sqlite, derby and hsql db if needed
	-structure <structureFilePath>: Optional path to db structure JSON file to be used with "-create" parameter
	-logerrors: Log error data items in file
	-zippassword '<zippassword>' (Only for .zip files)
	-kdbxpassword '<kdbxpassword>' (Only for .kdbx files)
	-dbtz '<databaseTimeZone>' (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)
	-idtz '<importDataTimeZone>' (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)
	-dateFormat: set fallback date format, especially for multiple file imports, can be overridden by mapping formats, use Java format characters (YMdhmsS) 
	-dateTimeFormat: set fallback date time format, especially for multiple file imports, can be overridden by mapping formats, use Java format characters (YMdhmsS)
	-secure: Use TLS/SSL for secure communication with database
	-truststore '<truststorefilepath>': Filepath to TrustStore in JKS format for encrypted DB connections of some DB vendors
	-truststorepassword '<password>': Optional password for TrustStore

## Global standalone parameters
    help: Show this help manual
    gui: Open a GUI
    menu: Open a Console menu
    version: Show current local version of this tool
    update: Check for online update and ask, whether an available update shell be installed. [username [password]]

## Blob import:
    Usage: java -jar DbImport.jar importblob dbtype hostname[:port] dbname username -updatesql sqlUpdateStatementWithPlaceholder -blobfile filePath [password]
    
    -updatesql sqlUpdateStatementWithPlaceholder: Import a single file as BLOB into a DB. (Placeholder for filedata is '?' like in prepared statements)
    -blobfile filePath: Import blob file path
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite, hsql or derby)

## Connection test:
    Usage: java -jar DbImport.jar connectiontest dbtype hostname[:port] dbname username [-iter n] [-sleep n] [-check checksql] [password]
    
    -iter n: Iterations to execute. Default = 1, 0 = unlimited
    -sleep n: Sleep for n seconds after each check. Default = 1
    -check checksql: SQL statement to check or the keyword "vendor" for the vendors default check statement
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite, hsql or derby)

## Create TrustStore:
    Usage: java -jar DbImport.jar createtruststore hostname:port truststorefilePath [truststorepassword]
    
    truststorefilePath: Filepath to create the TrustStore file in 
    truststorepassword: Optional password for the created TrustStore (JKS, JavaKeyStore)
