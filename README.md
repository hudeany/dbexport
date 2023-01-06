# Java tool to export db data into csv, json, xml or sql files 

## Comandline usage
	Usage: java -jar DbExport.jar [optional parameters] dbtype hostname[:port] dbname username -export exportdata -output outputpath [password]

	Simple usage: java -jar DbExport.jar dbtype hostname username dbname 'statement or list of tablepatterns' outputpath

## Mandatory parameters for db export
	dbtype: mysql | mariadb | oracle | postgresql | firebird | sqlite | derby | hsql | cassandra
	hostname: With optional port (Not needed for sqlite, hsql and derby)
	dbname: Dbname or filepath for sqlite db or derby db
	username: Username (Not needed for sqlite and derby)
	password: Is asked interactivly, if not given as parameter (Not needed for sqlite, hsql or derby)
	
	-export exportdata: SQL statement (Encapsulate by ', escape sequence is '')
						or comma-separated list of tablenames with wildcards *? and !(= not, use as tablename prefix)
						or text file path (See optional parameter '-file')
	-output outputpath: File for single statement
						or directory for tablepatterns
						or 'console' for output to terminal
						may contain datetime placeholders ([YYYY], [MM], [DD], [hh], [mm], [ss])

## Optional parameters for db export
	-x exportformat: Data export format, default format is CSV
		exportformat: CSV | JSON | XML | SQL
		(Don't forget to beautify json for human readable data)
	-n 'NULL': Set a string for null values (Only for csv and xml, default is '')
	-file: Read statement or tablepattern from text file
	-l: Log export information in .log files
	-v: Progress and e.t.a. output in terminal
	-z: Output as zipfile (Not for console output)
	-zippassword '<password>' (using AES-256 by default, not supported by Windows)
	-useZipCrypto (use ZipCrypto algorithm, which is weak but is supported by Windows)
	-e: Output encoding (Default UTF-8)
	-s: Separator character, default ';', encapsulate by '
	-q: String quote character, default '"', encapsulate by '
	-qe: String quote escape character, default '"', encapsulate by '
	-i: Indentation string for JSON and XML (TAB, BLANK, DOUBLEBLANK), default TAB or '\t', encapsulate by '
	-a: always quote value
	-f: Number and datetime format locale, default is systems locale, use 'de', 'en', etc. (Not needed for sqlite)
	-dateFormat: overrides language format, use Java format characters (YMdhmsS) 
	-dateTimeFormat: overrides language format, use Java format characters (YMdhmsS)
	-decimalSeparator: overrides language format, use '.' or ','
	-blobfiles: Create a file (.blob or .blob.zip) for each blob instead of base64 encoding
	-clobfiles: Create a file (.clob or .clob.zip) for each clob instead of data in csv file
	-beautify: Beautify csv output to make column values equal length (Takes extra time)
		or beautify json output to make it human readable with linebreak and indention
	-noheaders: Don't export csv header line
	-structure: Export the tables structure and column types
	-dbtz '<databaseTimeZone>': Use a DatabaseTimeZone (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)
	-edtz '<exportDataTimeZone>': Use a ExportDataTimeZone (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)
	-secure: Use TLS/SSL for secure communication with database
	-truststore '<truststorefilepath>': Filepath to TrustStore in JKS format for encrypted DB connections of some DB vendors
	-truststorepassword '<password>': Optional password for TrustStore

## Global standalone parameters
	help: Show this help manual
	gui: Open a GUI (Other parameters may also be preconfigured)
	menu: Open a Console menu (Other parameters may also be preconfigured)
	version: Show current local version of this tool
	update: Check for online update and ask, whether an available update shell be installed;

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
