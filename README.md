Usage: java -jar DbImport.jar [optional parameters] dbtype hostname[:port] dbname, username -table tablename -import importfilepathOrData [password]<br />
<br />
Mandatory parameters for db import<br />
    dbtype: mysql | mariadb | oracle | postgresql | firebird | sqlite | derby | hsql | cassandra<br />
    hostname: With optional port (Not needed for sqlite and derby)<br />
    dbname: Dbname or filepath for sqlite db or derby db<br />
    username: Username (Not needed for sqlite and derby)<br />
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite and derby)<br />
    <br />
    -table tablename: Table to import to (* for name by filename)<br />
    -import importfilepathOrData: File to import (?, * as wildcards, automapping only), maybe zipped (.zip)<br />
                                or data as text (See optional parameter '-data')<br />
<br />
Optional parameters for db import<br />
    -data: Declare importfilepathOrData explicitly as inline data (No filepath)<br />
    -x importDataFormat: Data import format, default format is CSV<br />
        importDataFormat: CSV | JSON | XML | SQL | EXCEL | ODS<br />
    -dp <datapath>: Optional datapath within the XML-Data (XPath), JSON-Data or EXCEL/ODS-Data (Sheetname)<br />
    -sp <schemaFilePath>: Optional path to schema file for XML (XSD) or JSON<br />
    -m: Column mappings (Separated by semicolon or linebreak): When not configured a simple mapping by column names is used (Automapping)<br />
        Mapping entry format: dbcolumnname="data column name" <formatinfo><br />
        <formatinfo> may be decimal delimiter (Default .), date pattern (Default dd.MM.yyyy HH:mm:ss) or file or lc (Lowercase) or uc (Uppercase) or email<br />
        Example: 'db1="def 1" ,;db2="def 2" .;db3="def 3" dd.MM.yyyy HH:mm:ss;db4="def 4" file'<br />
    -mf: Column mapping file, containing the mapping entries of -m<br />
    -n 'NULL': Set a string for null values (Only for csv and xml, default is '')<br />
    -l: Log import information in .log files<br />
    -v: Progress and e.t.a. import in terminal<br />
    -e: Encoding for CSV and JSON data files and clob files (Default UTF-8)<br />
    -s: Separator character, default ';', encapsulate by ' (for CSV)<br />
    -q: String quote character, default '"', encapsulate by ', 'null' or 'none' for empty (for CSV)<br />
    -qe: String quote escape character, default '"', encapsulate by ' (for CSV)<br />
    -noheaders: First csv line is data and not headers<br />
    -c: Complete commit only (Takes more time and makes rollback on any error)<br />
    -nonewindex: Do not create new indexes on destination table, even if they are helpful (By default new indexes are created, which saves time)<br />
    -deactivatefk: Deactivate foreign key constraints during import<br />
    -a: Allow underfilled lines (for CSV)<br />
    -r: Allow lines with surplus empty trailing columns (for CSV)<br />
    -t: Trim data values<br />
    -i 'importmode':<br />
        CLEARINSERT: Deletes all existing data before insert. If key columns are set, duplicates are prevented<br />
        INSERT: (Default) Inserts all data. If key columns are set, inserts only not already existing data<br />
        UPDATE: Updates only exiting data, needs key columns<br />
        UPSERT: Updates and inserts, needs key columns<br />
    -d 'duplicatemode':<br />
        NO_CHECK: No duplicate check. Only insert<br />
        CKECK_SOURCE_ONLY_DROP: Check duplicates in import data only. Drop duplicates<br />
        CKECK_SOURCE_ONLY_JOIN: Check duplicates in import data only. Join duplicates<br />
        UPDATE_FIRST_DROP: Check duplicates in import data and db data. Only update the first occurrence in db. Drop duplicates<br />
        UPDATE_FIRST_JOIN: Check duplicates in import data and db data. Only update the first occurrence in db. Join duplicates<br />
        UPDATE_ALL_DROP: Check duplicates in import data and db data. Update all occurrences in db. Drop duplicates<br />
        UPDATE_ALL_JOIN: (Default) Check duplicates in import data and db data. Update all occurrences in db. Join duplicates<br />
        MAKE_UNIQUE_DROP: Check duplicates in import data and db data. Remove duplicate occurrences in db. Drop duplicates<br />
        MAKE_UNIQUE_JOIN: Check duplicates in import data and db data. Remove duplicate occurrences in db. Join duplicates<br />
    -u: Don't update with null values from import data<br />
    -k 'keycolumnslist': Keycolumns list comma separated<br />
    -insvalues 'valuelist': Value list semicolon separated: Sometimes values not included in the data file are needed for inserts. E.g.: id=test_seq.NEXTVAL;flag='abc'<br />
    -updvalues 'valuelist': Value list semicolon separated: Sometimes values not included in the data file are needed for updates. E.g.: create=current_timestamp;flag='abc'<br />
    -create: Scan data and create suitable table, if not exists. Also creates sqlite, derby and hsql db if needed<br />
    -logerrors: Log error data items in file<br />
    -zippassword '<password>'<br />
    -dbtz '<databaseTimeZone>' (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)<br />
    -idtz '<importDataTimeZone>' (Default is systems default timezone, e.g. Europe/Berlin or Europe/Dublin)<br />
    -secure: Use TLS/SSL for secure communication with database<br />
    -truststore '<truststorefilepath>': Filepath to TrustStore in JKS format for encrypted DB connections of some DB vendors<br />
    -truststorepassword '<password>': Optional password for TrustStore<br />
<br />
Global standalone parameters<br />
    help: Show this help manual<br />
    gui: Open a GUI<br />
    menu: Open a Console menu<br />
    version: Show current local version of this tool<br />
    update: Check for online update and ask, whether an available update shell be installed. [username [password]]<br />
<br />
Blob import:<br />
    Usage: java -jar DbImport.jar importblob dbtype hostname[:port] dbname username -updatesql sqlUpdateStatementWithPlaceholder -blobfile filePath [password]<br />
    <br />
    -updatesql sqlUpdateStatementWithPlaceholder: Import a single file as BLOB into a DB. (Placeholder for filedata is '?' like in prepared statements)<br />
    -blobfile filePath: Import blob file path<br />
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite, hsql or derby)<br />
<br />
Connection test:<br />
    Usage: java -jar DbImport.jar connectiontest dbtype hostname[:port] dbname username [-iter n] [-sleep n] [-check checksql] [password]<br />
    <br />
    -iter n: Iterations to execute. Default = 1, 0 = unlimited<br />
    -sleep n: Sleep for n seconds after each check. Default = 1<br />
    -check checksql: SQL statement to check or the keyword "vendor" for the vendors default check statement<br />
    password: Is asked interactivly, if not given as parameter (Not needed for sqlite, hsql or derby)<br />
<br />
Create TrustStore:<br />
    Usage: java -jar DbImport.jar createtruststore hostname:port truststorefilePath [truststorepassword]<br />
    <br />
    truststorefilePath: Filepath to create the TrustStore file in <br />
    truststorepassword: Optional password for the created TrustStore (JKS, JavaKeyStore)<br />
