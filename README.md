# DbExport

**Export data from relational and NoSQL databases into files, from the command line, an interactive console menu or a Swing GUI.**

DbExport writes the result of a SQL statement, or whole tables selected by name patterns, as CSV, JSON, YAML, XML, SQL, vCard or KeePass files. The output can be compressed and password protected, written to the terminal, or replaced by a JSON description of the table structure.

---

## Features

- **Many export formats**: CSV, JSON, YAML, XML, SQL insert statements, vCards and KeePass databases
- **Statements or tables**: export a single SQL statement into one file, or many tables (with wildcards and exclusions) into one file per table
- **Compressed output**: zip (optionally password protected with AES-256 or ZipCrypto), tar.gz, tgz and gz
- **10 database vendors**: MySQL, MariaDB, Oracle, PostgreSQL, MS SQL Server, Firebird, SQLite, Derby, HSQL and Cassandra
- **Readable output**: aligned CSV columns, and indented JSON and XML
- **Flexible formats**: locale, date, date time and decimal formats, time zone conversion, null value text, quoting and escaping
- **Large objects**: blobs as base64 text or separate files, clobs as text or separate files
- **Table structure**: export the columns, data types, keys, indices, foreign keys and constraints of tables as JSON
  (usable with DbImport `-create -structure`)
- **Date placeholders**: `[YYYY]`, `[MM]`, `[DD]`, `[hh]`, `[mm]`, `[ss]` in output paths, e.g. for daily exports
- **Secure connections**: TLS/SSL with your own TrustStore, which DbExport can also create for you
- **Extras**: logging of each export and a database connection test
- **Three ways to use it**: command line, console menu (also in headless environments) and GUI

## Supported export formats

| Format | `-x` | File extension | Notes |
|---|---|---|---|
| CSV | `CSV` (default) | `.csv` | Configurable separator, string quote, escape character, null value text and headers |
| JSON | `JSON` | `.json` | Array of objects, optionally beautified with configurable indentation |
| YAML | `YAML` | `.yaml` | List of mappings |
| XML | `XML` | `.xml` | `table` element with one `line` element per data line, optionally beautified |
| SQL | `SQL` | `.sql` | `INSERT` statements, the table name is taken from the `FROM` clause |
| vCard | `VCF` | `.vcf` | One vCard per data line |
| KeePass | `KDBX` | `.kdbx` | One entry per data line, password via `-kdbxpassword` |

## Supported databases

| Vendor | `dbtype` | Notes |
|---|---|---|
| MySQL | `mysql` | TLS/SSL supported |
| MariaDB | `mariadb` | TLS/SSL supported |
| Oracle | `oracle` | SID, service name (`/<servicename>`) or TNS description, TLS/SSL supported |
| PostgreSQL | `postgresql` | |
| MS SQL Server | `mssql` | TLS/SSL supported |
| Firebird | `firebird` | |
| SQLite | `sqlite` | File based, no host and user needed |
| Derby | `derby` | File based, no host and user needed |
| HSQL | `hsql` | Server or file based (see [HSQL file databases](#hsql-file-databases)) |
| Cassandra | `cassandra` | |

**JDBC drivers:** If the driver of a database vendor is not available, DbExport asks once for the driver jar file (in a file dialog or on the console). The location is stored in `~/.DbExport/.DbExport.config`.

## Requirements

- Java 17 or newer
- The JDBC driver of your database (see above)

## Getting started

```bash
# Open the GUI (also the default when started without parameters)
java -jar DbExport.jar

# Open the console menu (default without parameters in headless environments)
java -jar DbExport.jar menu

# Show the help
java -jar DbExport.jar help
```

### Examples

Export the result of a statement into a CSV file (the password is asked interactively):

```bash
java -jar DbExport.jar postgresql localhost:5432 mydb myuser \
  -export "SELECT id, name, email FROM customer WHERE active = 1" -output ./customer.csv
```

Export all tables except those starting with `tmp_` as beautified JSON, one file per table:

```bash
java -jar DbExport.jar -x JSON -beautify mysql dbserver mydb myuser \
  -export "*,!tmp_*" -output ./export
```

Daily export into a password protected zip file:

```bash
java -jar DbExport.jar -compress ZIP -zippassword 'secret' oracle dbserver /ORCLPDB myuser \
  -export "SELECT * FROM orders" -output "./orders_[YYYY]-[MM]-[DD].csv"
```

Print a table of a SQLite database to the terminal:

```bash
java -jar DbExport.jar sqlite ./data.sqlite -export "SELECT * FROM orders" -output console
```

Export the structure of some tables as JSON (no data):

```bash
java -jar DbExport.jar postgresql localhost mydb myuser \
  -export "customer, orders" -output ./export -structure dbstructure.json
```

## Command line reference

```
java -jar DbExport.jar [optional parameters] dbtype hostname[:port] dbname username -export exportdata -output outputpath [password]
```

### Mandatory parameters

| Parameter | Description |
|---|---|
| `dbtype` | `mysql` \| `mariadb` \| `oracle` \| `postgresql` \| `mssql` \| `firebird` \| `sqlite` \| `derby` \| `hsql` \| `cassandra` |
| `hostname[:port]` | Database host with optional port (not needed for SQLite, Derby and HSQL file databases) |
| `dbname` | Database name, or file path for SQLite, Derby and HSQL file databases (Oracle: SID, `/<servicename>` or TNS description) |
| `username` | Database user (not needed for SQLite and Derby) |
| `password` | Asked interactively, if not given (not needed for SQLite, Derby and HSQL file databases) |
| `-export exportdata` | SQL statement, or comma separated list of table names with the wildcards `*` and `?` (`!` as prefix excludes tables), or the path of a text file containing one of them (see `-file`) |
| `-output outputpath` | File for a single statement, directory for table patterns, or `console` for output to the terminal. May contain the date placeholders `[YYYY]`, `[MM]`, `[DD]`, `[hh]`, `[mm]`, `[ss]`. |

### Export format

| Parameter | Description |
|---|---|
| `-x format` | `CSV` \| `JSON` \| `YAML` \| `XML` \| `SQL` \| `VCF` \| `KDBX` (default: `CSV`) |
| `-file` | Read the statement or table patterns from the text file given by `-export` |
| `-e encoding` | Output encoding (default: UTF-8) |
| `-n 'NULL'` | Text for null values (CSV and XML, default: empty text) |
| `-s separator` | CSV separator character (default: `;`) |
| `-q quote` | CSV string quote character (default: `"`) |
| `-qe escape` | CSV string quote escape character (default: `"`) |
| `-a` | Always quote CSV values |
| `-noheaders` | Don't export the CSV header line |
| `-noescapesequences` | Write line breaks and tabs in CSV values as raw characters instead of `\n` and `\t` |
| `-beautify` | Align CSV columns to equal length (takes extra time), or indent JSON and XML output |
| `-i indentation` | Indentation for JSON and XML: `TAB` (default), `BLANK`, `DOUBLEBLANK` or any text |
| `-kdbxpassword 'password'` | Password of the KeePass file (required for `KDBX`) |

### Values and formats

| Parameter | Description |
|---|---|
| `-f locale` | Locale of the date and number formats, e.g. `de` or `en` (default: system locale) |
| `-dateFormat 'pattern'` | Date format, overrides the locale (Java format characters) |
| `-dateTimeFormat 'pattern'` | Date time format, overrides the locale (Java format characters) |
| `-decimalSeparator 'char'` | Decimal separator `.` or `,`, overrides the locale |
| `-dbtz 'timezone'` | Time zone of the database (default: system time zone, e.g. `Europe/Berlin`) |
| `-edtz 'timezone'` | Time zone of the exported date values (default: system time zone) |
| `-blobfiles` | Create a file (`.blob` or `.blob.zip`) for each blob instead of base64 encoded text |
| `-clobfiles` | Create a file (`.clob` or `.clob.zip`) for each clob instead of the text |

### Output files

| Parameter | Description |
|---|---|
| `-z` | Output as zip file (same as `-compress ZIP`) |
| `-compress type` | Compress the output: `ZIP` \| `TARGZ` \| `TGZ` \| `GZ` (not for console output) |
| `-zippassword 'password'` | Password of zip files (AES-256 by default) |
| `-useZipCrypto` | Use the weak ZipCrypto encryption instead of AES-256, which is supported by Windows |
| `-structure filename` | Export the structure of the selected tables as JSON file instead of the data. A file name without directory is created in the output directory or next to the output file. |
| `-createOutputDirectoyIfNotExists` | Create the output directory if it is missing |
| `-replaceAlreadyExistingFiles` | Replace already existing export files |
| `-l` | Log the export information in a `.log` file next to the output file |
| `-v` | Show progress and estimated time on the console |

### Connection security

| Parameter | Description |
|---|---|
| `-secure` | Use TLS/SSL for the database connection |
| `-truststore 'filepath'` | TrustStore (JKS) for encrypted connections |
| `-truststorepassword 'password'` | Optional password of the TrustStore |

### Global parameters

| Parameter | Description |
|---|---|
| `help` | Show the help (only as single parameter) |
| `version` | Show the installed version (only as single parameter) |
| `gui` | Open the GUI (only as first parameter, optionally followed by export parameters to prefill) |
| `menu` | Open the console menu (only as first parameter, optionally followed by export parameters to prefill) |
| `update [username [password]]` | Check for an online update and install it after confirmation |

## HSQL file databases

For HSQL the hostname is only needed for a server database. A file database is given by its path instead of the hostname. The path must contain a path separator or start with `.` or `~`, otherwise it is taken as hostname:

```bash
java -jar DbExport.jar hsql ./mydb SA -export "SELECT * FROM orders" -output ./orders.csv
```

## Further tools

### Connection test

Connects to the database repeatedly and optionally executes a check statement:

```
java -jar DbExport.jar connectiontest dbtype hostname[:port] dbname username [-iter n] [-sleep n] [-check checksql] [password]
```

| Parameter | Description |
|---|---|
| `-iter n` | Number of checks (default: 1, 0 = unlimited) |
| `-sleep n` | Seconds to wait after each check (default: 1) |
| `-check checksql` | SQL statement to execute, or `vendor` for the vendor's default check statement |

### Create TrustStore

Creates a TrustStore (JKS) with the certificates of a database server:

```
java -jar DbExport.jar createtruststore hostname:port truststorefilePath [truststorepassword]
```

## Configuration files

DbExport stores its files in `~/.DbExport/`:

| File | Content |
|---|---|
| `.DbExport.config` | Application configuration, e.g. the locations of JDBC driver files |
| `.DbExport.secpref` | Encrypted connection preferences of the GUI and console menu |

