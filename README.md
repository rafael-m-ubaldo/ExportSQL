ExportSQL by Rafael Ubaldo Sr. 7/17/2017

Commandline utilty that exports the data in one or more SQL database tables into a script

for import into another database. To import the data into another database, run the script in Management Studio.

The script optionally includes the create table statements.

For efficiency, table columns with all NULLs in each row are excluded from the script.

Usage:

Command line arguments: (case insensitive)

-s	The SQL Database Server computer/machine name 

-d	The database to be exported.

-u	Username

-p	Password

Note: Currently,only SQL authentication is supported. (later version will have Windows Authentication)

-o	The full file spec output SQL script file. For example, C:\Directory\output.sql. The filename is
	typically the same as the database name. The filename part will be suffixed with a datestamp
	like _YYYYMMDD which is the date of the export. If the output file exists, it's overwritten.
	If the file spec has space, place it between double-quotes ("C:\Output Dir\Some Database.sql")
	
-t	Comma delimited list of the tables to be exported. If this is ommited, all tables are exported.

-n  Do not include create table statements in the output script. If this is omitted, the create table
	statements are included. Note, the script assumes the table does not exist and is not checking.





