using System;
using System.IO;
using System.Data;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using System.Windows.Forms;

/* ExportSQL by Rafael Ubaldo Sr. 7/17/2017
*  Commandline utilty that exports the data in one or more SQL database tables into a script
*  for import into another database. To import, run the script in Management Studio.
*  The script optionally includes the create table statements.
*  For efficiency, table columns with all NULLs in each row are excluded from the script.
*/

namespace ExportSQL
{
    class Program
    {
        static List<TableColumnInfo> TableInformation = new List<TableColumnInfo>();
        static StringBuilder FinalScript = new StringBuilder();
        static bool TableScripts = true;
        static string Server = string.Empty;
        static string Database = string.Empty;
        static string Username = string.Empty;
        static string Password = string.Empty;
        static string OutputName = string.Empty;
        static List<string> Tables = new List<string>();
        static string Tablename = string.Empty;
        static string OrderBy = string.Empty;
        static Boolean MultipleRow = true;
        static string Delimiter = "\t";
        static DateTime exportTime = DateTime.Now;

        static void Main(string[] args)
        {
            if(!ReadOptions(args))
            {
                return;
            }
            try
            {
                List<string> scripts = new List<string>();
                List<string> truncates = new List<string>();
                TableScripter scripter = new TableScripter(Server, Database, Username, Password);

                if (Tables.Count == 0)
                {
                    GetAllTablenames();
                }

                foreach (string table in Tables)
                {
                    Tablename = table;
                    GetTableInfo();
                    InsertScript ScriptOut = new InsertScript(Tablename, TableInformation, Delimiter);
                    string name = string.Empty;
                    ScriptOut.HasIdentity = HasIndentity(ref name);
                    ScriptOut.IdentityFieldname = name;
                    ScriptOut.MultipleRow = MultipleRow;
                    ReadTable(ScriptOut);
                    if (ScriptOut.RowCount() > 0)
                    {
                        ScriptOut.RemoveColumnsWithAllNulls();
                        String sout = ScriptOut.SQL();
                        byte[] b = Encoding.Unicode.GetBytes(sout);
                        string s = Encoding.Unicode.GetString(b);

                        scripts.Add(s);
                        truncates.Add(ScriptOut.TruncateTable);
                    }
                }
                FinalScript.AppendFormat("USE {0}\r\n", Database);
                FinalScript.AppendLine("GO");

                if (TableScripts)
                {
                    foreach (string table in Tables)
                    {
                        FinalScript.AppendLine(scripter.ScriptTable(table));
                    }
                }
                truncates.Reverse();
                foreach(string truc in truncates)
                {
                    FinalScript.AppendLine(truc);
                    FinalScript.AppendLine("GO");
                }
                foreach (string script in scripts)
                {
                    FinalScript.Append(script);
                }
                File.WriteAllText(OutputName, FinalScript.ToString());
                SetFileDate(OutputName, exportTime);
            }
            catch (Exception e)
            {
                Console.WriteLine(Tablename + " " + e.Message);
            }
        }
    
        static bool ReadOptions(string[] args)
        {
            bool result = true;
            try
            {
                int idx = 0;
                while (idx < args.Length)
                {
                    string arg = args[idx++];
                    if (arg.Equals("-s", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Server = args[idx++];
                    }
                    else if (arg.Equals("-d", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Database = args[idx++];
                    }
                    else if (arg.Equals("-u", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Username = args[idx++];
                    }
                    else if (arg.Equals("-p", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Password = args[idx++];
                    }
                    else if (arg.Equals("-o", StringComparison.CurrentCultureIgnoreCase))
                    {
                        string filepath = args[idx++];
                        string path = Path.GetDirectoryName(filepath);
                        string filename = Path.GetFileNameWithoutExtension(filepath);
                        string ext = Path.GetExtension(filepath);
                        if (string.IsNullOrEmpty(ext))
                        {
                            ext = ".sql";
                        }
                        filename = string.Format("{0}_{1}{2}", filename, DateString(exportTime),ext);
                        OutputName = Path.Combine(path, filename);
                    }
                    else if (arg.Equals("-t", StringComparison.CurrentCultureIgnoreCase))
                    {
                        string[] tables = args[idx++].Split(new char[] { ',' });
                        foreach(string t in tables)
                        {
                            Tables.Add(t);
                        }
                    }
                    else if (arg.Equals("-n", StringComparison.CurrentCultureIgnoreCase))
                    {
                        TableScripts = false;
                    }
                    else
                    {
                        result = false;
                        throw new ArgumentException(string.Format("Unknown option: {0}", arg));
                    }
                }
                if (Server == string.Empty)
                {
                    Console.WriteLine("No Server.");
                    result = false;
                }
                if (Database == string.Empty)
                {
                    Console.WriteLine("No Database.");
                    result = false;
                }
                if (Username == string.Empty)
                {
                    Console.WriteLine("No Username.");
                    result = false;
                }
                if (Password == string.Empty)
                {
                    Console.WriteLine("No Password.");
                    result = false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("Error reading command-line options: {0}", ex.Message));
                result = false;
            }
            return result;
        }

        static string ConnectionString
        {
            get
            {
                SqlConnectionStringBuilder connstr = new SqlConnectionStringBuilder();
                // Build connection string
                connstr["Data Source"] = Server;
                connstr["Persist Security Info"] = "True";
                connstr["Initial Catalog"] = Database;
                connstr["User ID"] = Username;
                connstr["Password"] = Password;
                return connstr.ConnectionString;
            }
        }

        static void GetAllTablenames()
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                Tables.Clear();
                SqlCommand cmd = new SqlCommand("SELECT name FROM sys.tables", conn);
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    string tablename = reader.GetString(reader.GetOrdinal("name"));
                    Tables.Add(tablename);
                }
                conn.Close();
            }
        }

            static void GetTableInfo()
        {
            TableInformation.Clear();
            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT COLUMN_NAME, CAST(CASE WHEN (COLLATION_NAME IS NULL AND DATA_TYPE <> 'xml') THEN 0 ELSE 1 END AS BIT) Quoted,")
                .Append(" CAST(CASE IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS BIT) IsNullAble")
                .Append(" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ")
                .Append("'").Append(Tablename).Append("'");
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand cmd = new SqlCommand(sql.ToString(), conn);
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    TableColumnInfo colInf = new TableColumnInfo();
                    colInf.ColumnName = reader.GetString(reader.GetOrdinal("COLUMN_NAME"));
                    colInf.Quoted = reader.GetBoolean(reader.GetOrdinal("Quoted"));
                    colInf.NullAble = reader.GetBoolean(reader.GetOrdinal("IsNullAble"));
                    TableInformation.Add(colInf);
                }
                conn.Close();
            }
        }

        static bool HasIndentity(ref string FieldName)
        {
            bool result = false;
            StringBuilder query = new StringBuilder();
            query.Append("SELECT name FROM sys.all_columns WHERE object_id = OBJECT_ID('").Append(Tablename).Append("') and is_identity = 1");
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand cmd = new SqlCommand(query.ToString(), conn);
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    FieldName = reader.GetString(reader.GetOrdinal("name"));
                    result = true;
                }
                conn.Close();
            }
            return result;
        }

        static string DatetimeToHex(DateTime dt)
        {
            DateTime ep = Convert.ToDateTime("1/1/1900");
            int days = Convert.ToInt32(dt.Subtract(ep).TotalDays);
            double dticks = dt.TimeOfDay.TotalMilliseconds * 300 / 1000;
            int ticks = Convert.ToInt32(dticks);
            string datepart = days.ToString("X8");
            string timepart = ticks.ToString("X8");
            return "0x" + datepart + timepart;
        }

        static string DateString(DateTime dt, bool IncludeTime = false)
        {
            if (IncludeTime)
            {
                return string.Format("{0:D4}{1:D2}{2:D2}_{3:D2}{4:D2}{5:D2}", dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
            }
            else
            {
                return string.Format("{0:D4}{1:D2}{2:D2}", dt.Year, dt.Month, dt.Day);
            }
        }

        static void SetFileDate(string filename, DateTime filedate)
        {
            File.SetCreationTime(filename, filedate);
            File.SetLastWriteTime(filename, filedate);
            File.SetLastAccessTime(filename, filedate);
        }

        static void ReadTable(InsertScript ScriptOut)
        {
            StringBuilder query = new StringBuilder();
            query.Append("SELECT ").Append(ScriptOut.Columns).Append(" FROM ").Append(Tablename);
            if (!String.IsNullOrEmpty(OrderBy))
            {
                query.Append(" ORDER BY ").Append(OrderBy);
            }
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand cmd = new SqlCommand(query.ToString(), conn);
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    InsertRow row = new InsertRow(TableInformation);
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Type type = reader.GetFieldType(i);
                        string column = reader.GetName(i);
                        string data = string.Empty;
                        if (reader.IsDBNull(i))
                        {
                            data = null;// string.Empty;
                        }
                        else if (type == typeof(SqlGuid))
                        {
                            data = reader.GetGuid(i).ToString();
                        }
                        else if (type == typeof(Int32))
                        {
                            data = reader.GetInt32(i).ToString();
                        }
                        else if (type == typeof(String))
                        {
                            // TODO: Handle CR/LF, TAB, etc
                            // See GetRow()
                            data = reader.GetString(i);
                        }
                        else if (type == typeof(Int64))
                        {
                            data = reader.GetInt64(i).ToString();
                        }
                        else if (type == typeof(Int16))
                        {
                            data = reader.GetInt16(i).ToString();
                        }
                        else if (type == typeof(Boolean))
                        {
                            // data = reader.GetBoolean(i).ToString(); Can't use True and False !
                            if (reader.GetBoolean(i))
                            {
                                data = "1";
                            }
                            else
                            {
                                data = "0";
                            }
                        }
                        else if (type == typeof(Decimal))
                        {
                            data = reader.GetDecimal(i).ToString();
                        }
                        else if (type == typeof(float))
                        {
                            data = reader.GetFloat(i).ToString();
                        }
                        else if (type == typeof(double))
                        {
                            data = reader.GetDouble(i).ToString();
                        }
                        else if (type == typeof(Byte))
                        {
                            data = reader.GetByte(i).ToString();
                        }
                        else if (type == typeof(Byte[]))
                        {
                            long len = reader.GetBytes(i, 0, null, 0, 0);
                            Byte[] buf = new Byte[len];
                            reader.GetBytes(i, 0, buf, 0, (int)len);
                            StringBuilder sb = new StringBuilder();
                            sb.Append("0x");
                            for (int bi = 0; bi < len; bi++)
                            {
                                sb.Append(buf[bi].ToString("X2"));
                            }
                            data = sb.ToString();
                        }
                        else if (type == typeof(DateTime))
                        {
                            DateTime dt = reader.GetDateTime(i);
                            data = string.Format("CAST({0} AS DateTime)", DatetimeToHex(dt));
                        }
                        else
                        {
                            string msg = string.Format("[Missing Type: {0}]", type.ToString());
                            data = msg;

                            throw new Exception(msg);
                        }
                        row.AddColumn(data);
                    }
                    ScriptOut.AddRow(row);
                }
                conn.Close();
            }
        }
    }

    public class TableColumnInfo
    {
        public TableColumnInfo()
        {
        }
        public string ColumnName { get; set; }
        public bool Quoted { get; set; }
        public bool NullAble { get; set; }
    }

    public class InsertRow : Object
    {
        List<string> cols = new List<string>();

        List<TableColumnInfo> colInfo;

        public InsertRow()
        {
        }

        public InsertRow(List<TableColumnInfo> columnInformation)
        {
            colInfo = columnInformation;
        }

        public string DataAtColumn(int index)
        {
            return cols[index];
        }

        public void RemoveColumn(int index)
        {
            cols.RemoveAt(index);
        }

        public void AddColumn(string col)
        {
            cols.Add(col);
        }

        public void ParseLine(string line, string delimter)
        {
            int idx = 0;
            int pos = 0;
            if (line.IndexOf(delimter) != -1)
            {
                while (idx < line.Length)
                {
                    pos = line.IndexOf(delimter, idx);
                    if (pos >= idx)
                    {
                        string data = line.Substring(idx, pos - idx);
                        cols.Add(data);
                        idx = pos + delimter.Length;
                        if (cols.Count + 1 == colInfo.Count)
                        {
                            data = line.Substring(idx, line.Length - idx);
                            cols.Add(data);
                            idx = line.Length;
                        }
                    }
                }
            }
        }

        public string GetRow()
        {
            StringBuilder row = new StringBuilder();
            int cnt = 0;
            foreach (string col in cols)
            {
                if (cnt > 0)
                    row.Append(", ");
                if (col == null)
                {
                    row.Append("NULL");
                }
                else if (!colInfo[cnt].Quoted)
                {
                    row.Append(col);
                }
                else
                {
                    Boolean inText = true;
                    string sCol = col.Replace("'", "''");
                    if ((sCol.IndexOf("\t") != -1) || (sCol.IndexOf("\r") != -1) || (sCol.IndexOf("\n") != -1))
                    {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < sCol.Length; i++)
                        {
                            char cx = sCol[i];
                            switch (cx)
                            {
                                case '\t':
                                    if (inText)
                                    {
                                        sb.Append("'");
                                        inText = false;
                                    }
                                    sb.Append("+CHAR(9)");
                                    break;
                                case '\r':
                                    if (inText)
                                    {
                                        sb.Append("'");
                                        inText = false;
                                    }
                                    sb.Append("+CHAR(13)");
                                    break;
                                case '\n':
                                    if (inText)
                                    {
                                        sb.Append("'");
                                        inText = false;
                                    }
                                    sb.Append("+CHAR(10)");
                                    break;
                                default:
                                    if (!inText)
                                    {
                                        sb.Append("+'");
                                        inText = true;
                                    }
                                    sb.Append(cx);
                                    break;
                            }
                        }
                        sCol = sb.ToString();
                    }
                    row.Append("N'").Append(sCol);
                    if (inText) row.Append("'");
                }
                cnt++;
            }

            return row.ToString();
        }
    }

    public class InsertRowInfo : Object
    {
        public InsertRowInfo(string cmd, int rowcount)
        {
            this.cmd = cmd;
            this.rowcount = rowcount;
        }
        public string cmd { get; set; }
        public int rowcount { get; set; }
    }

    public class InsertScript : Object
    {
        List<InsertRow> InsertRows = new List<InsertRow>();

        List<TableColumnInfo> tableInfo;

        public InsertScript(string tableName, List<TableColumnInfo> tableInformation, string Delimiter)
        {
            this.TableName = tableName;
            this.HasIdentity = false;
            this.tableInfo = tableInformation;
            this.Delimiter = Delimiter;
            this.Columns = GetTableColumns();
        }

        public InsertScript(string TableName, string Columns)
        {
            this.TableName = TableName;
            this.Columns = Columns;
            this.HasIdentity = false;
        }

        public void AddRow(InsertRow row)
        {
            InsertRows.Add(row);
        }
        public Boolean HasIdentity { get; set; }
        public string IdentityFieldname { get; set; }
        public string Columns { get; set; }
        public string TableName { get; set; }
        public string Delimiter { get; set; }
        public Boolean MultipleRow { get; set; }

        public int CodePage { get; set; }

        void AccessTest()
        {
            string col0 = this.tableInfo[0].ColumnName;
            tableInfo.RemoveAt(0);
            InsertRows[0].RemoveColumn(0);
            InsertRows[0].DataAtColumn(0);
        }

        public void RemoveColumnsWithAllNulls()
        {
            int rowCount  = InsertRows.Count;
            int colCount = tableInfo.Count;
            int column = 0;
            while (column < tableInfo.Count)
            {
                bool AllColumnsNull = true;
                for (int row = 0; row < rowCount; row++)
                {
                    if (InsertRows[row].DataAtColumn(column) != null)
                    {
                        AllColumnsNull = false;
                        break;
                    }
                }
                if (AllColumnsNull)
                {
                    tableInfo.RemoveAt(column);
                    for (int row = 0; row < rowCount; row++)
                    {
                        InsertRows[row].RemoveColumn(column);
                    }
                }
                else
                {
                    column++;
                }
            }
            Columns = GetTableColumns();
        }

        public string GetTableColumns()
        {
            StringBuilder sb = new StringBuilder();
            int colCount = 0;
            foreach (TableColumnInfo colInfo in tableInfo)
            {
                if (colCount++ > 0)
                {
                    sb.Append(", ");
                }
                sb.Append("[").Append(colInfo.ColumnName).Append("]");
            }
            return sb.ToString();
        }

        public void ReadFile(string Filename)
        {
            Encoding enc = Encoding.GetEncoding(CodePage);
            if (IsUTF8NoBOM(Filename))
            {
                enc = Encoding.UTF8;
            }
            string[] lines = File.ReadAllLines(Filename, enc);
            foreach (string line in lines)
            {
                InsertRow row = new InsertRow(tableInfo);
                row.ParseLine(line, Delimiter);
                AddRow(row);
            }
        }

        private bool IsUTF8NoBOM(string Filename)
        {
            bool result = false;
            int positive = 0;
            int negative = 0;

            Byte[] Bary = File.ReadAllBytes(Filename);

            int idx = 0;

            while (idx < Bary.Length)
            {
                byte B0, B1, B2, B3;
                B0 = Bary[idx++];
                if (idx < Bary.Length) B1 = Bary[idx]; else B1 = 0;
                if (idx + 1 < Bary.Length) B2 = Bary[idx + 1]; else B2 = 0;
                if (idx + 2 < Bary.Length) B3 = Bary[idx + 2]; else B3 = 0;
                if (is2Byte(B0))
                {
                    if (!isAdditionalByte(B1))
                        negative++;
                    else
                        positive++;
                }
                else if (is3Byte(B0))
                {
                    if (!isAdditionalByte(B1))
                        negative++;
                    else if (!isAdditionalByte(B2))
                        negative++;
                    else
                        positive++;
                }
                else if (is4Byte(B0))
                {
                    if (!isAdditionalByte(B1))
                        negative++;
                    else if (!isAdditionalByte(B2))
                        negative++;
                    else if (!isAdditionalByte(B3))
                        negative++;
                    else
                        positive++;
                }
            }
            if ((positive > 0) && (negative == 0))
                result = true;
            return result;
        }

        bool is2Byte(byte B)
        {
            return ((194 <= B) && (B <= 223));
        }
        bool is3Byte(byte B)
        {
            return ((224 <= B) && (B <= 239));
        }
        bool is4Byte(byte B)
        {
            return ((240 <= B) && (B <= 244));
        }
        bool isAdditionalByte(byte B)
        {
            return ((128 <= B) && (B <= 191));
        }

        public List<InsertRowInfo> GetInsertRows()
        {
            List<InsertRowInfo> insertRows = new List<InsertRowInfo>();

            StringBuilder sql = new StringBuilder();
            StringBuilder template = new StringBuilder();
            template.Append("INSERT INTO ").Append(TableName)
                .Append(" (").Append(Columns).Append(") VALUES");
            int values = 0;
            foreach (InsertRow row in InsertRows)
            {
                if (values == 0)
                {
                    sql.AppendLine(template.ToString());
                }
                else
                {
                    sql.AppendLine(",");
                }
                sql.Append(" (").Append(row.GetRow()).Append(")");
                if (++values == 1000)
                {
                    sql.AppendLine(";");
                    InsertRowInfo sqlrow = new InsertRowInfo(sql.ToString(), values);
                    insertRows.Add(sqlrow);
                    sql.Clear();
                    values = 0;
                }
            }
            if (values > 0)
            {
                sql.AppendLine(";");
                InsertRowInfo sqlrow = new InsertRowInfo(sql.ToString(), values);
                insertRows.Add(sqlrow);
                sql.Clear();
            }

            return insertRows;
        }

        public string TruncateTable { get; set; }

        public int RowCount()
        {
            return InsertRows.Count;
        }

        public string SQL()
        {
            StringBuilder sql = new StringBuilder();
            StringBuilder template = new StringBuilder();
            template.Append("INSERT INTO ").Append(TableName)
                .Append(" (").Append(Columns).Append(") VALUES");
            int values = 0;
            TruncateTable = string.Format("TRUNCATE TABLE {0};", TableName);
            if (HasIdentity) sql.Append("SET IDENTITY_INSERT ").Append(TableName).AppendLine(" ON").AppendLine("GO");
            foreach (InsertRow row in InsertRows)
            {
                if (MultipleRow)
                {
                    if (values == 0)
                    {
                        sql.AppendLine(template.ToString());
                    }
                    else
                    {
                        sql.AppendLine(",");
                    }
                    sql.Append(" (").Append(row.GetRow()).Append(")");
                }
                else
                    sql.Append(template.ToString()).Append(" (").Append(row.GetRow()).AppendLine(")");
                if (!MultipleRow)
                {
                    sql.AppendLine("GO");
                    values = 0;
                }
                else if (++values == 1000)
                {
                    if (MultipleRow) sql.AppendLine(";");
                    sql.AppendLine("GO");
                    values = 0;
                }
            }
            if (values > 0)
            {
                if (MultipleRow) sql.AppendLine(";");
                sql.AppendLine("GO");
            }
            if (HasIdentity)
            {
                sql.Append("SET IDENTITY_INSERT ").Append(TableName).AppendLine(" OFF").AppendLine("GO");
                sql.AppendLine("DECLARE @UID INT;");
                sql.Append("SELECT @UID = ISNULL(MAX(").Append(IdentityFieldname).Append("), 0) FROM ").Append(TableName).AppendLine(";");
                sql.Append("DBCC CHECKIDENT(").Append(TableName).AppendLine(", RESEED, @UID);");
                sql.AppendLine("GO");
            }
            return sql.ToString();
        }

    }
}
