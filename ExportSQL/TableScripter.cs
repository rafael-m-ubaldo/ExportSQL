using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;


namespace ExportSQL
{
    class TableScripter
    {
        public string Server { get; set; }
        public string DataBase { get; set; }
        public string TableName { get; set; }
        public string UserName { get; set; }
        public string PassWord { get; set; }
        public TableScripter(string server, string dataBase, string userName, string passWord)
        {
            Server = server;
            DataBase = dataBase;
            UserName = userName;
            PassWord = passWord;
        }
        public string ScriptTable(string tableName)
        {
            StringBuilder SB = new StringBuilder();
            ServerConnection conn = new ServerConnection(Server, UserName, PassWord);
            Server srv = new Server(conn);
            Database db = srv.Databases[DataBase];

            TableName = tableName;

            Scripter scpt = new Scripter(srv);
            scpt.Options.BatchSize = 1;
            scpt.Options.IncludeHeaders = true;
            scpt.Options.ScriptDrops = false;
            scpt.Options.DriPrimaryKey = true;
            scpt.Options.WithDependencies = false;
            scpt.Options.NoCollation = true;
            scpt.Options.Indexes = true;
            scpt.Options.DriAllConstraints = true;

            Table tb = new Table(db, TableName);

            StringCollection sc = scpt.Script(new Urn[] { tb.Urn });
            foreach (string so in sc)
            {
                SB.AppendLine(so.Replace("\t", "  "));
            }

            return SB.ToString();
        }
    }
}
