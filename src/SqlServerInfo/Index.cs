using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class Index
    {
        //"select     distinct i.name as IndexName, c.name as ColumnName, o.name as tablename " +
        public string Name { get; set; }
        public string TableName { get; set; }
        public List<string> ColumnNames { get; set; }

        public Index()
        {
            Name = "";
            TableName = "";
            ColumnNames = new List<string>();
        }


        public string CreateSQL
        {
            get
            {
                StringBuilder columnNamesSB = new StringBuilder();
                foreach (string columnName in ColumnNames)
                {
                    if (columnNamesSB.Length > 0) columnNamesSB.Append(",");
                    columnNamesSB.AppendFormat("[{0}]", columnName);
                }
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("CREATE INDEX [{0}] ON [{1}] ( {2} )\r\n", Name, TableName, columnNamesSB.ToString());
                return sb.ToString();
            }
        }

        public string DropSQL
        {
            get
            {
                return string.Format("DROP INDEX [{0}] ON [{1}]\r\n", Name, TableName);
            }
        }

    }
}
