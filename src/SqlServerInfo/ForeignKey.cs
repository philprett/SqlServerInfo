using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class ForeignKey
    {
        public string Name { get; set; }
        public string TableName { get; set; }
        public string FieldName { get; set; }
        public string ReferencedTableName { get; set; }
        public string ReferencesFieldName { get; set; }
        public bool DeleteReferential { get; set; }

        public ForeignKey()
        {
            Name = "";
            TableName = "";
            FieldName = "";
            ReferencedTableName = "";
            ReferencesFieldName = "";
            DeleteReferential = false;
        }

        public string CreateSQL
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat(
                    "ALTER TABLE [{0}] WITH NOCHECK ADD CONSTRAINT [{1}] FOREIGN KEY([{2}]) REFERENCES [{3}] ([{4}]) {5}\r\n",
                    TableName, Name, FieldName, ReferencedTableName, ReferencesFieldName, DeleteReferential ? "ON DELETE CASCADE" : "");
                sb.AppendFormat(
                    "ALTER TABLE [{0}] CHECK CONSTRAINT [{1}]\r\n",
                    TableName, Name);
                return sb.ToString();
            }
        }

        public string DropSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}]\r\n", TableName, Name);
            }
        }
    }
}
