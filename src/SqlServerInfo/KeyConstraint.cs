using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class KeyConstraint
    {
        public string ConstraintName { get; set; }
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsDescending { get; set; }
        public string ColumnName { get; set; }
        public string ObjectName { get; set; }

        public KeyConstraint()
        {
            ConstraintName = "";
            IsUnique = false;
            IsPrimaryKey = false;
            IsDescending = false;
            ColumnName = "";
            ObjectName = "";
        }
    }
}
