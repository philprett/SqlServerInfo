using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class Column
    {
        //USER_NAME(o.uid) AS SchemaName, o.name as ObjectName, " +
        //    "           col.name AS ColumnName, col.xtype as xtypeid, typ.name as xtype, col.length, " +
        //    "           col.isnullable, col2.is_identity as IsIdentity, " +
        //    "           col.xprec, col.xscale, ISNULL(dc.definition, '') as DefaultValue " +
        public string SchemaName { get; set; }
        public string ObjectName { get; set; }
        public string ColumnName { get; set; }
        public string ColumnType { get; set; }
        public int ColumnLength { get; set; }
        public int ColumnPrecision { get; set; }
        public int ColumnScale { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string DefaultDefinition { get; set; }
        public string DefaultConstraintName { get; set; }
        public string ComputedDefinition { get; set; }
        public bool IsComputedDefinition { get { return !string.IsNullOrWhiteSpace(ComputedDefinition); } }

        public Column()
        {
            SchemaName = "";
            ObjectName = "";
            ColumnName = "";
            ColumnType = "";
            ColumnLength = 0;
            ColumnPrecision = 0;
            ColumnScale = 0;
            IsNullable = true;
            IsIdentity = false;
            DefaultDefinition = "";
            DefaultConstraintName = "";
            ComputedDefinition = "";
        }

        public string QuoteValue(object value)
        {
            return value.ToString();
        }

        public string TypeDefinition
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                if (IsComputedDefinition)
                {
                    sb.Append(ComputedDefinition);
                }
                else
                {
                    sb.Append(ColumnType);
                    if (ColumnType == "sql_variant" || ColumnType == "varbinary" || ColumnType == "char" || ColumnType == "nchar" || ColumnType == "nvarchar" || ColumnType == "varchar")
                    {
                        sb.Append("(");
                        if (ColumnLength == -1)
                            sb.Append("MAX");
                        else if (ColumnType == "nchar" || ColumnType == "nvarchar")
                            sb.Append(ColumnLength / 2);
                        else
                            sb.Append(ColumnLength);
                        sb.Append(")");
                    }
                    else if (ColumnType == "decimal" || ColumnType == "numeric")
                    {
                        sb.Append("(");
                        sb.Append(ColumnPrecision);
                        sb.Append(",");
                        sb.Append(ColumnScale);
                        sb.Append(")");
                    }
                }
                return sb.ToString();
            }
        }
        public string DefinitionNoDefault
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("[");
                sb.Append(ColumnName);
                sb.Append("] ");
                if (IsComputedDefinition) sb.Append("AS ");
                sb.Append(TypeDefinition);

                if (!IsComputedDefinition)
                {
                    if (IsNullable)
                    {
                        sb.Append(" NULL");
                    }
                    else
                    {
                        sb.Append(" NOT NULL");
                    }
                }

                if (IsIdentity)
                {
                    sb.Append(" IDENTITY(1,1)");
                }

                return sb.ToString();
            }
        }

        public string Definition
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(DefinitionNoDefault);

                if (DefaultDefinition != "")
                {
                    sb.Append(" DEFAULT ");
                    sb.Append(DefaultDefinition);
                }

                return sb.ToString();
            }
        }

        public string AddSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] ADD {1}\r\n", ObjectName, Definition);
            }
        }

        public string AlterSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] ALTER COLUMN {1}\r\n", ObjectName, DefinitionNoDefault);
            }
        }

        public string DropSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] DROP COLUMN [{1}]\r\n", ObjectName, ColumnName);
            }
        }

        public string AddDefaultSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] ADD CONSTRAINT [{0}_{1}_DF] DEFAULT {2} FOR {1}\r\n", ObjectName, ColumnName, DefaultDefinition);
            }
        }

        public string DropDefaultSQL
        {
            get
            {
                return string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}]\r\n", ObjectName, DefaultConstraintName);
            }
        }
    }
}
