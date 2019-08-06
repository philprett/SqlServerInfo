using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class DBObject
    {

        public SqlConnection Connection { get; set; }
        public string ServerName { get; set; }
        public string DatabaseName { get; set; }
        public string Name { get; set; }
        public string SchemaName { get; set; }
        private string originalCreateSQL;
        public string CreateSQL { get; set; }
        public string ObjectType { get; set; }
        public List<Column> Columns { get; set; }
        public List<KeyConstraint> KeyConstraintColumns { get; set; }
        public List<ForeignKey> ForeignKeys { get; set; }
        public List<Index> Indexes { get; set; }

        public List<Permission> Permissions { get; set; }

        public string FullName { get { return string.Format("[{0}].[{1}]", SchemaName, Name); } }
        public bool IsTableType { get { return ObjectType == "TT"; } }
        public bool IsTable { get { return ObjectType == "U"; } }
        public bool IsView { get { return ObjectType == "V"; } }
        public bool IsFunction { get { return ObjectType == "TF" || ObjectType == "FN" || ObjectType == "AF" || ObjectType == "IF"; } }
        public bool IsProcedure { get { return ObjectType == "P"; } }
        public bool IsConstraint { get { return ObjectType == "PK" || ObjectType == "C" || ObjectType == "F"; } }
        public bool IsForeignKey { get { return ObjectType == "FK"; } }

        public bool HasPrimaryKey { get { return Columns.FirstOrDefault(c => c.IsPrimaryKey) != null; } }

        public string ObjectTypeName
        {
            get
            {
                if (IsTableType) return "TableType";
                if (IsTable) return "Table";
                if (IsView) return "View";
                if (IsFunction) return "Function";
                if (IsProcedure) return "Stored Procedure";
                return "";
            }
        }

        public int ObjectTypeSort
        {
            get
            {
                if (IsTableType) return 1;
                if (IsTable) return 2;
                if (IsView) return 3;
                if (IsFunction) return 4;
                if (IsProcedure) return 5;
                return 9999;
            }
        }

        public string DropSQL
        {
            get
            {
                if (IsTableType)
                    return string.Format("DROP TYPE [{0}]\r\n", Name);
                if (IsTable)
                    return string.Format("DROP TABLE [{0}]\r\n", Name);
                if (IsView)
                    return string.Format("DROP VIEW [{0}]\r\n", Name);
                if (IsFunction)
                    return string.Format("DROP FUNCTION [{0}]\r\n", Name);
                if (IsConstraint)
                    return string.Format("DROP CONSTRAINT [{0}]\r\n", Name);
                if (IsProcedure)
                    return string.Format("DROP PROCEDURE [{0}]\r\n", Name);
                return "";
            }
        }
        public string DropIfExistsSQL
        {
            get
            {
                if (IsTableType)
                    return string.Format("IF EXISTS (SELECT TOP 1 * FROM sys.types WHERE name = '{0}')\r\n    {1}", Name, DropSQL);

                return string.Format("IF OBJECT_ID('{0}') IS NOT NULL\r\n    {1}", Name, DropSQL);
            }
        }
        public string AlterSQL
        {
            get
            {
                // Update the AlterSQL (replace CREATE with ALTER, making sure that it does not occur in comments)
                string noSpaces = Utils.ReplaceSqlCommentsWithSpaces(CreateSQL);
                int createPos = noSpaces.IndexOf("CREATE", StringComparison.CurrentCultureIgnoreCase);
                return createPos < 0 ? "" : CreateSQL.Substring(0, createPos) + "ALTER" + CreateSQL.Substring(createPos + 6);
            }
        }
        public DBObject()
        {
            originalCreateSQL = "";
            Connection = null;
            Name = "";
            SchemaName = "";
            CreateSQL = "";
            ObjectType = "";
            Columns = new List<Column>();
            KeyConstraintColumns = new List<KeyConstraint>();
            ForeignKeys = new List<ForeignKey>();
            Indexes = new List<Index>();
            Permissions = new List<Permission>();
        }

        public static DBObject[] GetDBObjects(SqlConnection connection, string databaseName, string objectNameMask = "%")
        {
            const string SQL_USE_DATABASE = "USE [{0}]";
            connection.ExecuteQuery(SQL_USE_DATABASE, databaseName);

            /////////////////////////////////////
            // Get the table types
            /////////////////////////////////////
            const string SQL_GET_TABLE_TYPES =
                "select    tt.SchemaName, tt.TableTypeName, tt.ColumnName, tt.ColumnType, tt.ColumnMaxLength, tt.ColumnPrecision, tt.ColumnScale " +
                "from      sys.types t " +
                "left join ( select     s.name as SchemaName, tt.system_type_id, tt.user_type_id, tt.name as TableTypeName, c.name as ColumnName, t.name as ColumnType, " +
                "                       c.max_length as ColumnMaxLength, c.precision as ColumnPrecision, c.scale as ColumnScale " +
                "            from       sys.table_types tt " +
                "            inner join sys.objects o on o.object_id = tt.type_table_object_id " +
                "            inner join sys.schemas s on s.schema_id = o.schema_id  " +
                "            inner join sys.columns c on c.object_id = o.object_id " +
                "            inner join sys.types t on t.system_type_id = c.system_type_id and t.user_type_id = c.user_type_id " +
                "          ) tt " +
                "          on tt.system_type_id = t.system_type_id and tt.user_type_id = t.user_type_id " +
                "where     t.is_user_defined = 1 ";

            DataTable dt = connection.SelectQuery(SQL_GET_TABLE_TYPES);
            DBObject previousObject = null;
            List<DBObject> objects = new List<DBObject>();
            foreach (DataRow dr in dt.Rows)
            {
                DBObject o = new DBObject
                {
                    Connection = connection,
                    SchemaName = (string)dr["SchemaName"],
                    Name = (string)dr["TableTypeName"],
                    CreateSQL = "",
                    ObjectType = "TT",
                };
                SqlConnectionStringBuilder cs = new SqlConnectionStringBuilder(connection.ConnectionString);
                o.ServerName = cs.DataSource;
                o.DatabaseName = databaseName;
                if (previousObject != null && previousObject.Name == o.Name)
                {
                    previousObject.CreateSQL += o.CreateSQL;
                }
                else
                {
                    objects.Add(o);
                    previousObject = o;
                }
                previousObject.AddColumn(new Column
                {
                    ColumnName = (string)dr["ColumnName"],
                    ColumnType = (string)dr["ColumnType"],
                    ColumnLength = (int)(short)dr["ColumnMaxLength"],
                    ColumnPrecision = (int)(byte)dr["ColumnPrecision"],
                    ColumnScale = (int)(byte)dr["ColumnScale"],
                });
            }

            /////////////////////////////////////
            // Get the base objects
            /////////////////////////////////////
            const string SQL_GET_DATABASE_OBJECTS =
                "SELECT USER_NAME(o.uid) AS uid, o.name as name, c.text as objsql, LTRIM(RTRIM(o.type)) as type " +
                "FROM sysobjects o " +
                "LEFT JOIN sys.objects o2 ON o2.object_id = o.id " +
                "LEFT JOIN syscomments c " +
                "ON c.id = o.id " +
                "WHERE o2.is_ms_shipped = 0 " +
                "      AND NOT (o.name = 'dtproperties' AND LTRIM(RTRIM(o.type)) = 'U') " +
                "      AND NOT (o.name = 'sysdiagrams' AND LTRIM(RTRIM(o.type)) = 'U') " +
                "      AND NOT (o.name LIKE 'fn_%' AND LTRIM(RTRIM(o.type)) = 'FN') " +
                "      AND NOT (o.name LIKE 'sp_%' AND LTRIM(RTRIM(o.type)) = 'P') " +
                "      AND o.name LIKE '{0}' " +
                "ORDER BY CASE WHEN LTRIM(RTRIM(o.type)) = 'TT' THEN 1 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'U' THEN 2 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'V' THEN 3 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'TF' THEN 4 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'FN' THEN 5 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'AN' THEN 6 " +
                "              WHEN LTRIM(RTRIM(o.type)) = 'IN' THEN 7 " +
                "              ELSE 9999 END ASC, o.name ASC, c.number ASC, c.colid ASC";
            dt = connection.SelectQuery(SQL_GET_DATABASE_OBJECTS, objectNameMask);
            previousObject = null;
            foreach (DataRow dr in dt.Rows)
            {
                DBObject o = new DBObject
                {
                    Connection = connection,
                    SchemaName = (string)dr["uid"],
                    Name = (string)dr["name"],
                    CreateSQL = dr["objsql"] == DBNull.Value ? "" : (string)dr["objsql"],
                    ObjectType = (string)dr["type"],
                };
                SqlConnectionStringBuilder cs = new SqlConnectionStringBuilder(connection.ConnectionString);
                o.ServerName = cs.DataSource;
                o.DatabaseName = databaseName;
                if (previousObject != null && previousObject.Name == o.Name)
                {
                    previousObject.CreateSQL += o.CreateSQL;
                }
                else
                {
                    objects.Add(o);
                    previousObject = o;
                }
            }

            /////////////////////////////////////
            // Now get the columns for the objects
            /////////////////////////////////////
            const string SQL_GET_TABLE_COLUMNS =
               "SELECT		USER_NAME(o.uid) AS SchemaName, o.name as ObjectName, " +
               "           col.name AS ColumnName, col.xtype as xtypeid, typ.name as xtype, col.length, " +
               "           col.isnullable, col2.is_identity as IsIdentity, " +
               "           col.xprec, col.xscale, ISNULL(dc.definition, '') as DefaultValue, dc.name AS DefaultConstraintName, " +
               "           cc.definition AS ComputedDefinition " +
               "FROM		syscolumns col " +
               "INNER JOIN sysobjects o ON o.id = col.id " +
               "INNER JOIN	sys.columns col2 ON col2.object_id = col.id AND col2.column_id = col.colid " +
               "INNER JOIN	systypes typ ON col.xtype = typ.xtype " +
               "LEFT JOIN  sys.default_constraints def ON def.object_id = 1 " +
               "LEFT JOIN  sys.default_constraints dc on dc.object_id = col2.default_object_id " +
               "LEFT JOIN  sys.computed_columns cc on cc.object_id = col2.object_id and cc.column_id = col2.column_id " +
               "WHERE		col.id = o.id AND typ.status != 1 AND o.name LIKE '{0}' " +
               "ORDER BY	USER_NAME(o.uid), o.name, col.colorder ASC";
            dt = connection.SelectQuery(SQL_GET_TABLE_COLUMNS, objectNameMask);
            DBObject obj = null;
            foreach (DataRow dr in dt.Rows)
            {
                Column c = new Column
                {
                    SchemaName = (string)dr["SchemaName"],
                    ObjectName = (string)dr["ObjectName"],
                    ColumnName = (string)dr["ColumnName"],
                    ColumnType = (string)dr["xtype"],
                    ColumnLength = (int)(short)dr["length"],
                    ColumnPrecision = (int)(byte)dr["xprec"],
                    ColumnScale = (int)(byte)dr["xscale"],
                    IsNullable = (int)dr["isnullable"] == 1,
                    IsIdentity = (bool)dr["IsIdentity"],
                    DefaultDefinition = (string)dr["DefaultValue"],
                    DefaultConstraintName = dr["DefaultConstraintName"] == DBNull.Value ? "" : (string)dr["DefaultConstraintName"],
                    ComputedDefinition = dr["ComputedDefinition"] == DBNull.Value ? "" : (string)dr["ComputedDefinition"],
                };

                // Cleanup extra brackets on the DefaultDefinition
                if (!string.IsNullOrWhiteSpace(c.DefaultDefinition))
                {
                    while (c.DefaultDefinition.StartsWith("(") && c.DefaultDefinition.EndsWith(")"))
                    {
                        c.DefaultDefinition = c.DefaultDefinition.Substring(1, c.DefaultDefinition.Length - 2);
                    }
                }

                // Cleanup extra brackets on the ComputedDefinition
                if (c.IsComputedDefinition)
                {
                    while (c.ComputedDefinition.StartsWith("(") && c.ComputedDefinition.EndsWith(")"))
                    {
                        c.ComputedDefinition = c.ComputedDefinition.Substring(1, c.ComputedDefinition.Length - 2);
                    }
                }

                if (obj == null || obj.Name != c.ObjectName)
                {
                    obj = objects.FirstOrDefault(o => o.Name == c.ObjectName);
                }
                if (obj != null)
                {
                    obj.AddColumn(c);
                }
            }

            /////////////////////////////////////////////
            // Now get the key constraints
            /////////////////////////////////////////////
            const string SQL_GET_KEY_CONSTRAINTS =
                "select     k.name as ConstraintName, i.is_unique as IsUnique, i.is_primary_key as IsPrimaryKey, ic.is_descending_key as IsDescending, c.name as ColumnName, o.name as tablename, s.name as schemaname  " +
                "from       sys.key_constraints k " +
                "inner join sys.objects o on o.object_id = k.parent_object_id " +
                "inner join sys.schemas s on o.schema_id = s.schema_id " +
                "inner join sys.indexes i on i.object_id = k.parent_object_id " +
                "inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id " +
                "inner join sys.columns c on c.object_id = i.object_id and c.column_id = ic.column_id " +
                "where      i.is_primary_key = 1 " +
                "order by   s.name, o.name, c.name";

            dt = connection.SelectQuery(SQL_GET_KEY_CONSTRAINTS);
            obj = null;
            foreach (DataRow dr in dt.Rows)
            {
                KeyConstraint kc = new KeyConstraint
                {
                    ConstraintName = (string)dr["ConstraintName"],
                    IsUnique = (bool)dr["IsUnique"],
                    IsPrimaryKey = (bool)dr["IsPrimaryKey"],
                    IsDescending = (bool)dr["IsDescending"],
                    ColumnName = (string)dr["ColumnName"],
                    ObjectName = (string)dr["tablename"],
                };
                if (obj == null || obj.Name != kc.ObjectName)
                {
                    obj = objects.FirstOrDefault(o => o.Name == kc.ObjectName);
                }
                if (obj != null)
                {
                    obj.AddKeyConstraintColumn(kc);
                }
            }

            /////////////////////////////////////////////
            // Now get the foreign keys
            /////////////////////////////////////////////
            const string SQL_GET_FOREIGNKEYS =
               "SELECT      o.name AS FKName, p.name AS TableName, c.name AS FieldName, ro.name AS ReferencesTableName, rc.name AS ReferencedFieldName, fk.delete_referential_action " +
               "FROM        sys.foreign_keys fk " +
               "INNER JOIN  sys.objects o                ON o.object_id = fk.object_id " +
               "INNER JOIN  sys.objects p                ON p.object_id = fk.parent_object_id " +
               "INNER JOIN  sys.foreign_key_columns fkc  ON fkc.constraint_object_id = fk.object_id and fkc.parent_object_id = fk.parent_object_id " +
               "INNER JOIN  sys.columns c                ON c.object_id = fk.parent_object_id AND c.column_id = fkc.parent_column_id " +
               "INNER JOIN  sys.objects ro               ON ro.object_id = fkc.referenced_object_id " +
               "INNER JOIN  sys.columns rc               ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id " +
               "WHERE       fk.type = 'F' " +
               "ORDER BY    p.name, c.name ";
            dt = connection.SelectQuery(SQL_GET_FOREIGNKEYS);
            obj = null;
            foreach (DataRow dr in dt.Rows)
            {
                ForeignKey fk = new ForeignKey
                {
                    Name = (string)dr["FKName"],
                    TableName = (string)dr["TableName"],
                    FieldName = (string)dr["FieldName"],
                    ReferencedTableName = (string)dr["ReferencesTableName"],
                    ReferencesFieldName = (string)dr["ReferencedFieldName"],
                    DeleteReferential = ((byte)dr["delete_referential_action"]) == 1,
                };
                if (obj == null || obj.Name != fk.Name)
                {
                    obj = objects.FirstOrDefault(o => o.Name == fk.TableName);
                }
                if (obj != null)
                {
                    obj.AddForeignKey(fk);
                }
            }

            /////////////////////////////////////////////
            // Now get the indexes
            /////////////////////////////////////////////
            const string SQL_GET_INDEXES =
               "select     distinct i.name as IndexName, c.name as ColumnName, o.name as tablename " +
               "from       sys.indexes i " +
               "inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id " +
               "inner join sys.objects o on o.object_id = i.object_id and o.is_ms_shipped = 0 " +
               "inner join sys.columns c on c.object_id = o.object_id and c.column_id = ic.column_id " +
               "where      i.is_primary_key = 0 " +
               "order by   o.name, i.name, c.name";
            dt = connection.SelectQuery(SQL_GET_INDEXES);
            obj = null;
            foreach (DataRow dr in dt.Rows)
            {
                Index i = new Index
                {
                    Name = (string)dr["IndexName"],
                    TableName = (string)dr["tablename"],
                };
                i.ColumnNames.Add((string)dr["ColumnName"]);
                if (obj == null || obj.Name != i.Name)
                {
                    obj = objects.FirstOrDefault(o => o.Name == i.TableName);
                }
                if (obj != null)
                {
                    obj.AddIndex(i);
                }
            }

            /////////////////////////////////////////////
            // Now get the permissions
            /////////////////////////////////////////////
            const string SQL_GET_PERMISSIONS =
               "select o.name, " +
               "       (p.state_desc COLLATE Latin1_General_CI_AS) + ' ' + " +
               "       (p.permission_name COLLATE Latin1_General_CI_AS) + ' ON ' + " +
               "       '['+(o.name COLLATE Latin1_General_CI_AS)+']' + ' TO ' + " +
               "       '['+(u.name COLLATE Latin1_General_CI_AS)+']'" +
               "from sys.database_permissions p " +
               "inner join sys.objects o ON o.object_id = p.major_id " +
               "inner join sys.sysusers u ON u.uid = p.grantee_principal_id " +
               "where p.class_desc = 'OBJECT_OR_COLUMN' AND p.grantee_principal_id != 0 " +
               "order by o.name, u.name, p.state_desc, p.permission_name";

            dt = connection.SelectQuery(SQL_GET_PERMISSIONS);
            obj = null;
            foreach (DataRow dr in dt.Rows)
            {
                Permission p = new Permission { ObjectName = (string)dr[0], PermissionSQL = (string)dr[1] };
                if (obj == null || obj.Name != p.ObjectName)
                {
                    obj = objects.FirstOrDefault(o => o.Name == p.ObjectName);
                }
                if (obj != null)
                {
                    obj.AddPermission(p);
                }
            }

            return objects.ToArray();

        }

        public override string ToString()
        {
            return ObjectTypeName.Substring(0, 1) + " " + Name;
        }

        public void AddColumn(Column column)
        {
            this.Columns.Add(column);
            RefreshTableSQL();
        }

        public void AddKeyConstraintColumn(KeyConstraint column)
        {
            this.KeyConstraintColumns.Add(column);
            foreach (Column c in Columns)
            {
                if (c.ColumnName == column.ColumnName)
                {
                    c.IsPrimaryKey = true;
                }
            }
            RefreshTableSQL();
        }

        public void AddForeignKey(ForeignKey foreignKey)
        {
            this.ForeignKeys.Add(foreignKey);
            RefreshTableSQL();
        }

        public void AddIndex(Index index)
        {
            Index idx = Indexes.FirstOrDefault(i => i.Name == index.Name);
            if (idx == null)
            {
                this.Indexes.Add(index);
            }
            else
            {
                idx.ColumnNames.Add(index.ColumnNames[0]);
            }
            RefreshTableSQL();
        }

        public void AddPermission(Permission permission)
        {
            this.Permissions.Add(permission);
            RefreshTableSQL();
        }

        private void RefreshTableSQL()
        {
            if (IsTable)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("CREATE TABLE [");
                sb.Append(Name);
                sb.Append("]\r\n(");
                bool firstColumn = true;
                foreach (Column column in Columns)
                {
                    if (firstColumn) firstColumn = false; else sb.Append(",");
                    sb.Append("\r\n    ");
                    sb.Append(column.Definition);
                }

                if (KeyConstraintColumns.Count > 0)
                {
                    bool havePrimaryKey = false;
                    foreach (var constraint in KeyConstraintColumns.Select(c => new { c.ConstraintName, c.IsPrimaryKey, c.IsUnique }).Distinct())
                    {
                        if (constraint.IsPrimaryKey && havePrimaryKey)
                        {

                        }
                        else
                        {
                            if (constraint.IsPrimaryKey) havePrimaryKey = true;
                            if (firstColumn) firstColumn = false; else sb.Append(",");
                            sb.Append("\r\n    CONSTRAINT [");
                            sb.Append(constraint.ConstraintName);
                            sb.Append("] ");
                            if (constraint.IsPrimaryKey)
                                sb.Append("PRIMARY KEY CLUSTERED\r\n    (");
                            else if (constraint.IsUnique)
                                sb.Append("UNIQUE\r\n    (");

                            bool first = true;
                            foreach (KeyConstraint kc in KeyConstraintColumns.Where(c => c.ConstraintName == constraint.ConstraintName))
                            {
                                if (first) first = false; else sb.Append(",");
                                sb.Append("        \r\n        [");
                                sb.Append(kc.ColumnName);
                                sb.Append("] ");
                                sb.Append(kc.IsDescending ? "DESC" : "ASC");
                            }
                            sb.Append("\r\n    )");
                        }
                    }
                }

                sb.Append("\r\n)\r\n");

                if (ForeignKeys.Count > 0)
                {
                    sb.Append("\r\nGO\r\n\r\n");
                    foreach (ForeignKey fk in ForeignKeys)
                    {
                        sb.Append(fk.CreateSQL);
                    }
                }

                if (Indexes.Count > 0)
                {
                    sb.Append("\r\nGO\r\n\r\n");
                    foreach (Index i in Indexes)
                    {
                        sb.Append(i.CreateSQL);
                    }
                }

                if (Permissions.Count > 0)
                {
                    sb.Append("\r\nGO\r\n\r\n");
                    foreach (Permission i in Permissions)
                    {
                        sb.Append(i.PermissionSQL + "\r\n");
                    }
                }

                CreateSQL = sb.ToString();
            }
            else if (IsTableType)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("CREATE TYPE [");
                sb.Append(Name);
                sb.Append("] AS TABLE\r\n(");
                bool firstColumn = true;
                foreach (Column column in Columns)
                {
                    if (firstColumn) firstColumn = false; else sb.Append(",");
                    sb.Append("\r\n    ");
                    sb.Append(column.Definition);
                }
                sb.Append("\r\n)\r\n");

                CreateSQL = sb.ToString();
            }
            else
            {
                if (originalCreateSQL == "")
                {
                    originalCreateSQL = CreateSQL;
                }
                StringBuilder sb = new StringBuilder(originalCreateSQL);
                if (Permissions.Count > 0)
                {
                    sb.Append("\r\nGO\r\n\r\n");
                    foreach (Permission i in Permissions)
                    {
                        sb.Append(i.PermissionSQL + "\r\n");
                    }
                }
                CreateSQL = sb.ToString();
            }
        }
    }
}
