using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class Database
    {
        public SqlConnection Connection { get; set; }
        public string Name { get; set; }
        public int CompatibilityLevel { get; set; }
        public string Collation { get; set; }
        public string State { get; set; }
        public string RecoveryModel { get; set; }
        public string OwnerSid { get; set; }
        public bool IsSystemDatabase { get { return OwnerSid == "0x01"; } }
        public Database()
        {
            Connection = null;
            Name = "";
            CompatibilityLevel = 0;
            Collation = "";
            State = "";
            RecoveryModel = "";
            OwnerSid = "";
        }

        public static Database[] GetDatabases(SqlConnection connection, string nameMask = "%")
        {
            const string SQL_GET_DATABASES =
                "SELECT name, compatibility_level, collation_name, state_desc, recovery_model_desc, owner_sid " +
                "FROM sys.databases WHERE name LIKE '{0}'";

            DataTable dt = connection.SelectQuery(SQL_GET_DATABASES, nameMask);

            List<Database> databases = new List<Database>();
            foreach (DataRow dr in dt.Rows)
            {
                Database database = new Database
                {
                    Connection = connection,
                    Name = (string)dr["name"],
                    CompatibilityLevel = (int)(byte)dr["compatibility_level"],
                    Collation = (string)dr["collation_name"],
                    State = (string)dr["state_desc"],
                    RecoveryModel = (string)dr["recovery_model_desc"],
                    OwnerSid = Utils.ByteArrayToString((byte[])dr["owner_sid"]),
                };
                databases.Add(database);
            }

            return databases.ToArray();
        }

        public static Database GetDatabase(SqlConnection connection, string name)
        {
            Database[] databases = Database.GetDatabases(connection, name);
            if (databases.Length == 0)
            {
                throw new Exception(string.Format("Database {0} was not found", name));
            }
            return databases[0];
        }
    }
}
