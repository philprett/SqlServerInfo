using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    static class SQLConnectionExtension
    {
        public static DataTable SelectQuery(this SqlConnection connection, string sql, params object[] sqlArguments)
        {
            if (connection == null || connection.State != ConnectionState.Open)
            {
                throw new Exception("Connection to SQLServer has not been opened yet");
            }

            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter(string.Format(sql, sqlArguments), connection);
            da.Fill(dt);

            return dt;
        }

        public static int ExecuteQuery(this SqlConnection connection, string sql, params object[] sqlArguments)
        {
            if (connection == null || connection.State != ConnectionState.Open)
            {
                throw new Exception("Connection to SQLServer has not been opened yet");
            }

            SqlCommand command = connection.CreateCommand();
            command.CommandText = string.Format(sql, sqlArguments);
            return command.ExecuteNonQuery();
        }

        public static void UseDatabase(this SqlConnection connection, string name)
        {
            connection.ExecuteQuery("USE [{0}]", name);
        }

    }
}
