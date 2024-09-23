using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;

namespace DbFarm20
{
    public class DbFarm
    {
        private SqlConnection _conn = null;
        private SqlTransaction _tran = null;
        private bool _logging = false;
        private bool _connectionOn = false;
        private string _user = "";
        private string _datetimeFormat = "dd/MM/yyyy HH:mm:ss";
        private string _dateFormat = "dd/MM/yyyy";
        private string _parametriEsclusi = "#@pwd#";
        private string _spLog = "#RegistraSqlLog#";

        //METODI GET-SET
        public SqlConnection Connection() { return _conn; }
        public void SetConnection(SqlConnection conn) { _conn = conn; }
        public void SetConnection(string connStr) { _conn = new SqlConnection(connStr); }
        public void SetLogging(bool logging) { _logging = logging; }
        public void SetUser(string user) { _user = user; }
        public void SetDateTimeFormat(string format) { _datetimeFormat = format; }
        public void SetDateFormat(string format) { _dateFormat = format; }
        public void SetSpLog(string spLog) { _spLog = spLog; }
        public bool IsConnectionOn() { return _connectionOn; }
        public SqlTransaction Transaction() { return _tran; }
        public void SetTransaction(SqlTransaction tran) { _tran = tran; }
        public DbFarm(string connStr)
        {
            SetConnection(connStr);
            OpenConnection();
        }
        public DbFarm(SqlConnection conn)
        {
            SetConnection(conn);
        }

        //METODI DI GESTIONE CONNESSIONE
        public void OpenConnection()
        {
            if (_conn == null) throw new Exception("Connection not initialized");

            _conn.Open();
            _connectionOn = true;
        }
        /// <summary>
        /// Verifica se la connessione presenta una transazione attiva. In tal caso solleva un'eccezione.
        /// </summary>
        public void CloseConnection()
        {
            if (_tran != null) throw new Exception("Cannot close connection. Active transaction still exist.");

            checkConnection();
            _conn.Close();
            _connectionOn = false;
        }

        //METODI DI GESTIONE TRANSAZIONE
        public void BeginTransaction()
        {
            checkConnection();
            _tran = _conn.BeginTransaction();
        }
        public void CommitTransaction()
        {
            checkConnection();
            _tran.Commit();
            _tran = null;
        }
        public void RollbackTransaction()
        {
            checkConnection();
            _tran.Rollback();
            _tran = null;
        }

        public DataTable Read(string sql, Dictionary<string, object> inParams = null, bool closeConn = false)
        {
            checkConnection();
            DataTable dt;

            try
            {
                SqlCommand sqlCmd;
                using (sqlCmd = new SqlCommand(sql, _conn) { CommandTimeout = 0 })
                {
                    if (_tran != null) sqlCmd.Transaction = _tran;

                    setInputParams(inParams, sqlCmd, out var strIp);

                    var sqlDa = new SqlDataAdapter { SelectCommand = sqlCmd };
                    var ds = new DataSet();

                    var start = DateTime.Now;

                    sqlDa.Fill(ds);

                    var end = DateTime.Now;

                    Logging(sql, strIp, start, end);
                    dt = ds.Tables[0];
                }

                if (closeConn) CloseConnection();

                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.EseguiIstruzioneR(string sql, Dictionary<string, object> inParams = null): ", ex);
            }
        }

        public bool Insert(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud(sql, inParams, outParams, out results, out cudItems, closeConn);
        }
        public bool Update(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud(sql, inParams, outParams, out results, out cudItems, closeConn);
        }
        public bool Delete(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud(sql, inParams, outParams, out results, out cudItems, closeConn);
        }
        private bool Cud(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            results = null;
            cudItems = 0;
            checkConnection();

            try
            {
                var sqlCmd = new SqlCommand(sql, _conn) { CommandTimeout = 0 };

                if (_tran != null) sqlCmd.Transaction = _tran;

                setInputParams(inParams, sqlCmd, out var strIp);
                setOutputParams(sqlCmd, outParams);

                var start = DateTime.Now;

                cudItems = sqlCmd.ExecuteNonQuery();

                var end = DateTime.Now;

                Logging(sql, strIp, start, end);

                results = getOutputParams(sqlCmd, outParams);

                if (closeConn) CloseConnection();

                return true;
            }
            catch (Exception ex)
            {
                results = null;
                throw new Exception("Eccezione rilevata in DbFarm.EseguiIstruzioneCUD(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results): ", ex);
            }
        }

        public DataTable Read_Sp(string spName, Dictionary<string, object> inParams, bool closeConn = false)
        {
            return Read_Sp(spName, inParams, null, out _, closeConn);
        }

        public DataTable Read_Sp(string spName, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, bool closeConn = false)
        {
            results = null;
            checkConnection();
            DataTable dt;

            try
            {
                SqlCommand sqlCmd;
                using (sqlCmd = new SqlCommand(spName, _conn))
                {
                    sqlCmd.CommandType = CommandType.StoredProcedure;
                    sqlCmd.CommandTimeout = 0;

                    if (_tran != null) sqlCmd.Transaction = _tran;

                    setInputParams(inParams, sqlCmd, out var strIp);
                    setOutputParams(sqlCmd, outParams);

                    dt = new DataTable();

                    var start = DateTime.Now;
                    dt.Load(sqlCmd.ExecuteReader());
                    var end = DateTime.Now;

                    Logging(spName, strIp, start, end);
                    results = getOutputParams(sqlCmd, outParams);

                    if (closeConn) CloseConnection();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.EseguiIstruzioneR_Sp(string spName, SqlConnection conn, Dictionary<string, object> inParams): ", ex);
            }

            return dt;
        }

        public bool Insert_Sp(string spName, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud_Sp(spName, inParams, outParams, out results, out cudItems, closeConn);
        }
        public bool Update_Sp(string spName, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud_Sp(spName, inParams, outParams, out results, out cudItems, closeConn);
        }
        public bool Delete_Sp(string spName, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            return Cud_Sp(spName, inParams, outParams, out results, out cudItems, closeConn);
        }
        public bool Cud_Sp(string spName, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out int cudItems, bool closeConn = false)
        {
            results = null;
            cudItems = 0;
            checkConnection();

            try
            {
                SqlCommand sqlCmd;
                using (sqlCmd = new SqlCommand(spName, _conn))
                {
                    sqlCmd.CommandType = CommandType.StoredProcedure;
                    sqlCmd.CommandTimeout = 0;

                    if (_tran != null) sqlCmd.Transaction = _tran;

                    setInputParams(inParams, sqlCmd, out var strIp);
                    setOutputParams(sqlCmd, outParams);

                    var start = DateTime.Now;

                    cudItems = sqlCmd.ExecuteNonQuery();

                    var end = DateTime.Now;

                    Logging(spName, strIp, start, end);

                    results = getOutputParams(sqlCmd, outParams);

                    if (closeConn) CloseConnection();

                    return true;
                }
            }
            catch (Exception ex)
            {
                results = null;
                throw new Exception("Eccezione rilevata in DbFarm.EseguiIstruzioneCUD_Sp(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results): ", ex);
            }
        }

        public bool Scalare(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out object retscal, bool closeConn = false)
        {
            retscal = null;
            results = null;
            checkConnection();

            try
            {
                SqlCommand sqlCmd;
                using (sqlCmd = new SqlCommand(sql, _conn) { CommandTimeout = 0 })
                {
                    if (_tran != null) sqlCmd.Transaction = _tran;
                    setInputParams(inParams, sqlCmd, out var strIp);
                    setOutputParams(sqlCmd, outParams);

                    var start = DateTime.Now;

                    retscal = sqlCmd.ExecuteScalar();

                    var end = DateTime.Now;

                    Logging(sql, strIp, start, end);

                    results = getOutputParams(sqlCmd, outParams);
                }

                if (closeConn) CloseConnection();

                return true;
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.EseguiIstruzioneScalare(string sql, Dictionary<string, object> inParams, Dictionary<string, object> outParams, out Dictionary<string, object> results, out object retscal): ", ex);
            }
        }

        //METODI PRIVATI
        private void InsertSqlLog(string user, string sqlStr, string parametri, string startTime, string endTime, string delta)
        {
            try
            {
                var inParams = new Dictionary<string, object>
                {
                    {"@sqlStr", sqlStr}, {"@user", user}, {"@parametri", parametri}, {"@startTime", startTime}, {"@endTime", endTime}, {"@delta", delta}
                };

                Read_Sp(_spLog, inParams);
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.RegistraSqlLog(string user, string sqlStr, string parametri, string startTime, string endTime, string delta): ", ex);
            }
        }

        private void checkConnection()
        {
            if (_conn == null) throw new Exception("Connection not initialized");
            if (_conn.State != ConnectionState.Open) throw new Exception("Connection not already open");
        }

        private void Logging(string sql, string parametri, DateTime start, DateTime end)
        {
            if (_logging) InsertSqlLog(_user, sql, "", start.ToString(_datetimeFormat), end.ToString(_datetimeFormat), (end - start).TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
        }

        private void setInputParams(Dictionary<string, object> inParams, SqlCommand sqlCmd, out string strIp)
        {
            strIp = "";

            try
            {
                if (inParams != null)
                {
                    foreach (string k in inParams.Keys)
                    {
                        sqlCmd.Parameters.AddWithValue(k, inParams[k]);
                        if (_parametriEsclusi.Contains($"#{k}#")) strIp.Concat($"{k}:XXX;");
                        else strIp.Concat($"{k}:{inParams[k]};");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.MngListaParametriInput(Dictionary<string, object> inParams, SqlCommand sqlCmd, out string strIp): ", ex);
            }
        }

        private void setOutputParams(SqlCommand sqlCmd, Dictionary<string, object> outParams)
        {
            try
            {
                if (outParams == null) return;

                foreach (string k in outParams.Keys)
                {
                    var spParam = (SpParam)outParams[k];
                    var p = new SqlParameter
                    {
                        SqlDbType = spParam.Tipo,
                        Size = spParam.Size,
                        ParameterName = k,
                        Direction = ParameterDirection.Output
                    };

                    sqlCmd.Parameters.Add(p);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.MngListaParametriOutput(SqlCommand sqlCmd, Dictionary<string, object> outParams): ", ex);
            }
        }

        private Dictionary<string, object> getOutputParams(SqlCommand sqlCmd, Dictionary<string, object> outParams)
        {
            if (outParams == null) return null;

            var retVals = new Dictionary<string, object>();

            try
            {
                foreach (string k in outParams.Keys)
                {
                    retVals.Add(k, sqlCmd.Parameters[k].Value);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Eccezione rilevata in DbFarm.ReadParametriOutput(SqlCommand sqlCmd, Dictionary<string, object> outParams): ", ex);
            }

            return retVals;
        }
    }
    public sealed class SpParam : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // Free other state (managed objects).
            }
            // Free your own state (unmanaged objects).
            // Set large fields to null.
            _disposed = true;
        }

        ~SpParam()
        {
            // Simply call Dispose(false).
            Dispose(false);
        }
        public SpParam()
        {
            Tipo = SqlDbType.Char;
            Size = 0;
        }

        public SqlDbType Tipo { get; set; }
        public int Size { get; set; }
    }
}
