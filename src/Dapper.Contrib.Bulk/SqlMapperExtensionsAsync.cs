using Dapper;
using Dapper.Contrib.Extensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static Dapper.Contrib.Extensions.SqlMapperExtensions;

namespace Dapper.Contrib.Bulk.Extensions
{
    public static partial class SqlMapperExtensions
    {
        /// <summary>
        /// Add bulk insert to connection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        public static async Task BulkInsertAsync<T>(this IDbConnection connection, IEnumerable<T> entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var (tableName, columnList, parameterList, param) = GenerateBulkInsertParam(connection, entityToInsert);
            var cmd = $"insert into {tableName} ({columnList}) values {parameterList}";
            await connection.ExecuteAsync(cmd, param, transaction, commandTimeout);
        }

        /// <summary>
        /// Bulk update async
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entities"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>

        public static async Task BulkUpdateAsync<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (entities is IProxy proxy && !proxy.IsDirty)
            {
                return;
            }

            var (tableName, columnList, parameterList, param, listUpdateColumn, keys) = GetUpdateParam(connection, entities);
            var adapter = GetFormatter(connection);
            await adapter.BulkUpdateAsync<T>(connection, transaction, commandTimeout, tableName, columnList, listUpdateColumn, parameterList, keys, param, entities);
        }


        /// <summary>
        /// Add bulk delete to connection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        public static async Task BulkDeleteAsync<T>(this IDbConnection connection, List<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var (tableName, columnList, parameterList, param) = GenerateBulkDeleteParam(connection, entities);
            var query = $"Delete from {tableName} where {parameterList}";
            await connection.ExecuteAsync(query, param, transaction, commandTimeout);
        }
    }
}

/// <summary>
/// The interface for all Dapper.Contrib database operations
/// Implementing this is each provider's model.
/// </summary>
public partial interface ISqlBulkAdapter
{
    Task BulkUpdateAsync<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, List<string> updateColumns, string parameterList, List<string> keys, DynamicParameters param, IEnumerable<T> entities = null);

}

/// <summary>
/// The SQL Server database adapter.
/// </summary>
public partial class SqlServerBulkAdapter : ISqlBulkAdapter
{

    public async Task BulkUpdateAsync<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, List<string> updateColumns, string parameterList, List<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
        var tempTable = $"#tmp_{Guid.NewGuid().ToString("N")}";
        var query = $"SELECT {columnList} INTO {tempTable}  FROM {tableName} WHERE 1<>1 ;";
        await connection.ExecuteAsync(query);
        await connection.ExecuteAsync($"SET IDENTITY_INSERT {tempTable} ON;");
        var cmd = $"insert into {tempTable} ({columnList}) values {parameterList}";
        await connection.ExecuteAsync(cmd, param, transaction, commandTimeout);


        var equalKeyvalue = new StringBuilder();

        string updateCommand = GetUpdateCommand(tableName, updateColumns, keys, tempTable, equalKeyvalue);

        await connection.ExecuteAsync(updateCommand, null, transaction, commandTimeout);
    }
}


/// <summary>
/// The MySQL database adapter.
/// </summary>
public partial class MySqlBulkAdapter : ISqlBulkAdapter
{

    public async Task BulkUpdateAsync<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, List<string> updateColumns, string parameterList, List<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {

        var tempTable = $"{tableName}_{Guid.NewGuid().ToString("N")}";
        await connection.ExecuteAsync($"CREATE TEMPORARY TABLE IF NOT EXISTS `{tempTable}` AS SELECT {columnList} FROM `{tableName}` LIMIT 0;");
        var cmd = $"insert into `{tempTable}` ({columnList}) values {parameterList}";
        await connection.ExecuteAsync(cmd, param, transaction, commandTimeout);

        string updateCommand = GetUpdateCommand(tableName, updateColumns, keys, tempTable);

        await connection.ExecuteAsync(updateCommand, null, transaction, commandTimeout);
    }
}

/// <summary>
/// The Postgres database adapter.
/// </summary>
public partial class PostgresBulkAdapter : ISqlBulkAdapter
{
    public async Task BulkUpdateAsync<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, List<string> updateColumns, string parameterList, List<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
        var tempTable = $"\"{tableName}_{Guid.NewGuid().ToString("N")}\"";
        tableName = $"{tableName}";
        await connection.ExecuteAsync($"CREATE TEMPORARY TABLE IF NOT EXISTS {tempTable} AS SELECT {columnList} FROM {tableName} LIMIT 0;");
        var cmd = $"insert into {tempTable} ({columnList}) values {parameterList}";
        await connection.ExecuteAsync(cmd, param, transaction, commandTimeout);

        string updateCommand = GetUpdateCommand(tableName, updateColumns, keys, tempTable);

        await connection.ExecuteAsync(updateCommand, null, transaction, commandTimeout);
    }
}

/// <summary>
/// The SQLite database adapter.
/// </summary>
public partial class SQLiteBulkAdapter : ISqlBulkAdapter
{
    public async Task BulkUpdateAsync<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, List<string> updateColumns, string parameterList, List<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
        await connection.UpdateAsync(entities);
    }
}


