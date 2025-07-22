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
        /// The function to get a database type from the given <see cref="IDbConnection"/>.
        /// </summary>
        /// <param name="connection">The connection to get a database type name from.</param>
        public delegate string GetDatabaseTypeDelegate(IDbConnection connection);
        /// <summary>
        /// The function to get a table name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to get a table name for.</param>
        public delegate string TableNameMapperDelegate(Type type);

        private static readonly ConcurrentDictionary<int, string> TypeColumnName =
            new ConcurrentDictionary<int, string>();

        /// <summary>
        /// The function to get a a column name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="propertyInfo">The <see cref="PropertyInfo"/> to get a column name for.</param>
        public delegate string ColumnNameMapperDelegate(PropertyInfo propertyInfo);


        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ExplicitKeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> GetQueries = new ConcurrentDictionary<RuntimeTypeHandle, string>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ISqlBulkAdapter DefaultAdapter = new SqlServerBulkAdapter();
        private static readonly Dictionary<string, ISqlBulkAdapter> AdapterDictionary
            = new Dictionary<string, ISqlBulkAdapter>(6)
            {
                ["sqlconnection"] = new SqlServerBulkAdapter(),
                //["sqlceconnection"] = new SqlCeServerAdapter(),
                ["npgsqlconnection"] = new PostgresBulkAdapter(),
                ["sqliteconnection"] = new SQLiteBulkAdapter(),
                ["mysqlconnection"] = new MySqlBulkAdapter(),
                // ["fbconnection"] = new FbBulkAdapter()
            };

        /// <summary>
        /// Add bulk insert to connection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        public static void BulkInsert<T>(this IDbConnection connection, IEnumerable<T> entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (!entityToInsert.Any())
            {
                return;
            }
            var (tableName, columnList, parameterList, param) = GenerateBulkInsertParam(connection, entityToInsert);
            var cmd = $"insert into {tableName} ({columnList}) values {parameterList}";
            connection.Execute(cmd, param, transaction, commandTimeout);
        }


        /// <summary>
        /// Update list of entities
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entities"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <exception cref="ArgumentException"></exception>
        public static void BulkUpdate<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {

            if (!entities.Any())
            {
                return;
            }

            if (entities is IProxy proxy && !proxy.IsDirty)
            {
                return;
            }

        
            var (tableName, columnList, parameterList, param, listUpdateColumn, keys) = GetUpdateParam(connection, entities);
            var adapter = GetFormatter(connection);
            adapter.BulkUpdate<T>(connection, transaction, commandTimeout, tableName, columnList, listUpdateColumn, parameterList, keys, param, entities);
        }

      


        /// <summary>
        /// Add bulk delete to connection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        public static void BulkDelete<T>(this IDbConnection connection, List<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (!entities.Any())
            {
                return;
            }

            var (tableName, columnList, parameterList, param) = GenerateBulkDeleteParam(connection, entities);
            var query = $"Delete from {tableName} where {parameterList}";
            connection.Execute(query, param, transaction, commandTimeout);

        }



        private static (string, string, string, DynamicParameters, List<string>, List<string>) GetUpdateParam<T>(IDbConnection connection, IEnumerable<T> entities) where T : class
        {
            var adapter = GetFormatter(connection);
            var type = typeof(T);
            var (tableName, columnList, parameterList, param) = GenerateBulkInsertParam(connection, entities, true);

            var keyProperties = KeyPropertiesCache(type).ToList();  //added ToList() due to issue #418, must work on a list copy
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);
            if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var name = GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", name);

            var allProperties = TypePropertiesCache(type);
            keyProperties.AddRange(explicitKeyProperties);
            var computedProperties = ComputedPropertiesCache(type);
            var nonIdProps = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            var listUpdateColumn = new List<string>();
            for (var i = 0; i < nonIdProps.Count; i++)
            {
                var property = nonIdProps[i];
                listUpdateColumn.Add(GetColumnName(property));
            }

            var allKeyProperties = keyProperties.Concat(explicitKeyProperties).ToList();
            var keys = new List<string>();
            for (var i = 0; i < allKeyProperties.Count; i++)
            {
                var property = allKeyProperties[i];
                keys.Add(GetColumnName(property));
            }

            return (tableName, columnList, parameterList, param, listUpdateColumn, keys);

        }


        /// <summary>
        /// Generate bulk Insert command
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entities"></param>
        /// <returns></returns>
        private static (string, string, string, DynamicParameters) GenerateBulkDeleteParam<T>(IDbConnection connection, IEnumerable<T> entities, bool isContainsKey = false) where T : class
        {
            var type = typeof(T);


            var tableName = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);


            var adapter = GetFormatter(connection);

            var sbParameterList = new StringBuilder(null);
            DynamicParameters parameters = new DynamicParameters();

            for (int j = 0, length = Enumerable.Count(entities); j < length; j++)
            {
                sbParameterList.Append("(");
                var item = Enumerable.ElementAt(entities, j);
                {
                    for (var i = 0; i < keyProperties.Count; i++)
                    {
                        var property = keyProperties[i];
                        var columnName = $"{GetColumnName(property)}";
                        var columnNameParam = $"{columnName}{j}";
                        adapter.AppendColumnNameEqualsValueMulty(sbParameterList, columnName, columnNameParam);

                        var val = property.GetValue(item);
                        if (property.PropertyType.IsValueType)
                        {
                            if (property.PropertyType == typeof(DateTime))
                            {
                                var datetimevalue = Convert.ToDateTime(val);
                                if (property.GetCustomAttribute<DateAttribute>() != null)
                                {
                                    parameters.Add(columnNameParam, datetimevalue.Date, DbType.Date);
                                }
                                else
                                {
                                    parameters.Add(columnNameParam, datetimevalue, DbType.DateTime);
                                }
                            }
                            else if (property.PropertyType == typeof(bool))
                            {
                                var boolvalue = Convert.ToBoolean(val);
                                {
                                    parameters.Add(columnNameParam, boolvalue ? 1 : 0, DbType.Int32);
                                }
                            }
                            else
                            {
                                parameters.Add(columnNameParam, val);
                            }
                        }
                        else
                        {
                            parameters.Add(columnNameParam, val);
                        }

                        if (i < keyProperties.Count - 1)
                            sbParameterList.Append(" and ");
                    }
                }

                if (j < length - 1)
                {
                    sbParameterList.Append(") OR ");
                }


            }

            sbParameterList.Append(");");
            return (tableName, sbColumnList.ToString(), sbParameterList.ToString(), parameters);
        }




        /// <summary>
        /// Generate bulk Insert command
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entities"></param>
        /// <returns></returns>
        private static (string, string, string, DynamicParameters) GenerateBulkInsertParam<T>(IDbConnection connection, IEnumerable<T> entities, bool isContainsKey = false) where T : class
        {
            var type = typeof(T);


            var tableName = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            List<PropertyInfo> insertProperties = null;

            if (isContainsKey)
            {
                insertProperties = allProperties.Except(computedProperties).ToList();
            }
            else
            {
                insertProperties = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            }

            var adapter = GetFormatter(connection);

            for (var i = 0; i < insertProperties.Count; i++)
            {
                var property = insertProperties[i];
                adapter.AppendColumnName(sbColumnList, GetColumnName(property));
                if (i < insertProperties.Count - 1)
                    sbColumnList.Append(", ");
            }

            var sbParameterList = new StringBuilder(null);
            DynamicParameters parameters = new DynamicParameters();

            for (int j = 0, length = Enumerable.Count(entities); j < length; j++)
            {
                var item = Enumerable.ElementAt(entities, j);
                {
                    sbParameterList.Append("(");

                    for (var i = 0; i < insertProperties.Count; i++)
                    {
                        var property = insertProperties[i];

                        var columnName = $"@{GetColumnName(property)}{j}";
                        sbParameterList.Append(columnName);
                        var val = property.GetValue(item);
                        if (property.PropertyType.IsValueType)
                        {
                            if (property.PropertyType == typeof(DateTime))
                            {
                                var datetimevalue = Convert.ToDateTime(val);
                                if (property.GetCustomAttribute<DateAttribute>() != null)
                                {
                                    parameters.Add(columnName, datetimevalue.Date, DbType.Date);
                                }
                                else
                                {
                                    parameters.Add(columnName, datetimevalue, DbType.DateTime);
                                }
                            }
                            else if (property.PropertyType == typeof(bool))
                            {
                                var boolvalue = Convert.ToBoolean(val);
                                {
                                    parameters.Add(columnName, boolvalue ? 1 : 0, DbType.Int32);
                                }
                            }
                            else
                            {
                                parameters.Add(columnName, val);
                            }
                        }
                        else
                        {
                            parameters.Add(columnName, val);
                        }
                        if (i < insertProperties.Count - 1)
                            sbParameterList.Append(", ");
                    }

                    sbParameterList.Append("),");
                }

            }

            sbParameterList.Remove(sbParameterList.Length - 1, 1);
            sbParameterList.Append(";");
            return (tableName, sbColumnList.ToString(), sbParameterList.ToString(), parameters);
        }

        /// <summary>
        /// Specifies a custom callback that detects the database type instead of relying on the default strategy (the name of the connection type object).
        /// Please note that this callback is global and will be used by all the calls that require a database specific adapter.
        /// </summary>
#pragma warning disable CA2211 // Non-constant fields should not be visible - I agree with you, but we can't do that until we break the API
        public static GetDatabaseTypeDelegate GetDatabaseType;
#pragma warning restore CA2211 // Non-constant fields should not be visible

        private static ISqlBulkAdapter GetFormatter(IDbConnection connection)
        {
            var name = GetDatabaseType?.Invoke(connection).ToLower()
                       ?? connection.GetType().Name.ToLower();

            return AdapterDictionary.TryGetValue(name, out var adapter)
                ? adapter
                : DefaultAdapter;
        }


        /// <summary>
        /// Specify a custom table name mapper based on the POCO type name
        /// </summary>
#pragma warning disable CA2211 // Non-constant fields should not be visible - I agree with you, but we can't do that until we break the API
        public static TableNameMapperDelegate TableNameMapper;
#pragma warning restore CA2211 // Non-constant fields should not be visible

        private static string GetTableName(Type type)
        {
            if (TypeTableName.TryGetValue(type.TypeHandle, out string name)) return name;

            if (TableNameMapper != null)
            {
                name = TableNameMapper(type);
            }
            else
            {
                //NOTE: This as dynamic trick falls back to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableAttrName =
                    type.GetCustomAttribute<TableAttribute>(false)?.Name
                    ?? (type.GetCustomAttributes(false).FirstOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic)?.Name;

                if (tableAttrName != null)
                {
                    name = tableAttrName;
                }
                else
                {
                    name = type.Name + "s";
                    if (type.IsInterface && name.StartsWith("I"))
                        name = name.Substring(1);
                }
            }

            TypeTableName[type.TypeHandle] = name;
            return name;
        }

        /// <summary>
        /// Specify a custom Column name mapper based on the POCO type name
        /// </summary>
        public static ColumnNameMapperDelegate ColumnNameMapper;
        private static string GetColumnName(PropertyInfo propertyInfo)
        {
            if (TypeColumnName.TryGetValue(propertyInfo.GetHashCode(), out string name)) return name;
            if (ColumnNameMapper != null)
            {
                name = ColumnNameMapper(propertyInfo);
            }
            else
            {
                var info = propertyInfo;
                //NOTE: This as dynamic trick falls back to handle both our own Column-attribute as well as the one in EntityFramework 
                var columnAttrName =
                    info.GetCustomAttribute<ColumnNameAttribute>(false)?.Name
                    ?? (info.GetCustomAttributes(false)
                        .FirstOrDefault(attr => attr.GetType().Name == "ColumnNameAttribute") as dynamic)?.Name;

                if (columnAttrName != null)
                {
                    name = columnAttrName;
                }
                else
                {
                    name = info.Name;
                }
            }

            TypeColumnName[propertyInfo.GetHashCode()] = name;
            return name;
        }

        private static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            if (TypeProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pis))
            {
                return pis.ToList();
            }

            var properties = type.GetProperties().Where(IsWriteable).ToArray();
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }

        private static bool IsWriteable(PropertyInfo pi)
        {
            var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false).AsList();
            if (attributes.Count != 1) return true;

            var writeAttribute = (WriteAttribute)attributes[0];
            return writeAttribute.Write;
        }

        private static List<PropertyInfo> KeyPropertiesCache(Type type)
        {
            if (KeyProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties = allProperties.Where(p => p.GetCustomAttributes(true).Any(a => a is KeyAttribute)).ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.Find(p => string.Equals(p.Name, "id", StringComparison.CurrentCultureIgnoreCase));
                if (idProp != null && !idProp.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute))
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }

        private static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            if (ComputedProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ComputedAttribute)).ToList();

            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }
        private static List<PropertyInfo> ExplicitKeyPropertiesCache(Type type)
        {
            if (ExplicitKeyProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var explicitKeyProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute)).ToList();

            ExplicitKeyProperties[type.TypeHandle] = explicitKeyProperties;
            return explicitKeyProperties;
        }




    }

    /// <summary>
    /// Specifies that this is a date column.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class DateAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnNameAttribute : Attribute
    {
        /// <summary>
        /// Creates a ColumnName mapping to a specific name for Dapper.Contrib commands
        /// </summary>
        /// <param name="ColumnName">The name of this Column in the database.</param>
        public ColumnNameAttribute(string ColumnName)
        {
            Name = ColumnName;
        }

        /// <summary>
        /// The name of the Column Name in the database
        /// </summary>
        public string Name { get; set; }
    }


}

/// <summary>
/// The interface for all Dapper.Contrib database operations
/// Implementing this is each provider's model.
/// </summary>
public partial interface ISqlBulkAdapter
{


    void BulkUpdate<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, IEnumerable<string> updateColumns, string parameterList, IEnumerable<string> keys, DynamicParameters param,IEnumerable<T> entities=null);


    /// <summary>
    /// Adds the name of a column.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    void AppendColumnName(StringBuilder sb, string columnName);
    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    void AppendColumnNameEqualsValue(StringBuilder sb, string columnName);

    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    void AppendColumnNameEqualsValueMulty(StringBuilder sb, string columnName, string columnNameParam);


}

/// <summary>
/// The SQL Server database adapter.
/// </summary>
public partial class SqlServerBulkAdapter : ISqlBulkAdapter
{

    /// <summary>
    /// Adds the name of a column.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("[{0}]", columnName);
    }

    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("[{0}] = @{1}", columnName, columnName);
    }

    public void AppendColumnNameEqualsValueMulty(StringBuilder sb, string columnName, string columnNameParam)
    {
        sb.AppendFormat("[{0}] = @{1}", columnName, columnNameParam);
    }

    public void BulkUpdate<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, IEnumerable<string> updateColumns, string parameterList, IEnumerable<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
      
        var tempTable = $"#tmp_{Guid.NewGuid().ToString("N")}";
        var query = $"SELECT {columnList} INTO {tempTable}  FROM {tableName} WHERE 1<>1 ;";
        connection.Execute(query);
        connection.Execute($"SET IDENTITY_INSERT {tempTable} ON;");
        var cmd = $"insert into {tempTable} ({columnList}) values {parameterList}";
        connection.Execute(cmd, param, transaction, commandTimeout);


        var equalKeyvalue = new StringBuilder();

        string updateCommand = GetUpdateCommand(tableName, updateColumns.ToList(), keys.ToList(), tempTable, equalKeyvalue);

        connection.Execute(updateCommand, null, transaction, commandTimeout);
    }

    private static string GetUpdateCommand(string tableName, List<string> updateColumns, List<string> keys, string tempTable, StringBuilder equalKeyvalue)
    {
        for (int i = 0; i < keys.Count; i++)
        {
            var key = $"[{keys[i]}]";
            equalKeyvalue.Append($@"t1.{key} = t2.{key}");
            if (i < keys.Count - 1)
                equalKeyvalue.Append(" AND ");

        }

        var updateSetItem = new StringBuilder();
        for (var i = 0; i < updateColumns.Count; i++)
        {
            var columnName = $"[{updateColumns[i]}]";
            updateSetItem.Append($" t1.{columnName}=t2.{columnName} ");
            if (i < updateColumns.Count - 1)
                updateSetItem.Append(", ");
        }


        var updateCommand = $@"UPDATE t1 
                                SET {updateSetItem}
                                 FROM {tableName} t1   INNER JOIN {tempTable} t2 ON {equalKeyvalue} 
                            ;";
        return updateCommand;
    }
}


/// <summary>
/// The MySQL database adapter.
/// </summary>
public partial class MySqlBulkAdapter : ISqlBulkAdapter
{
    /// <summary>
    /// Adds the name of a column.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("`{0}`", columnName);
    }

    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("`{0}` = @{1}", columnName, columnName);
    }

    public void AppendColumnNameEqualsValueMulty(StringBuilder sb, string columnName, string columnNameParam)
    {
        sb.AppendFormat("`{0}` = @{1}", columnName, columnNameParam);
    }


    public void BulkUpdate<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, IEnumerable<string> updateColumns, string parameterList, IEnumerable<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {

        var tempTable = $"{tableName}_{Guid.NewGuid().ToString("N")}";
        connection.Execute($"CREATE TEMPORARY TABLE IF NOT EXISTS `{tempTable}` AS SELECT {columnList} FROM `{tableName}` LIMIT 0;");
        var cmd = $"insert into `{tempTable}` ({columnList}) values {parameterList}";
        connection.Execute(cmd, param, transaction, commandTimeout);

        string updateCommand = GetUpdateCommand(tableName, updateColumns.ToList(), keys.ToList(), tempTable);

        connection.Execute(updateCommand, null, transaction, commandTimeout);

    }

    private static string GetUpdateCommand(string tableName, List<string> updateColumns, List<string> keys, string tempTable)
    {
        var equalKeyvalue = new StringBuilder();

        for (int i = 0; i < keys.Count; i++)
        {
            var key = $"`{keys[i]}`";
            equalKeyvalue.Append($@"t1.{key} = t2.{key}");
            if (i < keys.Count - 1)
                equalKeyvalue.Append(" AND ");

        }

        var updateSetItem = new StringBuilder();
        for (var i = 0; i < updateColumns.Count; i++)
        {
            var columnName = $"`{updateColumns[i]}`";
            updateSetItem.Append($" t1.{columnName}=t2.{columnName} ");
            if (i < updateColumns.Count - 1)
                updateSetItem.Append(", ");
        }


        var updateCommand = $@"UPDATE  `{tableName}` t1 
                                    INNER JOIN `{tempTable}` t2 ON {equalKeyvalue} 
                            SET {updateSetItem} ;";
        return updateCommand;
    }
}

/// <summary>
/// The Postgres database adapter.
/// </summary>
public partial class PostgresBulkAdapter : ISqlBulkAdapter
{
    /// <summary>
    /// Adds the name of a column.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("\"{0}\"", columnName);
    }

    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("\"{0}\" = @{1}", columnName, columnName);
    }


    /// <summary>
    /// Add column and param 
    /// </summary>
    /// <param name="sb"></param>
    /// <param name="columnName"></param>
    /// <param name="columnNameParam"></param>
    public void AppendColumnNameEqualsValueMulty(StringBuilder sb, string columnName, string columnNameParam)
    {
        sb.AppendFormat("\"{0}\" = @{1}", columnName, columnNameParam);
    }

    /// <summary>
    /// Update list of entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="tableName"></param>
    /// <param name="columnList"></param>
    /// <param name="updateColumns"></param>
    /// <param name="parameterList"></param>
    /// <param name="keys"></param>
    /// <param name="param"></param>
    /// <param name="entities"></param>
    public void BulkUpdate<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, IEnumerable<string> updateColumns, string parameterList, IEnumerable<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
        var tempTable = $"\"{tableName}_{Guid.NewGuid().ToString("N")}\"";
        tableName = $"{tableName}";
        connection.Execute($"CREATE TEMPORARY TABLE IF NOT EXISTS {tempTable} AS SELECT {columnList} FROM {tableName} LIMIT 0;");
        var cmd = $"insert into {tempTable} ({columnList}) values {parameterList}";
        connection.Execute(cmd, param, transaction, commandTimeout);

        string updateCommand = GetUpdateCommand(tableName, updateColumns.ToList(), keys.ToList(), tempTable);

        connection.Execute(updateCommand, null, transaction, commandTimeout);
    }

    /// <summary>
    /// Build update command
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="updateColumns"></param>
    /// <param name="keys"></param>
    /// <param name="tempTable"></param>
    /// <returns></returns>
    private static string GetUpdateCommand(string tableName, List<string> updateColumns, List<string> keys, string tempTable)
    {
        var equalKeyvalue = new StringBuilder();

        for (int i = 0; i < keys.Count; i++)
        {
            var key = $"\"{keys[i]}\"";
            equalKeyvalue.Append($"t1.{key} = t2.{key}");
            if (i < keys.Count - 1)
                equalKeyvalue.Append(" AND ");

        }

        var updateSetItem = new StringBuilder();
        for (var i = 0; i < updateColumns.Count; i++)
        {
            var columnName = $"\"{updateColumns[i]}\"";
            updateSetItem.Append($" {columnName}=t2.{columnName} ");
            if (i < updateColumns.Count - 1)
                updateSetItem.Append(", ");
        }


        var updateCommand = $@"UPDATE  {tableName} as t1 
                            SET {updateSetItem}
                            FROM {tempTable} as  t2
                            WHERE {equalKeyvalue} ;";
        return updateCommand;
    }
}

/// <summary>
/// The SQLite database adapter.
/// </summary>
public partial class SQLiteBulkAdapter : ISqlBulkAdapter
{
    /// <summary>
    /// Adds the name of a column.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("\"{0}\"", columnName);
    }

    /// <summary>
    /// Adds a column equality to a parameter.
    /// </summary>
    /// <param name="sb">The string builder  to append to.</param>
    /// <param name="columnName">The column name.</param>
    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.AppendFormat("\"{0}\" = @{1}", columnName, columnName);
    }

    /// <summary>
    /// Adds a column equality to a parameter
    /// </summary>
    /// <param name="sb"></param>
    /// <param name="columnName"></param>
    /// <param name="columnNameParam"></param>
    public void AppendColumnNameEqualsValueMulty(StringBuilder sb, string columnName, string columnNameParam)
    {
        sb.AppendFormat("\"{0}\" = @{1}", columnName, columnNameParam);
    }

    /// <summary>
    /// Update list of entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="tableName"></param>
    /// <param name="columnList"></param>
    /// <param name="updateColumns"></param>
    /// <param name="parameterList"></param>
    /// <param name="keys"></param>
    /// <param name="param"></param>
    /// <param name="entities"></param>
    public void BulkUpdate<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, IEnumerable<string> updateColumns, string parameterList, IEnumerable<string> keys, DynamicParameters param, IEnumerable<T> entities = null)
    {
        connection.Update(entities);
    }

}


