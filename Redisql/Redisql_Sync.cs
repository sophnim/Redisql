using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;

using StackExchange.Redis;

namespace Redisql
{
    public partial class Redisql
    {
        public bool TableCreate(string tableName, string primaryKeyColumnName, List<Tuple<string, Type, bool, bool, object>> columnInfoList)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableCreateAsync(tableName, primaryKeyColumnName, columnInfoList));
        }

        public bool TableDelete(string tableName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableDeleteAsync(tableName));
        }

        public Int64 TableRowInsert(string tableName, Dictionary<string, string> insertRowColumnNameValuePairs)
        {
            return WaitTaskAndReturnTaskResult<Int64>(TableRowInsertAsync(tableName, insertRowColumnNameValuePairs));
        }

        public bool TableRowUpdate(string tableName, Dictionary<string, string> updateColumnNameValuePairs)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableRowUpdateAsync(tableName, updateColumnNameValuePairs));
        }

        public bool TableRowDelete(string tableName, string primaryKeyValue)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableRowDeleteAsync(tableName, primaryKeyValue));
        }

        public Dictionary<string, string> TableRowSelectByPrimaryKeyColumnValue(List<string> selectColumnNames, string tableName, string primaryKeyColumnValue)
        {
            return WaitTaskAndReturnTaskResult<Dictionary<string, string>>(TableRowSelectByPrimaryKeyColumnValueAsync(selectColumnNames, tableName, primaryKeyColumnValue));
        }

        public List<Dictionary<string, string>> TableRowSelectByMatchIndexColumnValue(List<string> selectColumnNames, string tableName, string compareMatchIndexColumnName, string compareColumnValue)
        {
            return WaitTaskAndReturnTaskResult<List<Dictionary<string, string>>>(TableRowSelectByMatchIndexColumnValueAsync(selectColumnNames, tableName, compareMatchIndexColumnName, compareColumnValue));
        }

        public List<Dictionary<string, string>> TableRowSelectByRangeIndex(List<string> selectColumnNames, string tableName, string compareRangeIndexColumnName, string lowValue, string highValue)
        {
            return WaitTaskAndReturnTaskResult<List<Dictionary<string, string>>>(TableRowSelectByRangeIndexAsync(selectColumnNames, tableName, compareRangeIndexColumnName, lowValue, highValue));
        }

        public bool TableCreateNewColumn(string tableName, string columnName, Type columnType, bool makeMatchIndex, bool makeRangeIndex, object defaultValue)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableCreateNewColumnAsync(tableName, columnName, columnType, makeMatchIndex, makeRangeIndex, defaultValue));
        }

        public bool TableDeleteExistingColumn(string tableName, string columnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableDeleteExistingColumnAsync(tableName, columnName));
        }

        public bool TableAddMatchIndex(string tableName, string columnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableAddMatchIndexAsync(tableName, columnName));
        }

        public bool TableRemoveMatchIndex(string tableName, string columnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableRemoveMatchIndexAsync(tableName, columnName));
        }

        public bool TableAddRangeIndex(string tableName, string columnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableAddRangeIndexAsync(tableName, columnName));
        }

        public bool TableRemoveRangeIndex(string tableName, string columnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableRemoveRangeIndexAsync(tableName, columnName));
        }
    }
}
