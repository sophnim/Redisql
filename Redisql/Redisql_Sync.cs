using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;

using StackExchange.Redis;

namespace Redisql.Core
{
    public partial class RedisqlCore
    {
        public bool TableLockClearAll()
        {
            return WaitTaskAndReturnTaskResult<bool>(TableLockClearAllAsync());
        }

        public bool TableCreate(string tableName, List<ColumnConfig> columnConfigList, string primaryKeyColumnName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableCreateAsync(tableName, columnConfigList, primaryKeyColumnName));
        }

        public bool TableDelete(string tableName)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableDeleteAsync(tableName));
        }

        public Int64 TableInsertRow(string tableName, Dictionary<string, string> insertRowColumnNameValuePairs)
        {
            return WaitTaskAndReturnTaskResult<Int64>(TableInsertRowAsync(tableName, insertRowColumnNameValuePairs));
        }

        public bool TableUpdateRow(string tableName, Dictionary<string, string> updateColumnNameValuePairs)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableUpdateRowAsync(tableName, updateColumnNameValuePairs));
        }

        public bool TableDeleteRow(string tableName, string primaryKeyValue)
        {
            return WaitTaskAndReturnTaskResult<bool>(TableDeleteRowAsync(tableName, primaryKeyValue));
        }

        public Dictionary<string, string> TableSelectRow(List<string> selectColumnNames, string tableName, string primaryKeyColumnValue)
        {
            return WaitTaskAndReturnTaskResult<Dictionary<string, string>>(TableSelectRowAsync(selectColumnNames, tableName, primaryKeyColumnValue));
        }

        public List<Dictionary<string, string>> TableSelectRow(List<string> selectColumnNames, string tableName, string compareMatchIndexColumnName, string compareColumnValue)
        {
            return WaitTaskAndReturnTaskResult<List<Dictionary<string, string>>>(TableSelectRowAsync(selectColumnNames, tableName, compareMatchIndexColumnName, compareColumnValue));
        }

        public List<Dictionary<string, string>> TableSelectRow(List<string> selectColumnNames, string tableName, string compareRangeIndexColumnName, string lowValue, string highValue)
        {
            return WaitTaskAndReturnTaskResult<List<Dictionary<string, string>>>(TableSelectRowAsync(selectColumnNames, tableName, compareRangeIndexColumnName, lowValue, highValue));
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

        // select all rows in table
        public IEnumerable<Dictionary<string, string>> TableSelectRowAll(List<string> selectColumnNames, string tableName)
        {
            var ts = WaitTaskAndReturnTaskResult<TableSetting>(TableGetSettingAsync(tableName));
            var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);
            var db = this.redis.GetDatabase();

            RedisValue[] rva = null;
            if (null != selectColumnNames)
            {
                // read specified column values
                rva = GetSelectColumnIndexNumbers(ts, selectColumnNames);
            }

            foreach (var primaryKeyValue in db.SetScan(key, "*"))
            {
                var retdic = new Dictionary<string, string>();
                key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                if (null == selectColumnNames)
                {
                    var rowdata = db.HashGetAll(key);
                    var len = rowdata.Length;
                    for (var i = 0; i < len; i++)
                    {
                        var e = rowdata[i];
                        string tableFieldName;
                        if (ts.columnIndexNameDic.TryGetValue(e.Name.ToString(), out tableFieldName))
                        {
                            retdic.Add(tableFieldName, e.Value.ToString());
                        }
                    }
                }
                else
                {
                    var rowdata = db.HashGet(key, rva);
                    var len = rowdata.Length;
                    if (null != rowdata)
                    {
                        for (var i = 0; i < len; i++)
                        {
                            retdic.Add(selectColumnNames[i], rowdata[i].ToString());
                        }
                    }
                }

                yield return retdic;
            }

            yield break;
        }
    }
}
