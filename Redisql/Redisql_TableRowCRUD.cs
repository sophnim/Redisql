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
        // add row to table and get auto incremented _id value
        public async Task<Int64> TableInsertRowAsync(string tableName, Dictionary<string, string> insertRowColumnNameValuePairs)
        {
            string key;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return -1;
                
                if (!ts.primaryKeyColumnName.Equals("_id"))
                {
                    // get primaryKey value of insert row
                    if (!insertRowColumnNameValuePairs.TryGetValue(ts.primaryKeyColumnName, out primaryKeyValue))
                        return -1;
                    
                    // check if row already exists
                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                    var ret = await db.KeyExistsAsync(key);
                    if (ret)
                        return -1;
                }

                HashEntry[] heArray = new HashEntry[ts.tableSchemaDic.Count];

                // get _id value : auto increment
                var idValue = await db.HashIncrementAsync(Consts.RedisKey_Hash_TableAutoIncrementColumnValues, ts.tableID.ToString());
                heArray[0] = new HashEntry(0, idValue);

                if (ts.primaryKeyColumnName.Equals("_id"))
                    primaryKeyValue = idValue.ToString();

                int arrayIndex = 1;
                List<Task> tasklist = new List<Task>();

                foreach (var e in ts.tableSchemaDic)
                {
                    var cs = e.Value;
                    if (!e.Key.Equals("_id")) // _id Column can not assigned from request : ignored
                    {
                        string value;
                        if (insertRowColumnNameValuePairs.TryGetValue(e.Key, out value))
                        {
                            if (!CheckDataType(cs.dataType, value)) // check column value data type 
                                return -1;

                            heArray[arrayIndex++] = new HashEntry(cs.indexNumber, value);
                        }
                        else
                        {
                            // apply default value
                            if (null == cs.defaultValue)
                            {
                                // This column has no default value
                                return -1;
                            }
                            else
                            {
                                switch (cs.dataType.ToString())
                                {
                                    case "System.Byte":
                                    case "System.Int16":
                                    case "System.UInt16":
                                    case "System.Int32":
                                    case "System.UInt32":
                                    case "System.Single":
                                    case "System.Double":
                                    case "System.String":
                                        heArray[arrayIndex++] = new HashEntry(cs.indexNumber, cs.defaultValue.ToString());
                                        break;

                                    case "System.DateTime":
                                        if (cs.defaultValue.ToString().ToLower().Equals("now"))
                                        {
                                            heArray[arrayIndex++] = new HashEntry(cs.indexNumber, DateTime.Now.ToString());
                                        }
                                        else if (cs.defaultValue.ToString().ToLower().Equals("utcnow"))
                                        {
                                            heArray[arrayIndex++] = new HashEntry(cs.indexNumber, DateTime.UtcNow.ToString());
                                        }
                                        else
                                        {
                                            heArray[arrayIndex++] = new HashEntry(cs.indexNumber, cs.defaultValue.ToString());
                                        }
                                        break;
                                }
                            }
                        }

                        if (e.Value.isMatchIndex)
                        {
                            // make match index
                            key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.indexNumber, value);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (e.Value.isRangeIndex)
                        {
                            // make range index
                            key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value.indexNumber);
                            var score = ConvertToScore(e.Value.dataType, value);
                            tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                        }
                    }
                    else
                    {
                        // _id field
                    }
                }

                // save table row
                key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                tasklist.Add(db.HashSetAsync(key, heArray));

                // save table primary key 
                key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);
                tasklist.Add(db.SetAddAsync(key, primaryKeyValue));

                foreach (var task in tasklist)
                {
                    await task;
                }

                return idValue;
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        public async Task<bool> TableUpdateRowAsync(string tableName, Dictionary<string, string> updateColumnNameValuePairs)
        {
            string key;
            bool enterTableLock = false;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;
                
                if (!updateColumnNameValuePairs.TryGetValue(ts.primaryKeyColumnName, out primaryKeyValue))
                    return false; // primary key column not found

                var updatedColumns = new Dictionary<string, Tuple<Int32, string>>(); // Column name, Tuple<columnIndex, updatedValue>

                foreach (var e in ts.matchIndexColumnDic)
                {
                    string value;
                    if (updateColumnNameValuePairs.TryGetValue(e.Key, out value))
                        updatedColumns.Add(e.Key, new Tuple<Int32, string>(e.Value, value)); // update column is index column
                }

                foreach (var e in ts.rangeIndexColumnDic)
                {
                    string value;
                    if (updateColumnNameValuePairs.TryGetValue(e.Key, out value))
                    {
                        if (!updatedColumns.ContainsKey(e.Key))
                            updatedColumns.Add(e.Key, new Tuple<Int32, string>(e.Value, value)); // add if not duplicated with index column
                    }
                }

                enterTableLock = await TableLockEnterAsync(ts, primaryKeyValue);

                // get values from stored in update columns
                if (updatedColumns.Count > 0)
                {
                    int index = 0;
                    var rvArray = new RedisValue[updatedColumns.Count];
                    foreach (var e in updatedColumns)
                    {
                        rvArray[index++] = e.Value.Item1;
                    }

                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                    var ret = await db.HashGetAsync(key, rvArray);

                    index = 0;
                    foreach (var e in updatedColumns)
                    {
                        if (ts.matchIndexColumnDic.ContainsKey(e.Key))
                        {
                            // update match index column to new value
                            key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.Item1, ret[index].ToString());
                            tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                            key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.Item1, e.Value.Item2);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (ts.rangeIndexColumnDic.ContainsKey(e.Key))
                        {
                            // update range index column to new value
                            key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value.Item1);
                            var score = ConvertToScore(ts.tableSchemaDic[e.Key].dataType, e.Value.Item2);
                            tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                        }

                        index++;
                    }
                }

                int arrayIndex = 0;
                HashEntry[] heArray = new HashEntry[updateColumnNameValuePairs.Count];
                foreach (var e in updateColumnNameValuePairs)
                {
                    ColumnSetting cs;
                    if (ts.tableSchemaDic.TryGetValue(e.Key, out cs))
                    {
                        heArray[arrayIndex++] = new HashEntry(cs.indexNumber, e.Value);
                    }
                }

                // save table row
                key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                tasklist.Add(db.HashSetAsync(key, heArray));

                foreach (var task in tasklist)
                {
                    await task;
                }

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                if (enterTableLock)
                    await TableLockExit(tableName, primaryKeyValue);
            }
        }

        public async Task<bool> TableDeleteRowAsync(string tableName, string primaryKeyValue)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;

                enterTableLock = await TableLockEnterAsync(ts, primaryKeyValue);

                // before delete, read all row to delete index
                var key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                var ret = await db.HashGetAllAsync(key);
                if (null == ret)
                    return false;

                var fvdic = new Dictionary<string, string>();
                foreach (var e in ret)
                {
                    fvdic.Add(e.Name.ToString(), e.Value.ToString());
                }

                // delete match index
                foreach (var fieldIndex in ts.matchIndexColumnDic.Values)
                {
                    key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, fieldIndex, fvdic[fieldIndex.ToString()]);
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // delete range index
                foreach (var e in ts.rangeIndexColumnDic)
                {
                    key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value);
                    tasklist.Add(db.SortedSetRemoveAsync(key, primaryKeyValue));
                }

                // delete table row
                key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                tasklist.Add(db.KeyDeleteAsync(key));

                key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);
                tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                foreach (var task in tasklist)
                {
                    await task;
                }

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                if (enterTableLock)
                    await TableLockExit(tableName, primaryKeyValue);
            }
        }

        // select one row that matches with primary key column value. 
        // If selectColumnNames is null, select all columns in selected row, or select specified columns only.
        public async Task<Dictionary<string, string>> TableSelectRowByPrimaryKeyColumnValueAsync(List<string> selectColumnNames, string tableName, string primaryKeyColumnValue)
        {
            try
            {
                var retdic = new Dictionary<string, string>();
                var ts = await TableGetSettingAsync(tableName);
                var key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyColumnValue);
                var db = this.redis.GetDatabase();

                if (null == selectColumnNames)
                {
                    // read all column values
                    var ret = await db.HashGetAllAsync(key);
                    if (null != ret)
                    {
                        var len = ret.Length;
                        for (var i = 0; i < len; i++)
                        {
                            var e = ret[i];
                            string tableFieldName;
                            if (ts.columnIndexNameDic.TryGetValue(e.Name.ToString(), out tableFieldName))
                            {
                                retdic.Add(tableFieldName, e.Value.ToString());
                            }
                        }
                    }
                }
                else
                {
                    // read specified column values
                    var rva = GetSelectColumnIndexNumbers(ts, selectColumnNames);
                    
                    var ret = await db.HashGetAsync(key, rva);
                    if (null != ret)
                    {
                        for (var i = 0; i < rva.Length; i++)
                        {
                            retdic.Add(selectColumnNames[i], ret[i].ToString());
                        }
                    }
                }

                return retdic;
            }
            catch
            {
                return null;
            }
        }

        // select all rows that matches match index column value
        public async Task<List<Dictionary<string, string>>> TableSelectRowByMatchIndexColumnValueAsync(List<string> selectColumnNames, string tableName, string compareMatchIndexColumnName, string compareColumnValue)
        {
            try
            {
                var retlist = new List<Dictionary<string, string>>();
                var ts = await TableGetSettingAsync(tableName);
                var db = this.redis.GetDatabase();

                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(compareMatchIndexColumnName, out cs))
                    throw new Exception(string.Format("Table '{0}' does not have '{1}' column", tableName, compareMatchIndexColumnName));

                var key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, compareColumnValue);
                var pkvs = new List<RedisValue>();
                foreach (var rv in db.SetScan(key, "*"))
                {
                    pkvs.Add(rv);
                }

                if (null == selectColumnNames)
                {
                    // read all columns
                    var tasklist = new List<Task<HashEntry[]>>();
                    foreach (var pk in pkvs)
                    {
                        key = RedisKey.GetRedisKey_TableRow(ts.tableID, pk.ToString());
                        tasklist.Add(db.HashGetAllAsync(key));
                    }

                    foreach (var task in tasklist)
                    {
                        await task;
                        var heArray = task.Result;
                        var dic = new Dictionary<string, string>();
                        foreach (var he in heArray)
                        {
                            string tableFieldName;
                            if (ts.columnIndexNameDic.TryGetValue(he.Name.ToString(), out tableFieldName))
                            {
                                dic.Add(tableFieldName, he.Value.ToString());
                            }
                        }
                        retlist.Add(dic);
                    }
                }
                else
                {
                    // read specified columns
                    var rva = GetSelectColumnIndexNumbers(ts, selectColumnNames);
                    
                    var tasklist = new List<Task<RedisValue[]>>();
                    foreach (var pk in pkvs)
                    {
                        key = RedisKey.GetRedisKey_TableRow(ts.tableID, pk.ToString());
                        tasklist.Add(db.HashGetAsync(key, rva));
                    }

                    foreach (var task in tasklist)
                    {
                        await task;
                        rva = task.Result;
                        var dic = new Dictionary<string, string>();

                        for (var i = 0; i < rva.Length; i++)
                        {
                            dic.Add(selectColumnNames[i], rva[i].ToString());
                        }
                        retlist.Add(dic);
                    }
                }

                return retlist;
            }
            catch
            {
                return null;
            }
        }

        // select all rows that has range index column values is between lowValue and highValue
        public async Task<List<Dictionary<string, string>>> TableSelectRowByRangeIndexAsync(List<string> selectColumnNames, string tableName, string compareRangeIndexColumnName, string lowValue, string highValue)
        {
            try
            {
                var retlist = new List<Dictionary<string, string>>();
                var ts = await TableGetSettingAsync(tableName);
                var db = this.redis.GetDatabase();

                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(compareRangeIndexColumnName, out cs))
                {
                    return retlist;
                }

                var lv = ConvertToScore(cs.dataType, lowValue);
                var hv = ConvertToScore(cs.dataType, highValue);

                var key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
                var primaryKeyValues = await db.SortedSetRangeByScoreAsync(key, lv, hv);

                if (null == selectColumnNames)
                {
                    // read all columns
                    List<Task<HashEntry[]>> tasklist = new List<Task<HashEntry[]>>();
                    foreach (var primaryKeyValue in primaryKeyValues)
                    {
                        key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                        tasklist.Add(db.HashGetAllAsync(key));
                    }

                    foreach (var task in tasklist)
                    {
                        await task;
                        var heArray = task.Result;
                        var dic = new Dictionary<string, string>();
                        foreach (var he in heArray)
                        {
                            string tableFieldName;
                            if (ts.columnIndexNameDic.TryGetValue(he.Name.ToString(), out tableFieldName))
                            {
                                dic.Add(tableFieldName, he.Value.ToString());
                            }
                        }
                        retlist.Add(dic);
                    }
                }
                else
                {
                    // read specified columns
                    var rva = GetSelectColumnIndexNumbers(ts, selectColumnNames);

                    var tasklist = new List<Task<RedisValue[]>>();
                    foreach (var primaryKeyValue in primaryKeyValues)
                    {
                        key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                        tasklist.Add(db.HashGetAsync(key, rva));
                    }

                    foreach (var task in tasklist)
                    {
                        await task;
                        rva = task.Result;
                        var dic = new Dictionary<string, string>();

                        for (var i = 0; i < rva.Length; i++)
                        {
                            dic.Add(selectColumnNames[i], rva[i].ToString());
                        }
                        retlist.Add(dic);
                    }
                }

                return retlist;
            }
            catch
            {
                return null;
            }
        }
    }
}
