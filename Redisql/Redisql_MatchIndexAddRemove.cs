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
        // Add match index to unindexed existing table field
        public async Task<bool> TableAddMatchIndexAsync(string tableName, string columnName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;
                
                if (ts.primaryKeyColumnName.Equals(columnName))
                    return false; // Can not add index to primary key column
                
                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(columnName, out cs))
                    return false;
                
                if (cs.isMatchIndex)
                    return false; // Already indexed column. No need to add index.
                
                enterTableLock = await TableLockEnterAsync(tableName, "");

                cs.isMatchIndex = true;
                ts.matchIndexColumnDic.Add(columnName, cs.indexNumber);

                var tableSchemaName = RedisKey.GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", cs.indexNumber.ToString(), cs.dataType.ToString(), cs.isMatchIndex.ToString(), "False", cs.isRangeIndex.ToString(), cs.defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, columnName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);

                foreach (var primaryKeyValue in db.SetScan(key, "*"))
                {
                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, cs.indexNumber);

                    // add index
                    key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, v.ToString());
                    tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                }

                foreach (var t in tasklist)
                {
                    if (!await t) return false;
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
                    TableLockExit(tableName, "");
            }
        }

        // Remove match index to indexed existing table field
        public async Task<bool> TableRemoveMatchIndexAsync(string tableName, string columnName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;
                
                if (ts.primaryKeyColumnName.Equals(columnName))
                    return false; // Can not remove index to primary key column
                
                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(columnName, out cs))
                    return false;
                
                if (!cs.isMatchIndex)
                    return false; // Not indexed column. Could not remove index.
                
                enterTableLock = await TableLockEnterAsync(tableName, "");

                cs.isMatchIndex = false;
                ts.matchIndexColumnDic.Remove(columnName);

                var tableSchemaName = RedisKey.GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", cs.indexNumber.ToString(), cs.dataType.ToString(), cs.isMatchIndex.ToString(), "False", cs.isRangeIndex.ToString(), cs.defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, columnName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);

                foreach (var primaryKeyValue in db.SetScan(key, "*"))
                {
                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, cs.indexNumber);

                    // remove index
                    key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, v.ToString());
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                foreach (var t in tasklist)
                {
                    if (!await t) return false;
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
                    TableLockExit(tableName, "");
            }
        }
    }
}
