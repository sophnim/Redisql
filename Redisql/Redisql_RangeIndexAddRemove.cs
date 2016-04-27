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
        // Add range index to not sort-indexed existing table field
        public async Task<bool> TableAddRangeIndexAsync(string tableName, string columnName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(columnName, out cs))
                {
                    return false;
                }

                if (cs.isRangeIndex)
                {
                    return false; // Already range indexed field. 
                }

                bool pkFlag = false;
                if (ts.primaryKeyColumnName.Equals(columnName))
                {
                    pkFlag = true;
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                cs.isRangeIndex = true;
                ts.rangeIndexColumnDic.Add(columnName, cs.indexNumber);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", cs.indexNumber.ToString(), cs.dataType.ToString(), cs.isMatchIndex.ToString(), pkFlag.ToString(), cs.isRangeIndex.ToString(), cs.defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, columnName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, cs.indexNumber);

                    // add range index
                    key = GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
                    var score = ConvertToScore(cs.dataType, v.ToString());
                    tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }

        // Remove range index to range indexed existing table field
        public async Task<bool> TableRemoveRangeIndexAsync(string tableName, string columnName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(columnName, out cs))
                {
                    return false;
                }

                if (!cs.isRangeIndex)
                {
                    return false; // Not range indexed field. 
                }

                bool pkFlag = false;
                if (ts.primaryKeyColumnName.Equals(columnName))
                {
                    pkFlag = true;
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                cs.isRangeIndex = false;
                ts.rangeIndexColumnDic.Remove(columnName);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", cs.indexNumber.ToString(), cs.dataType.ToString(), cs.isMatchIndex.ToString(), pkFlag.ToString(), cs.isRangeIndex.ToString(), cs.defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, columnName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, cs.indexNumber);

                    // remove range index
                    key = GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
                    tasklist.Add(db.SortedSetRemoveAsync(key, primaryKeyValue));
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }
    }
}
