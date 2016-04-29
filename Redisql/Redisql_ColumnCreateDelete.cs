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
        public async Task<bool> TableCreateNewColumnAsync(string tableName, string columnName, Type columnType, bool makeMatchIndex, bool makeRangeIndex, object defaultValue)
        {
            bool enterTableLock = false;
            try
            {
                if (defaultValue == null)
                    return false;

                if (!CheckDataType(columnType, defaultValue.ToString()))
                    return false;

                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;
                
                enterTableLock = await TableLockEnterAsync(tableName, "");

                var cs = new ColumnSetting();
                cs.indexNumber = ts.GetNextColumnIndexNumber();
                cs.dataType = columnType;
                cs.isMatchIndex = makeMatchIndex;
                cs.isRangeIndex = makeRangeIndex;
                cs.defaultValue = defaultValue;

                if (cs.dataType == typeof(DateTime))
                {
                    var dvt = defaultValue.ToString().ToLower();
                    if (dvt.Equals("now"))
                    {
                        defaultValue = DateTime.Now.ToString();
                    }
                    else if (dvt.Equals("utcnow"))
                    {
                        defaultValue = DateTime.UtcNow.ToString();
                    }
                }

                ts.tableSchemaDic.Add(columnName, cs);

                if (makeMatchIndex)
                    ts.matchIndexColumnDic.Add(columnName, cs.indexNumber);
                
                if (makeRangeIndex)
                    ts.rangeIndexColumnDic.Add(columnName, cs.indexNumber);
                
                ts.columnIndexNameDic.Add(cs.indexNumber.ToString(), columnName);

                var tableSchemaName = RedisKey.GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", cs.indexNumber.ToString(), cs.dataType.ToString(), makeMatchIndex.ToString(), "False", makeRangeIndex.ToString(), defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, columnName, value);


                var tasklist = new List<Task<bool>>();
                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);

                //var pvks = await db.SetMembersAsync(key);
                //foreach (var primaryKeyValue in pvks)
                foreach (var primaryKeyValue in db.SetScan(key, "*"))
                {
                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashSetAsync(key, cs.indexNumber.ToString(), defaultValue.ToString()));

                    if (makeMatchIndex)
                    {
                        // make match index
                        key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, defaultValue.ToString());
                        tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                    }

                    if (makeRangeIndex)
                    {
                        // make range index
                        key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
                        var score = ConvertToScore(cs.dataType, defaultValue.ToString());
                        tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                    }
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

        public async Task<bool> TableDeleteExistingColumnAsync(string tableName, string columnName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;
                
                if (ts.primaryKeyColumnName.Equals(columnName))
                    return false; // Can not delete PrimaryKey column
                
                enterTableLock = await TableLockEnterAsync(tableName, "");

                ColumnSetting cs;
                if (!ts.tableSchemaDic.TryGetValue(ts.primaryKeyColumnName, out cs))
                    return false;
                
                var primaryKeyColumnIndex = cs.indexNumber;

                if (!ts.tableSchemaDic.TryGetValue(columnName, out cs))
                    return false;

                var tasklist = new List<Task<RedisValue[]>>();
                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);
                //var pkvs = await db.SetMembersAsync(key);

                // read column stored value
                var rva = new RedisValue[2];
                rva[0] = primaryKeyColumnIndex;
                rva[1] = cs.indexNumber;

                //foreach (var primaryKeyValue in pkvs)
                foreach (var primaryKeyValue in db.SetScan(key, "*"))
                {
                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                var tasklist2 = new List<Task<bool>>();
                foreach (var t in tasklist)
                {
                    var ret = await t;
                    if (cs.isMatchIndex)
                    {
                        key = RedisKey.GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, ret[1].ToString());
                        tasklist2.Add(db.SetRemoveAsync(key, ret[0].ToString()));
                    }

                    if (cs.isRangeIndex)
                    {
                        key = RedisKey.GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
                        tasklist2.Add(db.SortedSetRemoveAsync(key, ret[0].ToString()));
                    }

                    key = RedisKey.GetRedisKey_TableRow(ts.tableID, ret[0].ToString());
                    tasklist2.Add(db.HashDeleteAsync(key, cs.indexNumber));
                }

                foreach (var t in tasklist2)
                {
                    if (!await t) return false;
                }

                // 데이터를 다 지웠으니 테이블 스키마 제거
                ts.tableSchemaDic.Remove(columnName);
                ts.columnIndexNameDic.Remove(cs.indexNumber.ToString());

                Int32 v;
                if (ts.matchIndexColumnDic.TryGetValue(columnName, out v))
                    ts.matchIndexColumnDic.Remove(columnName);
                
                if (ts.rangeIndexColumnDic.TryGetValue(columnName, out v))
                    ts.rangeIndexColumnDic.Remove(columnName);
                
                key = RedisKey.GetRedisKey_TableSchema(tableName);
                await db.HashDeleteAsync(key, columnName);

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
