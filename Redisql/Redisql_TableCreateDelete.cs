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
        public async Task<bool> TableCreateAsync(string tableName, List<ColumnConfig> columnConfigList, string primaryKeyColumnName)
        {
            bool enterTableLock = false;
            try
            {
                var ccflist = new List<ColumnConfig>(columnConfigList);

                // check input parameters 
                foreach (var cf in ccflist)
                {
                    if (cf.name.Equals("_id"))
                        return false; // _id column name is reserved.  

                    if (cf.makeRankgeIndex)
                    {
                        switch (cf.type.ToString())
                        {
                            case "System.Byte":
                            case "System.Int16":
                            case "System.UInt16":
                            case "System.Int32":
                            case "System.UInt32":
                            case "System.Single":
                            case "System.Double":
                            case "System.DateTime": // these types could be range indexed
                                break;

                            default: // other types cannot be range indexed
                                return false;
                        }
                    }
                }

                // every table automatically generate _id column (auto increment)
                ccflist.Insert(0, new ColumnConfig("_id", typeof(Int64), defaultValue:null));

                var ts = new TableSetting();
                ts.tableName = tableName;
                enterTableLock = await TableLockEnterAsync(ts, "");

                var db = this.redis.GetDatabase();

                // check table already exists
                var ret = await db.HashExistsAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (ret)
                    return false; // already existing table name

                // get table id
                var tableID = await db.StringIncrementAsync(Consts.RedisKey_String_TableNameIds);

                // write tableName-id
                await db.HashSetAsync(Consts.RedisKey_Hash_TableNameIds, tableName, tableID);

                // write table schema
                var tableSchemaName = RedisKey.GetRedisKey_TableSchema(tableName);
                int fieldIndex = 0;
                foreach (var cf in ccflist)
                {
                    bool pkFlag = false;
                    if (cf.defaultValue == null)
                        cf.defaultValue = "null";

                    if (cf.name.Equals(primaryKeyColumnName))
                    {
                        pkFlag = true;
                        cf.makeMatchIndex = false; // primary key column does not need additional match index
                    }

                    var value = string.Format("{0},{1},{2},{3},{4},{5}", (fieldIndex++).ToString(), cf.type.ToString(), cf.makeMatchIndex.ToString(), pkFlag.ToString(), cf.makeRankgeIndex.ToString(), cf.defaultValue.ToString()); // fieldIndex, Type, matchIndexFlag, primaryKeyFlag, rangeIndexFlag, defaultValue
                    await db.HashSetAsync(tableSchemaName, cf.name, value);
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
                    await TableLockExit(tableName, "");
            }
        }

        public async Task<bool> TableDeleteAsync(string tableName)
        {
            bool enterTableLock = false;
            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                    return false;

                enterTableLock = await TableLockEnterAsync(ts, "");

                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);

                // delete every table rows
                var tasklist = new List<Task<bool>>();
                foreach (var primaryKeyValue in db.SetScan(key, "*"))
                {
                    tasklist.Add(TableDeleteRowAsync(tableName, primaryKeyValue.ToString()));
                }

                // delete table schema 
                key = RedisKey.GetRedisKey_TableSchema(tableName);
                tasklist.Add(db.KeyDeleteAsync(key));

                // delete table name id
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableNameIds, tableName));

                // delete table auto increment column value
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableAutoIncrementColumnValues, ts.tableID));

                foreach (var t in tasklist)
                {
                    await t;
                }

                // remove table setting info
                this.tableSettingDic.TryRemove(tableName, out ts);

                return true;
            }
            catch (Exception ex)
            {

                return false;
            }
            finally
            {
                if (enterTableLock)
                    await TableLockExit(tableName, "");
            }
        }
    }
}
