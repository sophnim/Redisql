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
        public async Task<bool> TableDeleteAsync(string tableName)
        {
            bool enterTableLock = false;
            try
            {
                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                var key = RedisKey.GetRedisKey_TablePrimaryKeyList(tableName);

                // 모든 Table row를 지워서 삭제한다.
                var tasklist = new List<Task<bool>>();
                var pkvs = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pkvs)
                {
                    tasklist.Add(TableRowDeleteAsync(tableName, primaryKeyValue.ToString()));
                }

                // 테이블 스키마 삭제
                key = RedisKey.GetRedisKey_TableSchema(tableName);
                tasklist.Add(db.KeyDeleteAsync(key));

                // 테이블 ID 해시 삭제
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableNameIds, tableName));

                // 테이블 자동 증가 값 해시 삭제
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableAutoIncrementColumnValues, ts.tableID));

                foreach (var t in tasklist)
                {
                    await t;
                }

                // 메모리상의 테이블 세팅 삭제
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }

        // List<Tuple<string,Type,bool,bool,object>> column list : columnName, columnType, make matchIndex, make rangeIndex, defaultValue
        public async Task<bool> TableCreateAsync(string tableName, string primaryKeyColumnName, List<Tuple<string, Type, bool, bool, object>> columnInfoList)
        {
            bool enterTableLock = false;
            try
            {
                // check input parameters 
                foreach (var tpl in columnInfoList)
                {
                    if (tpl.Item4)
                    {
                        switch (tpl.Item2.ToString())
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

                // 모든 테이블은 기본적으로 _id field가 추가되고 이 필드는 자동 증가값을 갖는다.
                columnInfoList.Insert(0, new Tuple<string, Type, bool, bool, object>("_id", typeof(Int64), false, false, null));

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                var db = this.redis.GetDatabase();

                // check table already exists
                var ret = await db.HashExistsAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (ret)
                {
                    // already existing table name
                    return false;
                }

                // get table id
                var tableID = await db.StringIncrementAsync(Consts.RedisKey_String_TableNameIds);

                // write tableName-id
                await db.HashSetAsync(Consts.RedisKey_Hash_TableNameIds, tableName, tableID);

                // write table schema
                var tableSchemaName = RedisKey.GetRedisKey_TableSchema(tableName);
                int fieldIndex = 0;
                foreach (var t in columnInfoList)
                {
                    bool pkFlag = false;
                    bool matchIndexFlag = t.Item3;
                    bool rangeIndexFlag = t.Item4;
                    object defaultValue = t.Item5;
                    if (defaultValue == null)
                    {
                        defaultValue = "null";
                    }

                    if (t.Item1.Equals(primaryKeyColumnName))
                    {
                        pkFlag = true;
                        matchIndexFlag = false;
                    }

                    var value = string.Format("{0},{1},{2},{3},{4},{5}", (fieldIndex++).ToString(), t.Item2.ToString(), matchIndexFlag.ToString(), pkFlag.ToString(), rangeIndexFlag.ToString(), defaultValue.ToString()); // fieldIndex, Type, matchIndexFlag, primaryKeyFlag, rangeIndexFlag
                    await db.HashSetAsync(tableSchemaName, t.Item1, value);
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
