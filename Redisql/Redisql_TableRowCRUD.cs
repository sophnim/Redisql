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
        // 테이블 row를 추가하고 추가된 row의 자동 증가 _id값을 얻는다.
        public async Task<Int64> TableRowInsertAsync(string tableName, Dictionary<string, string> columnValues)
        {
            string key;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return -1;
                }

                if (!ts.primaryKeyColumnName.Equals("_id"))
                {
                    // Insert하려는 row의 primary key field가 이미 존재하면 insert는 할수 없음
                    // get primaryKey value of insert row
                    if (!columnValues.TryGetValue(ts.primaryKeyColumnName, out primaryKeyValue))
                    {
                        return -1;
                    }

                    // check if row already exists
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                    var ret = await db.KeyExistsAsync(key);
                    if (ret)
                    {
                        return -1;
                    }
                }

                HashEntry[] heArray = new HashEntry[ts.tableSchemaDic.Count];

                // 자동 증가값 _id의 값을 설정한다.
                var idValue = await db.HashIncrementAsync(Consts.RedisKey_Hash_TableAutoIncrementColumnValues, ts.tableID.ToString());
                heArray[0] = new HashEntry(0, idValue);

                if (ts.primaryKeyColumnName.Equals("_id"))
                {
                    primaryKeyValue = idValue.ToString();
                }


                int arrayIndex = 1;
                List<Task> tasklist = new List<Task>();

                foreach (var e in ts.tableSchemaDic)
                {
                    var cs = e.Value;
                    if (!e.Key.Equals("_id")) // _id Column은 사용자가 값을 할당할 수 없다.
                    {
                        string value;
                        if (columnValues.TryGetValue(e.Key, out value))
                        {
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
                                        heArray[arrayIndex++] = new HashEntry(cs.indexNumber, cs.defaultValue.ToString());
                                        break;

                                    case "System.DateTime":
                                        if (cs.defaultValue.Equals("now"))
                                        {
                                            heArray[arrayIndex++] = new HashEntry(cs.indexNumber, DateTime.Now.ToString());
                                        }
                                        else if (cs.defaultValue.Equals("utcnow"))
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
                            // make index
                            key = GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.indexNumber, value);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (e.Value.isRangeIndex)
                        {
                            // sorted set index
                            key = GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value.indexNumber);
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
                key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                tasklist.Add(db.HashSetAsync(key, heArray));

                // save table primary key 
                key = GetRedisKey_TablePrimaryKeyList(tableName);
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

        public async Task<bool> TableRowUpdateAsync(string tableName, Dictionary<string, string> updateColumnValues)
        {
            bool enterLock = false;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                // 업데이트하려는 row의 primaryKey value를 얻는다.
                if (!updateColumnValues.TryGetValue(ts.primaryKeyColumnName, out primaryKeyValue))
                {
                    // 업데이트하려는 값 정보에 PrimaryKey value가 없다. 
                    return false;
                }

                // 업데이트 하려는 row의 값중 인덱스, Sorted된 값을 찾는다. 이 값들은 읽어서 갱신해야 한다.
                var updatedFields = new Dictionary<string, Tuple<Int32, string>>(); // fieldName, Tuple<fieldIndex, updatedValue>

                foreach (var e in ts.matchIndexColumnDic)
                {
                    string value;
                    if (updateColumnValues.TryGetValue(e.Key, out value))
                    {
                        // update 하려는 값중에 인덱스가 걸려있는 값이 있다. 인덱스를 갱신해야 한다.
                        updatedFields.Add(e.Key, new Tuple<Int32, string>(e.Value, value));
                    }
                }

                foreach (var e in ts.rangeIndexColumnDic)
                {
                    string value;
                    if (updateColumnValues.TryGetValue(e.Key, out value))
                    {
                        if (!updatedFields.ContainsKey(e.Key))
                        {
                            // 인덱스에서 추가된 값과 중복되지 않는 경우에만 추가 
                            updatedFields.Add(e.Key, new Tuple<Int32, string>(e.Value, value));
                        }
                    }
                }

                enterLock = true;
                await TableLockEnterAsync(tableName, primaryKeyValue);

                string key;

                // 인덱스에 저장되어 있는 값을 가져온다.
                if (updatedFields.Count > 0)
                {
                    int index = 0;
                    var rvArray = new RedisValue[updatedFields.Count];
                    foreach (var e in updatedFields)
                    {
                        rvArray[index++] = e.Value.Item1;
                    }

                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                    var ret = await db.HashGetAsync(key, rvArray);

                    index = 0;
                    foreach (var e in updatedFields)
                    {
                        if (ts.matchIndexColumnDic.ContainsKey(e.Key))
                        {
                            // 원래 값으로 저장되어 있던 인덱스를 지우고 새 값으로 갱신
                            key = GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.Item1, ret[index].ToString());
                            tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                            key = GetRedisKey_TableMatchIndexColumn(ts.tableID, e.Value.Item1, e.Value.Item2);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (ts.rangeIndexColumnDic.ContainsKey(e.Key))
                        {
                            // SortedSet의 Score를 갱신
                            key = GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value.Item1);
                            var score = ConvertToScore(ts.tableSchemaDic[e.Key].dataType, e.Value.Item2);
                            tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                        }

                        index++;
                    }
                }

                int arrayIndex = 0;
                HashEntry[] heArray = new HashEntry[updateColumnValues.Count];
                foreach (var e in updateColumnValues)
                {
                    ColumnSetting cs;
                    if (ts.tableSchemaDic.TryGetValue(e.Key, out cs))
                    {
                        heArray[arrayIndex++] = new HashEntry(cs.indexNumber, e.Value);
                    }
                }

                // save table row
                key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
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
                if (enterLock)
                {
                    TableLockExit(tableName, primaryKeyValue);
                }
            }
        }

        public async Task<bool> TableRowDeleteAsync(string tableName, string primaryKeyValue)
        {
            try
            {
                await TableLockEnterAsync(tableName, primaryKeyValue);

                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                // 지우기 전에 전체값을 읽는다. 인덱스를 지우기 위함이다.
                var key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                var ret = await db.HashGetAllAsync(key);
                if (null == ret)
                {
                    return false;
                }

                var fvdic = new Dictionary<string, string>();
                foreach (var e in ret)
                {
                    fvdic.Add(e.Name.ToString(), e.Value.ToString());
                }

                // 인덱스 삭제
                foreach (var fieldIndex in ts.matchIndexColumnDic.Values)
                {
                    key = GetRedisKey_TableMatchIndexColumn(ts.tableID, fieldIndex, fvdic[fieldIndex.ToString()]);
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // sortedset 삭제
                foreach (var e in ts.rangeIndexColumnDic)
                {
                    key = GetRedisKey_TableRangeIndexColumn(ts.tableID, e.Value);
                    tasklist.Add(db.SortedSetRemoveAsync(key, primaryKeyValue));
                }

                // 테이블 로우 아이템 삭제
                key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
                tasklist.Add(db.KeyDeleteAsync(key));

                key = GetRedisKey_TablePrimaryKeyList(tableName);
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
                TableLockExit(tableName, primaryKeyValue);
            }
        }

        // primaryKeyValue하고 일치하는 테이블 row 1개를 선택한다.
        // selectColumnNames : 선택할 column name list. 만약 null이면 모든 field를 선택한다.
        public async Task<Dictionary<string, string>> TableRowSelectByPrimaryKeyColumnValueAsync(List<string> selectColumnNames, string tableName, string primaryKeyValue)
        {
            var retdic = new Dictionary<string, string>();
            var ts = await TableGetSettingAsync(tableName);
            var key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
            var db = this.redis.GetDatabase();

            if (null == selectColumnNames)
            {
                // selectFields가 null이면 모든 필드를 읽는다.
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
                // selectColumnNames 존재하면 해당 컬럼만 읽는다.
                var len = selectColumnNames.Count;
                RedisValue[] rv = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    ColumnSetting cs;
                    if (ts.tableSchemaDic.TryGetValue(selectColumnNames[i], out cs))
                    {
                        rv[i] = cs.indexNumber.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 Column
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' column", tableName, selectColumnNames[i]));
                    }
                }
                var ret = await db.HashGetAsync(key, rv);
                if (null != ret)
                {
                    for (var i = 0; i < len; i++)
                    {
                        retdic.Add(selectColumnNames[i], ret[i].ToString());
                    }
                }
            }

            return retdic;
        }

        // 인덱스된 컬럼 값이 일치하는 모든 테이블 row를 선택한다.
        public async Task<List<Dictionary<string, string>>> TableRowSelectByMatchIndexFieldValueAsync(List<string> selectColumnNames, string tableName, string compareIndexColumnName, string columnValue)
        {
            var retlist = new List<Dictionary<string, string>>();
            var ts = await TableGetSettingAsync(tableName);
            var db = this.redis.GetDatabase();

            ColumnSetting cs;
            if (!ts.tableSchemaDic.TryGetValue(compareIndexColumnName, out cs))
            {
                return retlist;
            }

            var key = GetRedisKey_TableMatchIndexColumn(ts.tableID, cs.indexNumber, columnValue);
            var pkvs = await db.SetMembersAsync(key);

            if (null == selectColumnNames)
            {
                // selectColumnNames가 null이면 모든 column을 읽는다.
                var tasklist = new List<Task<HashEntry[]>>();
                foreach (var pk in pkvs)
                {
                    key = GetRedisKey_TableRow(ts.tableID, pk.ToString());
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
                // selectColumnNames가 null이 아니면 해당 column만 읽는다.
                var len = selectColumnNames.Count;
                var rva = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    if (ts.tableSchemaDic.TryGetValue(selectColumnNames[i], out cs))
                    {
                        rva[i] = cs.indexNumber.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 field
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' field", tableName, selectColumnNames[i]));
                    }
                }

                var tasklist = new List<Task<RedisValue[]>>();
                foreach (var pk in pkvs)
                {
                    key = GetRedisKey_TableRow(ts.tableID, pk.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                foreach (var task in tasklist)
                {
                    await task;
                    rva = task.Result;
                    var dic = new Dictionary<string, string>();

                    for (var i = 0; i < len; i++)
                    {
                        dic.Add(selectColumnNames[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist;
        }

        // sort된 field값이 lowValue와 highValue 사이에 있는 모든 row를 구한다.
        public async Task<List<Dictionary<string, string>>> TableRowSelectByRangeIndexAsync(List<string> selectColumnNames, string tableName, string compareRangeIndexColumnName, string lowValue, string highValue)
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

            var key = GetRedisKey_TableRangeIndexColumn(ts.tableID, cs.indexNumber);
            var primaryKeyValues = await db.SortedSetRangeByScoreAsync(key, lv, hv);

            if (null == selectColumnNames)
            {
                // selectColumnNames가 null이면 모든 column을 읽는다.
                List<Task<HashEntry[]>> tasklist = new List<Task<HashEntry[]>>();
                foreach (var primaryKeyValue in primaryKeyValues)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
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
                // selectColumnNames가 null이 아니면 해당 column만 읽는다.
                var len = selectColumnNames.Count;
                var rva = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    if (ts.tableSchemaDic.TryGetValue(selectColumnNames[i], out cs))
                    {
                        rva[i] = cs.indexNumber.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 field
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' field", tableName, selectColumnNames[i]));
                    }
                }

                var tasklist = new List<Task<RedisValue[]>>();
                foreach (var primaryKeyValue in primaryKeyValues)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                foreach (var task in tasklist)
                {
                    await task;
                    rva = task.Result;
                    var dic = new Dictionary<string, string>();

                    for (var i = 0; i < len; i++)
                    {
                        dic.Add(selectColumnNames[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist;
        }
    }
}
