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
    public class FieldSetting
    {
        public Int32 fieldIndex;
        public string fieldType;
        public bool fieldIndexFlag;
    }

    public class TableSetting
    {
        public int tableID;
        public string primaryKeyFieldName;
        public Dictionary<string, FieldSetting> tableSchemaDic = new Dictionary<string, FieldSetting>();
        public Dictionary<string, Int32> indexedFieldDic = new Dictionary<string, int>();
    }

    public class Redisql
    {
        ConcurrentDictionary<string, TableSetting> tableSettingDic = new ConcurrentDictionary<string, TableSetting>();
        ConnectionMultiplexer redis;

        public Redisql(string redisIp, Int32 redisPort, string redisPassword)
        {
            string connString = null;
            if (string.IsNullOrEmpty(redisPassword))
            {
                connString = string.Format("{0}:{1}", redisIp, redisPort);
            }
            else
            {

            }

            this.redis = ConnectionMultiplexer.Connect(connString);
            Console.WriteLine("Redis Connected");
        }

        private string GetTableSchemaRedisKey(string tableName)
        {
            return string.Format("HA:TSM:{0}", tableName);
        }

        private string GetTableFieldIndexRedisKey(Int32 tableID, Int32 fieldIndex, string value)
        {
            return string.Format("SE:TFX:{0}:{1}:{2}", tableID, fieldIndex, value);
        }

        private string GetTableRowRedisKey(Int32 tableID, string primaryKeyValue)
        {
            return string.Format("HA:TRW:{0}:{1}", tableID, primaryKeyValue);
        }

        private string GetTablePrimaryKeyListRedisKey(string tableName)
        {
            return string.Format("SE:TPK:{0}", tableName);
        }

        private string GetTableLockRedisKey(string tableName, string primaryKeyValue)
        {
            return string.Format("ST:TLK:{0}:{1}", tableName, primaryKeyValue);
        }

        private async Task<bool> EnterTableLock(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = GetTableLockRedisKey(tableName, primaryKeyValue);
            var ts = new TimeSpan(0, 0, 300);

            bool ret;
            do
            {
                ret = await db.StringSetAsync(key, Thread.CurrentThread.ManagedThreadId.ToString(), ts, When.NotExists);
                if (false == ret)
                {
                    await Task.Delay(1);
                }
            } while (false == ret);

            return true;
        }

        private void LeaveTableLock(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = GetTableLockRedisKey(tableName, primaryKeyValue);
            db.KeyDeleteAsync(key, CommandFlags.FireAndForget);
        }

        private async Task<TableSetting> GetTableSetting(string tableName)
        {
            TableSetting ts;
            var db = this.redis.GetDatabase();

            if (!this.tableSettingDic.TryGetValue(tableName, out ts))
            {
                // 아직 로드되지 않았다. redis로부터 읽어들인다.
                ts = new TableSetting();

                // get table id
                var tableID = await db.HashGetAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (RedisValue.Null == tableID)
                {
                    return null;
                }

                ts.tableID = Convert.ToInt32(tableID);

                // read table schema
                var tableSchema = await db.HashGetAllAsync(GetTableSchemaRedisKey(tableName));
                if (null == tableSchema)
                {
                    return null;
                }

                // get table info
                foreach (var e in tableSchema)
                {
                    var tokens = e.Value.ToString().Split(',');

                    var fs = new FieldSetting();
                    fs.fieldIndex = Convert.ToInt32(tokens[0]);
                    fs.fieldType = tokens[1];
                    fs.fieldIndexFlag = Convert.ToBoolean(tokens[2]);
                    if (fs.fieldIndexFlag)
                    {
                        ts.indexedFieldDic.Add(e.Name, fs.fieldIndex);
                    }

                    var fieldPrimaryKeyFlag = Convert.ToBoolean(tokens[3]);
                    if (fieldPrimaryKeyFlag)
                    {
                        ts.primaryKeyFieldName = e.Name;
                    }

                    ts.tableSchemaDic.Add(e.Name, fs);
                }

                this.tableSettingDic.TryAdd(tableName, ts);
            }

            return ts;
        }

        // List<Tuple<string,Type,bool>> fieldList : fieldName, fieldType, IndexFlag
        public async Task<bool> CreateTable(string tableName, string primaryKeyName, List<Tuple<string, Type, bool>> fieldInfoList)
        {
            try
            {
                await EnterTableLock(tableName, "");

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
                var tableSchemaName = GetTableSchemaRedisKey(tableName);
                int fieldIndex = 0;
                foreach (var t in fieldInfoList)
                {
                    bool pkFlag = false;
                    bool indexFlag = t.Item3;
                    if (t.Item1.Equals(primaryKeyName))
                    {
                        pkFlag = true;
                        indexFlag = false;
                    }

                    var value = string.Format("{0},{1},{2},{3}", fieldIndex++, t.Item2.ToString(), indexFlag.ToString(), pkFlag.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag
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
                LeaveTableLock(tableName, "");
            }
        }

        public async Task<bool> InsertTableRow(string tableName, Dictionary<string, string> fieldValues)
        {
            bool enterLock = false;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();

                var ts = await GetTableSetting(tableName);
                if (null == ts)
                {
                    return false;
                }

                // get primaryKey value of insert row
                if (!fieldValues.TryGetValue(ts.primaryKeyFieldName, out primaryKeyValue))
                    return false;

                enterLock = true;
                await EnterTableLock(tableName, primaryKeyValue);

                string key;
                int arrayIndex = 0;
                List<Task> tasklist = new List<Task>();
                HashEntry[] heArray = new HashEntry[fieldValues.Count];
                foreach (var e in ts.tableSchemaDic)
                {
                    string value;
                    if (fieldValues.TryGetValue(e.Key, out value))
                    {
                        var fieldIndex = e.Value.fieldIndex;
                        var fieldType = e.Value.fieldType;
                        var fieldIndexFlag = e.Value.fieldIndexFlag;
                        heArray[arrayIndex++] = new HashEntry(fieldIndex, value);

                        if (fieldIndexFlag)
                        {
                            // make index
                            key = GetTableFieldIndexRedisKey(ts.tableID, fieldIndex, value); 
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }
                    }
                    else
                    {
                        // apply default value?
                        return false;
                    }
                }

                // save table row
                key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
                tasklist.Add(db.HashSetAsync(key, heArray));

                // save table primary key 
                key = GetTablePrimaryKeyListRedisKey(tableName); 
                tasklist.Add(db.SetAddAsync(key, primaryKeyValue));

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
                    LeaveTableLock(tableName, primaryKeyValue);
                }
            }
        }

        public async Task<bool> UpdateTableRow(string tableName, Dictionary<string, string> updateFieldValues)
        {
            bool enterLock = false;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await GetTableSetting(tableName);
                if (null == ts)
                {
                    return false;
                }

                // 업데이트하려는 row의 primaryKey value를 얻는다.
                if (!updateFieldValues.TryGetValue(ts.primaryKeyFieldName, out primaryKeyValue))
                {
                    // 업데이트하려는 값 정보에 PrimaryKey value가 없다. 
                    return false;
                }

                // 업데이트 하려는 row의 값중 인덱스에 해당하는 값을 찾는다. 이 값들은 읽어서 갱신해야 한다.

                var updatedIndexFields = new HashSet<Tuple<string, Int32, string>>(); // fieldName, fieldIndex, updatedValue
                                                                                       // 이미 저장되어 있는 값중 인덱스값을 읽는다.
                foreach (var e in ts.indexedFieldDic)
                {
                    string value;
                    if (updateFieldValues.TryGetValue(e.Key, out value))
                    {
                        // update 하려는 값중에 인덱스가 걸려있는 값이 있다. 인덱스를 갱신해야 한다.
                        updatedIndexFields.Add(new Tuple<string, Int32, string>(e.Key, e.Value, value));
                    }
                }

                enterLock = true;
                await EnterTableLock(tableName, primaryKeyValue);

                string key;

                // 인덱스에 저장되어 있는 값을 가져온다.
                if (updatedIndexFields.Count > 0)
                {
                    int index = 0;
                    var rvArray = new RedisValue[updatedIndexFields.Count];
                    foreach (var tpl in updatedIndexFields)
                    {
                        rvArray[index++] = tpl.Item2;
                    }

                    key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
                    var ret = await db.HashGetAsync(key, rvArray);

                    // 원래 값으로 저장되어 있던 인덱스를 지우고 새 값으로 갱신
                    index = 0;
                    foreach (var tpl in updatedIndexFields)
                    {
                        key = GetTableFieldIndexRedisKey(ts.tableID, tpl.Item2, ret[index].ToString()); 
                        tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                        key = GetTableFieldIndexRedisKey(ts.tableID, tpl.Item2, tpl.Item3); 
                        tasklist.Add(db.SetAddAsync(key, primaryKeyValue));

                        index++;
                    }
                }

                int arrayIndex = 0;
                HashEntry[] heArray = new HashEntry[updateFieldValues.Count];
                foreach (var e in updateFieldValues)
                {
                    FieldSetting fs;
                    if (ts.tableSchemaDic.TryGetValue(e.Key, out fs))
                    {
                        heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, e.Value);
                    }
                }

                // save table row
                key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
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
                    LeaveTableLock(tableName, primaryKeyValue);
                }
            }
        }

        public async Task<bool> DeleteTableRow(string tableName, string primaryKeyValue)
        {
            try
            {
                await EnterTableLock(tableName, primaryKeyValue);

                var db = this.redis.GetDatabase();
                List<Task> tasklist = new List<Task>();

                var ts = await GetTableSetting(tableName);
                if (null == ts)
                {
                    return false;
                }

                // 지우기 전에 전체값을 읽는다. 인덱스를 지우기 위함이다.
                var key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
                var ret = await db.HashGetAllAsync(key);
                if (null == ret)
                {
                    return false;
                }

                // 인덱스 삭제
                foreach (var fieldIndex in ts.indexedFieldDic.Values)
                {
                    key = GetTableFieldIndexRedisKey(ts.tableID, fieldIndex, ret[Convert.ToInt32(fieldIndex)].Value); 
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // 테이블 로우 아이템 삭제
                key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
                tasklist.Add(db.KeyDeleteAsync(key));

                // remove table primary key 
                key = GetTablePrimaryKeyListRedisKey(tableName);  
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
                LeaveTableLock(tableName, primaryKeyValue);
            }
        }
    }
        
}
