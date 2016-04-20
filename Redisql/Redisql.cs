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
        Int32 fieldIndex;
        Type fieldType;
        bool fieldIndexFlag;
        bool fieldPrimaryKeyFlag;
    }

    public class TableSetting
    {
        int tableID;
        Dictionary<string, FieldSetting> tableSchemaDic = new Dictionary<string, FieldSetting>();
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

        private string GetTableFieldIndexRedisKey(string tableID, string fieldIndex, string value)
        {
            return string.Format("SE:TFX:{0}:{1}:{2}", tableID, fieldIndex, value);
        }

        private string GetTableRowRedisKey(string tableID, string primaryKeyValue)
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

        private void GetTableIdAndSchema(string tableName)
        {

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

                    var value = string.Format("{0},{1},{2},{3}", fieldIndex++, t.Item2.ToString(), Convert.ToInt32(indexFlag), Convert.ToInt32(pkFlag)); // fieldIndex, Type, IndexFlag, primaryKeyFlag
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

                // get table id
                var tableID = await db.HashGetAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (RedisValue.Null == tableID)
                {
                    return false;
                }

                // read table schema
                var tableSchemaName = GetTableSchemaRedisKey(tableName); //string.Format("HA:TSM:{0}", tableName);
                var tableSchema = await db.HashGetAllAsync(tableSchemaName);
                if (null == tableSchema)
                {
                    return false;
                }

                // get table info
                string tablePrimaryKeyFieldName = null;
                var tableSchemaDic = new Dictionary<string, Tuple<string, string, string, string>>();
                foreach (var e in tableSchema)
                {
                    var tokens = e.Value.ToString().Split(',');
                    var fieldIndex = tokens[0];
                    var fieldType = tokens[1];
                    var fieldIndexFlag = tokens[2];
                    var fieldPrimaryKeyFlag = tokens[3];
                    if (fieldPrimaryKeyFlag.Equals("1"))
                    {
                        tablePrimaryKeyFieldName = e.Name;
                    }

                    tableSchemaDic.Add(e.Name, new Tuple<string, string, string, string>(fieldIndex, fieldType, fieldIndexFlag, fieldPrimaryKeyFlag));
                }

                // get primaryKey value of insert row
                if (!fieldValues.TryGetValue(tablePrimaryKeyFieldName, out primaryKeyValue))
                    return false;

                enterLock = true;
                await EnterTableLock(tableName, primaryKeyValue);

                string key;
                int arrayIndex = 0;
                List<Task> tasklist = new List<Task>();
                HashEntry[] heArray = new HashEntry[fieldValues.Count];
                foreach (var e in tableSchemaDic)
                {
                    string value;
                    if (fieldValues.TryGetValue(e.Key, out value))
                    {
                        var fieldIndex = e.Value.Item1;
                        var fieldType = e.Value.Item2;
                        var fieldIndexFlag = e.Value.Item3;
                        var fieldPrimaryKeyFlag = e.Value.Item4;

                        heArray[arrayIndex++] = new HashEntry(fieldIndex, value);

                        if (fieldIndexFlag.Equals("1"))
                        {
                            // make index
                            key = GetTableFieldIndexRedisKey(tableID, fieldIndex, value); //string.Format("SE:TIX:{0}:{1}:{2}", tableID, fieldIndex, value);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }
                    }
                    else
                    {
                        return false;
                    }
                }

                // save table row
                key = GetTableRowRedisKey(tableID, primaryKeyValue); //string.Format("HA:TIM:{0}:{1}", tableID, primaryKeyValue);
                tasklist.Add(db.HashSetAsync(key, heArray));

                // save table primary key 
                key = GetTablePrimaryKeyListRedisKey(tableName); //string.Format("SE:TPK:{0}", tableName);
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

                // get table id
                var tableID = await db.HashGetAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (RedisValue.Null == tableID)
                {
                    return false;
                }

                // read table schema
                var tableSchemaName = GetTableSchemaRedisKey(tableName); //string.Format("HA:TSM:{0}", tableName);
                var tableSchema = await db.HashGetAllAsync(tableSchemaName);
                if (null == tableSchema)
                {
                    return false;
                }

                // get table info
                var indexedFieldSet = new HashSet<Tuple<string, string>>(); // fieldName, fieldIndex
                string tablePrimaryKeyFieldName = null;
                var tableSchemaDic = new Dictionary<string, Tuple<string, string, string, string>>();
                foreach (var e in tableSchema)
                {
                    var tokens = e.Value.ToString().Split(',');
                    var fieldIndex = tokens[0];
                    var fieldType = tokens[1];
                    var fieldIndexFlag = tokens[2];
                    var fieldPrimaryKeyFlag = tokens[3];

                    if (fieldIndexFlag.Equals("1"))
                    {
                        indexedFieldSet.Add(new Tuple<string, string>(e.Name, fieldIndex));
                    }

                    if (fieldPrimaryKeyFlag.Equals("1"))
                    {
                        tablePrimaryKeyFieldName = e.Name;
                    }

                    tableSchemaDic.Add(e.Name, new Tuple<string, string, string, string>(fieldIndex, fieldType, fieldIndexFlag, fieldPrimaryKeyFlag));
                }

                // 업데이트하려는 row의 primaryKey value를 얻는다.
                if (!updateFieldValues.TryGetValue(tablePrimaryKeyFieldName, out primaryKeyValue))
                {
                    // 업데이트하려는 값 정보에 PrimaryKey value가 없다. 
                    return false;
                }

                // 업데이트 하려는 row의 값중 인덱스에 해당하는 값을 찾는다. 이 값들은 읽어서 갱신해야 한다.

                var updatedIndexFields = new HashSet<Tuple<string, string, string>>(); // fieldName, fieldIndex, updatedValue
                                                                                       // 이미 저장되어 있는 값중 인덱스값을 읽는다.
                foreach (var tpl in indexedFieldSet)
                {
                    string value;
                    if (updateFieldValues.TryGetValue(tpl.Item1, out value))
                    {
                        // update 하려는 값중에 인덱스가 걸려있는 값이 있다. 인덱스를 갱신해야 한다.
                        updatedIndexFields.Add(new Tuple<string, string, string>(tpl.Item1, tpl.Item2, value));
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

                    key = GetTableRowRedisKey(tableID, primaryKeyValue); //string.Format("HA:TIM:{0}:{1}", tableID, primaryKeyValue);
                    var ret = await db.HashGetAsync(key, rvArray);

                    // 원래 값으로 저장되어 있던 인덱스를 지우고 새 값으로 갱신
                    index = 0;
                    foreach (var tpl in updatedIndexFields)
                    {
                        key = GetTableFieldIndexRedisKey(tableID, tpl.Item2, ret[index].ToString()); //string.Format("SE:TIX:{0}:{1}:{2}", tableID, tpl.Item2, ret[index].ToString());
                        tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                        key = GetTableFieldIndexRedisKey(tableID, tpl.Item2, tpl.Item3); //string.Format("SE:TIX:{0}:{1}:{2}", tableID, tpl.Item2, tpl.Item3);
                        tasklist.Add(db.SetAddAsync(key, primaryKeyValue));

                        index++;
                    }
                }

                int arrayIndex = 0;
                HashEntry[] heArray = new HashEntry[updateFieldValues.Count];
                foreach (var e in updateFieldValues)
                {
                    Tuple<string, string, string, string> tpl;
                    if (tableSchemaDic.TryGetValue(e.Key, out tpl))
                    {
                        heArray[arrayIndex++] = new HashEntry(tpl.Item1, e.Value);
                    }
                }

                // save table row
                key = GetTableRowRedisKey(tableID, primaryKeyValue); //string.Format("HA:TIM:{0}:{1}", tableID, primaryKeyValue);
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

                // get table id
                var tableID = await db.HashGetAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (RedisValue.Null == tableID)
                {
                    return false;
                }

                // read table schema
                var tableSchemaName = GetTableSchemaRedisKey(tableName); //string.Format("HA:TSM:{0}", tableName);
                var tableSchema = await db.HashGetAllAsync(tableSchemaName);
                if (null == tableSchema)
                {
                    return false;
                }

                // get table info
                var indexedFieldSet = new HashSet<Tuple<string, string>>(); // fieldName, fieldIndex
                string tablePrimaryKeyFieldName = null;
                var tableSchemaDic = new Dictionary<string, Tuple<string, string, string, string>>();
                foreach (var e in tableSchema)
                {
                    var tokens = e.Value.ToString().Split(',');
                    var fieldIndex = tokens[0];
                    var fieldType = tokens[1];
                    var fieldIndexFlag = tokens[2];
                    var fieldPrimaryKeyFlag = tokens[3];

                    if (fieldIndexFlag.Equals("1"))
                    {
                        indexedFieldSet.Add(new Tuple<string, string>(e.Name, fieldIndex));
                    }

                    if (fieldPrimaryKeyFlag.Equals("1"))
                    {
                        tablePrimaryKeyFieldName = e.Name;
                    }

                    tableSchemaDic.Add(e.Name, new Tuple<string, string, string, string>(fieldIndex, fieldType, fieldIndexFlag, fieldPrimaryKeyFlag));
                }

                // 지우기 전에 전체값을 읽는다. 인덱스를 지우기 위함이다.
                var key = GetTableRowRedisKey(tableID, primaryKeyValue); //string.Format("HA:TIM:{0}:{1}", tableID, primaryKeyValue);
                var ret = await db.HashGetAllAsync(key);
                if (null == ret)
                {
                    return false;
                }

                // 인덱스 삭제
                foreach (var tpl in indexedFieldSet)
                {
                    var fieldIndex = tpl.Item2;
                    key = GetTableFieldIndexRedisKey(tableID, fieldIndex, ret[Convert.ToInt32(fieldIndex)].Value); //string.Format("SE:TIX:{0}:{1}:{2}", tableID, fieldIndex, ret[Convert.ToInt32(fieldIndex)].Value);
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // 테이블 로우 아이템 삭제
                key = GetTableRowRedisKey(tableID, primaryKeyValue); //string.Format("HA:TIM:{0}:{1}", tableID, primaryKeyValue);
                tasklist.Add(db.KeyDeleteAsync(key));

                // remove table primary key 
                key = GetTablePrimaryKeyListRedisKey(tableName);  //string.Format("SE:TPK:{0}", tableName);
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
