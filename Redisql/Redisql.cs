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
        public Type fieldType;
        public bool fieldIndexFlag;
        public bool fieldSortFlag;
        public object fieldDefaultValue;
    }

    public class TableSetting
    {
        public int tableID;
        public string primaryKeyFieldName;
        public Dictionary<string, FieldSetting> tableSchemaDic = new Dictionary<string, FieldSetting>();
        public Dictionary<string, Int32> indexedFieldDic = new Dictionary<string, int>();
        public Dictionary<string, Int32> sortedFieldDic = new Dictionary<string, int>();
        public Dictionary<string, string> fieldIndexNameDic = new Dictionary<string, string>();
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
                connString = string.Format("{0}:{1}", redisIp, redisPort.ToString());
            }
            else
            {

            }

            this.redis = ConnectionMultiplexer.Connect(connString);
            Console.WriteLine("Redis Connected");
        }

        private string GetTableSchemaRedisKey(string tableName)
        {
            return string.Format("C:{0}", tableName);
        }

        private string GetTableFieldIndexRedisKey(Int32 tableID, Int32 fieldIndex, string value)
        {
            return string.Format("I:{0}:{1}:{2}", tableID.ToString(), fieldIndex.ToString(), value);
        }

        private string GetTableFieldSortedSetIndexRedisKey(Int32 tableID, Int32 fieldIndex)
        {
            return string.Format("S:{0}:{1}", tableID.ToString(), fieldIndex.ToString());
        }

        private string GetTableRowRedisKey(Int32 tableID, string primaryKeyValue)
        {
            return string.Format("W:{0}:{1}", tableID.ToString(), primaryKeyValue);
        }

        private string GetTablePrimaryKeyListRedisKey(string tableName)
        {
            return string.Format("K:{0}", tableName);
        }

        private string GetTableLockRedisKey(string tableName, string primaryKeyValue)
        {
            return string.Format("L:{0}:{1}", tableName, primaryKeyValue);
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

                ts.tableID = Convert.ToInt32(tableID.ToString());

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

                    switch (tokens[1])
                    {
                        case "System.Byte": fs.fieldType = typeof(Byte); break;
                        case "System.Int16": fs.fieldType = typeof(Int16); break;
                        case "System.UInt16": fs.fieldType = typeof(UInt16);break;
                        case "System.Int32": fs.fieldType = typeof(Int32); break;
                        case "System.UInt32": fs.fieldType = typeof(UInt32); break;
                        case "System.Int64": fs.fieldType = typeof(Int64);  break;
                        case "System.UInt64": fs.fieldType = typeof(UInt64); break;
                        case "System.Single": fs.fieldType = typeof(Single); break;
                        case "System.Double": fs.fieldType = typeof(Double); break;
                        case "System.String": fs.fieldType = typeof(String); break;
                        case "System.DateTime": fs.fieldType = typeof(DateTime); break;
                    }

                    if (tokens[5].Equals("null"))
                    {
                        fs.fieldDefaultValue = null;
                    }
                    else
                    {
                        switch (tokens[1])
                        {
                            case "System.Byte": fs.fieldDefaultValue = Convert.ToByte(tokens[5]); break;
                            case "System.Int16": fs.fieldDefaultValue = Convert.ToInt16(tokens[5]); break;
                            case "System.UInt16": fs.fieldDefaultValue = Convert.ToUInt16(tokens[5]); break;
                            case "System.Int32": fs.fieldDefaultValue = Convert.ToInt32(tokens[5]); break;
                            case "System.UInt32": fs.fieldDefaultValue = Convert.ToUInt32(tokens[5]); break;
                            case "System.Int64": fs.fieldDefaultValue = Convert.ToInt64(tokens[5]); break;
                            case "System.UInt64": fs.fieldDefaultValue = Convert.ToUInt64(tokens[5]); break;
                            case "System.Single": fs.fieldDefaultValue = Convert.ToSingle(tokens[5]); break;
                            case "System.Double": fs.fieldDefaultValue = Convert.ToDouble(tokens[5]); break;
                            case "System.String": fs.fieldDefaultValue = Convert.ToString(tokens[5]); break;
                            case "System.DateTime":
                                if (tokens[5].ToLower().Equals("now"))
                                {
                                    fs.fieldDefaultValue = "now";
                                }
                                else if (tokens[5].ToLower().Equals("utcnow"))
                                {
                                    fs.fieldDefaultValue = "utcnow";
                                }
                                else
                                {
                                    fs.fieldDefaultValue = Convert.ToDateTime(tokens[5]);
                                }
                                break;
                        }
                    }
                    
                    fs.fieldIndexFlag = Convert.ToBoolean(tokens[2]);
                    if (fs.fieldIndexFlag)
                    {
                        ts.indexedFieldDic.Add(e.Name.ToString(), fs.fieldIndex);
                    }

                    var fieldPrimaryKeyFlag = Convert.ToBoolean(tokens[3]);
                    if (fieldPrimaryKeyFlag)
                    {
                        ts.primaryKeyFieldName = e.Name;
                    }

                    fs.fieldSortFlag = Convert.ToBoolean(tokens[4]);
                    if (fs.fieldSortFlag)
                    {
                        ts.sortedFieldDic.Add(e.Name.ToString(), fs.fieldIndex);
                    }

                    ts.fieldIndexNameDic.Add(fs.fieldIndex.ToString(), e.Name.ToString());
                    ts.tableSchemaDic.Add(e.Name.ToString(), fs);
                }

                this.tableSettingDic.TryAdd(tableName, ts);
            }

            return ts;
        }

        // List<Tuple<string,Type,bool,bool,object>> fieldList : fieldName, fieldType, IndexFlag, sortFlag, defaultValue
        public async Task<bool> CreateTable(string tableName, string primaryKeyFieldName, List<Tuple<string, Type, bool, bool, object>> fieldInfoList)
        {
            bool enterTableLock = false;
            try
            {
                // check input parameters 
                foreach (var tpl in fieldInfoList)
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
                            case "System.DateTime": // these types could be sorted
                                break;

                            default: // other types cannot be sorted
                                return false;
                        }
                    }
                }

                // 모든 테이블은 기본적으로 _id field가 추가되고 이 필드는 자동 증가값을 갖는다.
                fieldInfoList.Insert(0, new Tuple<string, Type, bool, bool, object>("_id", typeof(Int64), false, false, null));

                enterTableLock = true;
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
                    bool sortFlag = t.Item4;
                    object defaultValue = t.Item5;
                    if (defaultValue == null)
                    {
                        defaultValue = "null";
                    }

                    if (t.Item1.Equals(primaryKeyFieldName))
                    {
                        pkFlag = true;
                        indexFlag = false;
                    }

                    var value = string.Format("{0},{1},{2},{3},{4},{5}", (fieldIndex++).ToString(), t.Item2.ToString(), indexFlag.ToString(), pkFlag.ToString(), sortFlag.ToString(), defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
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
                    LeaveTableLock(tableName, "");
                }
            }
        }

        private Double ConvertToScore(Type type, string value)
        {
            switch (type.ToString())
            {
                case "System.Byte":
                case "System.Int16":
                case "System.UInt16":
                case "System.Int32":
                case "System.UInt32":
                case "System.Single":
                case "System.Double":
                    return Convert.ToDouble(value);

                case "System.DateTime":
                    return (DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

                default: 
                    return 0.0f;
            }
        }

        // 테이블 row를 추가하고 추가된 row의 자동 증가 _id값을 얻는다.
        public async Task<Int64> InsertTableRow(string tableName, Dictionary<string, string> fieldValues)
        {
            string key;
            string primaryKeyValue = null;

            try
            {
                var db = this.redis.GetDatabase();

                var ts = await GetTableSetting(tableName);
                if (null == ts)
                {
                    return -1;
                }

                if (!ts.primaryKeyFieldName.Equals("_id"))
                {
                    // Insert하려는 row의 primary key field가 이미 존재하면 insert는 할수 없음
                    // get primaryKey value of insert row
                    if (!fieldValues.TryGetValue(ts.primaryKeyFieldName, out primaryKeyValue))
                    {
                        return -1;
                    }

                    // check if row already exists
                    key = GetTableRowRedisKey(ts.tableID, primaryKeyValue);
                    var ret = await db.KeyExistsAsync(key);
                    if (ret)
                    {
                        return -1;
                    }
                }
                
                HashEntry[] heArray = new HashEntry[ts.tableSchemaDic.Count]; 

                // 자동 증가값 _id의 값을 설정한다.
                var idValue = await db.HashIncrementAsync(Consts.RedisKey_Hash_TableAutoIncrementFieldValues, ts.tableID.ToString());
                heArray[0] = new HashEntry(0, idValue);

                if (ts.primaryKeyFieldName.Equals("_id"))
                {
                    primaryKeyValue = idValue.ToString();
                }

                
                int arrayIndex = 1;
                List<Task> tasklist = new List<Task>();
                
                foreach (var e in ts.tableSchemaDic)
                {
                    var fs = e.Value;
                    if (!e.Key.Equals("_id")) // _id field는 사용자가 값을 할당할 수 없다.
                    {
                        string value;
                        if (fieldValues.TryGetValue(e.Key, out value))
                        {
                            heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, value);
                        }
                        else
                        {
                            // apply default value
                            if (null == fs.fieldDefaultValue)
                            {
                                // This field has no default value
                                return -1;
                            }
                            else
                            {
                                switch (fs.fieldType.ToString())
                                {
                                    case "System.Byte": 
                                    case "System.Int16":
                                    case "System.UInt16":
                                    case "System.Int32":
                                    case "System.UInt32":
                                    case "System.Single":
                                    case "System.Double":
                                        heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, fs.fieldDefaultValue.ToString());
                                        break;

                                    case "System.DateTime":
                                        if (fs.fieldDefaultValue.Equals("now"))
                                        {
                                            heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, DateTime.Now.ToString());
                                        }
                                        else if (fs.fieldDefaultValue.Equals("utcnow"))
                                        {
                                            heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, DateTime.UtcNow.ToString());
                                        }
                                        else
                                        {
                                            heArray[arrayIndex++] = new HashEntry(fs.fieldIndex, fs.fieldDefaultValue.ToString());
                                        }
                                        break;
                                }
                            }
                        }

                        if (e.Value.fieldIndexFlag)
                        {
                            // make index
                            key = GetTableFieldIndexRedisKey(ts.tableID, e.Value.fieldIndex, value);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (e.Value.fieldSortFlag)
                        {
                            // sorted set index
                            key = GetTableFieldSortedSetIndexRedisKey(ts.tableID, e.Value.fieldIndex);
                            var score = ConvertToScore(e.Value.fieldType, value);
                            tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                        }
                    }
                    else
                    {
                        // _id field
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

                return idValue;
            }
            catch (Exception ex)
            {
                return -1;
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

                // 업데이트 하려는 row의 값중 인덱스, Sorted된 값을 찾는다. 이 값들은 읽어서 갱신해야 한다.
                var updatedFields = new Dictionary<string, Tuple<Int32, string>>(); // fieldName, Tuple<fieldIndex, updatedValue>

                foreach (var e in ts.indexedFieldDic)
                {
                    string value;
                    if (updateFieldValues.TryGetValue(e.Key, out value))
                    {
                        // update 하려는 값중에 인덱스가 걸려있는 값이 있다. 인덱스를 갱신해야 한다.
                        updatedFields.Add(e.Key, new Tuple<Int32, string>(e.Value, value));
                    }
                }

                foreach (var e in ts.sortedFieldDic)
                {
                    string value;
                    if (updateFieldValues.TryGetValue(e.Key, out value))
                    {
                        if (!updatedFields.ContainsKey(e.Key))
                        {
                            // 인덱스에서 추가된 값과 중복되지 않는 경우에만 추가 
                            updatedFields.Add(e.Key, new Tuple<Int32, string>(e.Value, value));
                        }
                    }
                }

                enterLock = true;
                await EnterTableLock(tableName, primaryKeyValue);

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

                    key = GetTableRowRedisKey(ts.tableID, primaryKeyValue); 
                    var ret = await db.HashGetAsync(key, rvArray);
                   
                    index = 0;
                    foreach (var e in updatedFields)
                    {
                        if (ts.indexedFieldDic.ContainsKey(e.Key))
                        {
                            // 원래 값으로 저장되어 있던 인덱스를 지우고 새 값으로 갱신
                            key = GetTableFieldIndexRedisKey(ts.tableID, e.Value.Item1, ret[index].ToString());
                            tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                            key = GetTableFieldIndexRedisKey(ts.tableID, e.Value.Item1, e.Value.Item2);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (ts.sortedFieldDic.ContainsKey(e.Key))
                        {
                            // SortedSet의 Score를 갱신
                            key = GetTableFieldSortedSetIndexRedisKey(ts.tableID, e.Value.Item1);
                            var score = ConvertToScore(ts.tableSchemaDic[e.Key].fieldType, e.Value.Item2);
                            tasklist.Add(db.SortedSetAddAsync(key, primaryKeyValue, score));
                        }

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
                    key = GetTableFieldIndexRedisKey(ts.tableID, fieldIndex, ret[Convert.ToInt32(fieldIndex)].Value.ToString()); 
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // sortedset 삭제
                foreach (var e in ts.sortedFieldDic)
                {
                    key = GetTableFieldSortedSetIndexRedisKey(ts.tableID, e.Value);
                    tasklist.Add(db.SortedSetRemoveAsync(key, primaryKeyValue));
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

        // primaryKeyValue하고 일치하는 테이블 row 1개를 선택한다.
        // selectFields : 선택할 field name list. 만약 null이면 모든 field를 선택한다.
        public async Task<Dictionary<string, string>> SelectTableRowByPrimaryKeyField(List<string> selectFields, string tableName, string primaryKeyValue)
        {
            var retdic = new Dictionary<string, string>();
            var ts = await GetTableSetting(tableName);
            var key = GetTableRowRedisKey(ts.tableID, primaryKeyValue);
            var db = this.redis.GetDatabase();

            if (null == selectFields)
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
                        if (ts.fieldIndexNameDic.TryGetValue(e.Name.ToString(), out tableFieldName))
                        {
                            retdic.Add(tableFieldName, e.Value.ToString());
                        }
                    }
                }
            }
            else
            {
                // selectFields가 존재하면 해당 필드만 읽는다.
                var len = selectFields.Count;
                RedisValue[] rv = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    FieldSetting fs;
                    if (ts.tableSchemaDic.TryGetValue(selectFields[i], out fs))
                    {
                        rv[i] = fs.fieldIndex.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 field
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' field", tableName, selectFields[i]));
                    }
                }
                var ret = await db.HashGetAsync(key, rv);
                if (null != ret)
                {
                    for (var i = 0; i < len; i++)
                    {
                        retdic.Add(selectFields[i], ret[i].ToString());
                    }
                }
            }

            return retdic;
        }

        // 인덱스된 필드에 값이 일치하는 모든 테이블 row를 선택한다.
        public async Task<List<Dictionary<string,string>>> SelectTableRowByIndexedField(List<string> selectFields, string tableName, string fieldName, string value)
        {
            var retlist = new List<Dictionary<string, string>>();
            var ts = await GetTableSetting(tableName);
            var db = this.redis.GetDatabase();

            FieldSetting fs;
            if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
            {
                return retlist;
            }
            
            var key = GetTableFieldIndexRedisKey(ts.tableID, fs.fieldIndex, value);
            var pkvs = await db.SetMembersAsync(key);
            
            if (null == selectFields)
            {
                // selectedFields가 null이면 모든 field를 읽는다.
                var tasklist = new List<Task<HashEntry[]>>();
                foreach (var pk in pkvs)
                {
                    key = GetTableRowRedisKey(ts.tableID, pk.ToString());
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
                        if (ts.fieldIndexNameDic.TryGetValue(he.Name.ToString(), out tableFieldName))
                        {
                            dic.Add(tableFieldName, he.Value.ToString());
                        }
                    }
                    retlist.Add(dic);
                }
            }
            else
            {
                // selectField가 null이 아니면 해당 field만 읽는다.
                var len = selectFields.Count;
                var rva = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    if (ts.tableSchemaDic.TryGetValue(selectFields[i], out fs))
                    {
                        rva[i] = fs.fieldIndex.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 field
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' field", tableName, selectFields[i]));
                    }
                }

                var tasklist = new List<Task<RedisValue[]>>();
                foreach (var pk in pkvs)
                {
                    key = GetTableRowRedisKey(ts.tableID, pk.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                foreach (var task in tasklist)
                {
                    await task;
                    rva = task.Result;
                    var dic = new Dictionary<string, string>();

                    for (var i = 0; i < len; i++)
                    {
                        dic.Add(selectFields[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist;
        }

        // sort된 field값이 lowValue와 highValue 사이에 있는 모든 row를 구한다.
        public async Task<List<Dictionary<string, string>>> SelectTableRowBySortedFieldRange(List<string> selectFields, string tableName, string fieldName, string lowValue, string highValue)
        {
            var retlist = new List<Dictionary<string, string>>();
            var ts = await GetTableSetting(tableName);
            var db = this.redis.GetDatabase();

            FieldSetting fs;
            if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
            {
                return retlist;
            }

            var lv = ConvertToScore(fs.fieldType, lowValue);
            var hv = ConvertToScore(fs.fieldType, highValue);

            var key = GetTableFieldSortedSetIndexRedisKey(ts.tableID, fs.fieldIndex);
            var primaryKeyValues = await db.SortedSetRangeByScoreAsync(key, lv, hv);

            if (null == selectFields)
            {
                // selectField가 null이면 모든 field를 읽는다.
                List<Task<HashEntry[]>> tasklist = new List<Task<HashEntry[]>>();
                foreach (var primaryKeyValue in primaryKeyValues)
                {
                    key = GetTableRowRedisKey(ts.tableID, primaryKeyValue.ToString());
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
                        if (ts.fieldIndexNameDic.TryGetValue(he.Name.ToString(), out tableFieldName))
                        {
                            dic.Add(tableFieldName, he.Value.ToString());
                        }
                    }
                    retlist.Add(dic);
                }
            }
            else
            {
                // selectField가 null이 아니면 해당 field만 읽는다.
                var len = selectFields.Count;
                var rva = new RedisValue[len];
                for (var i = 0; i < len; i++)
                {
                    if (ts.tableSchemaDic.TryGetValue(selectFields[i], out fs))
                    {
                        rva[i] = fs.fieldIndex.ToString();
                    }
                    else
                    {
                        // 존재하지 않는 field
                        throw new Exception(string.Format("Table '{0}' does not have '{1}' field", tableName, selectFields[i]));
                    }
                }

                var tasklist = new List<Task<RedisValue[]>>();
                foreach (var primaryKeyValue in primaryKeyValues)
                {
                    key = GetTableRowRedisKey(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                foreach (var task in tasklist)
                {
                    await task;
                    rva = task.Result;
                    var dic = new Dictionary<string, string>();

                    for (var i = 0; i < len; i++)
                    {
                        dic.Add(selectFields[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist; 
        }
    }
        
}
