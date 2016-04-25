﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;

using StackExchange.Redis;

namespace Redisql
{
    

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

        private string GetRedisKey_TableSchema(string tableName)
        {
            return string.Format("C:{0}", tableName);
        }

        private string GetRedisKey_TableMatchIndexField(Int32 tableID, Int32 fieldIndex, string value)
        {
            return string.Format("I:{0}:{1}:{2}", tableID.ToString(), fieldIndex.ToString(), value);
        }

        private string GetRedisKey_TableRangeIndexField(Int32 tableID, Int32 fieldIndex)
        {
            return string.Format("S:{0}:{1}", tableID.ToString(), fieldIndex.ToString());
        }

        private string GetRedisKey_TableRow(Int32 tableID, string primaryKeyValue)
        {
            return string.Format("W:{0}:{1}", tableID.ToString(), primaryKeyValue);
        }

        private string GetRedisKey_TablePrimaryKeyList(string tableName)
        {
            return string.Format("K:{0}", tableName);
        }

        private string GetRedisKey_TableLock(string tableName, string primaryKeyValue)
        {
            return string.Format("L:{0}:{1}", tableName, primaryKeyValue);
        }

        private async Task<bool> TableLockEnterAsync(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = GetRedisKey_TableLock(tableName, primaryKeyValue);
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

        private void TableLockExit(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = GetRedisKey_TableLock(tableName, primaryKeyValue);
            db.KeyDeleteAsync(key, CommandFlags.FireAndForget);
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

        private async Task<TableSetting> TableGetSettingAsync(string tableName)
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
                var tableSchema = await db.HashGetAllAsync(GetRedisKey_TableSchema(tableName));
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
                    
                    fs.fieldMatchIndexFlag = Convert.ToBoolean(tokens[2]);
                    if (fs.fieldMatchIndexFlag)
                    {
                        ts.matchIndexFieldDic.Add(e.Name.ToString(), fs.fieldIndex);
                    }

                    var fieldPrimaryKeyFlag = Convert.ToBoolean(tokens[3]);
                    if (fieldPrimaryKeyFlag)
                    {
                        ts.primaryKeyFieldName = e.Name;
                    }

                    fs.fieldRangeIndexFlag = Convert.ToBoolean(tokens[4]);
                    if (fs.fieldRangeIndexFlag)
                    {
                        ts.rangeIndexFieldDic.Add(e.Name.ToString(), fs.fieldIndex);
                    }

                    ts.fieldIndexNameDic.Add(fs.fieldIndex.ToString(), e.Name.ToString());
                    ts.tableSchemaDic.Add(e.Name.ToString(), fs);
                }

                this.tableSettingDic.TryAdd(tableName, ts);
            }

            return ts;
        }

        public async Task<bool> TableDeleteAsync(string tableName)
        {
            bool enterTableLock = false;
            try
            {
                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                // 테이블을 지울때는 테이블 데이터만 지우고 TableID, 자동 증가값은 지우지 않는다. 다시 생성될때를 대비하기 위함이다.
                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                var key = GetRedisKey_TablePrimaryKeyList(tableName);

                // 모든 Table row를 지워서 삭제한다.
                var tasklist = new List<Task<bool>>();
                var pkvs =  await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pkvs)
                {
                    tasklist.Add(TableDeleteRowAsync(tableName, primaryKeyValue.ToString()));
                }

                // 테이블 스키마 삭제
                key = GetRedisKey_TableSchema(tableName);
                tasklist.Add(db.KeyDeleteAsync(key));

                // 테이블 ID 해시 삭제
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableNameIds, tableName));

                // 테이블 자동 증가 값 해시 삭제
                tasklist.Add(db.HashDeleteAsync(Consts.RedisKey_Hash_TableAutoIncrementFieldValues, ts.tableID));

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

        // List<Tuple<string,Type,bool,bool,object>> fieldList : fieldName, fieldType, matchIndexFlag, rangeIndexFlag, defaultValue
        public async Task<bool> TableCreateAsync(string tableName, string primaryKeyFieldName, List<Tuple<string, Type, bool, bool, object>> fieldInfoList)
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
                            case "System.DateTime": // these types could be range indexed
                                break;

                            default: // other types cannot be range indexed
                                return false;
                        }
                    }
                }

                // 모든 테이블은 기본적으로 _id field가 추가되고 이 필드는 자동 증가값을 갖는다.
                fieldInfoList.Insert(0, new Tuple<string, Type, bool, bool, object>("_id", typeof(Int64), false, false, null));

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
                var tableSchemaName = GetRedisKey_TableSchema(tableName);
                int fieldIndex = 0;
                foreach (var t in fieldInfoList)
                {
                    bool pkFlag = false;
                    bool matchIndexFlag = t.Item3;
                    bool rangeIndexFlag = t.Item4;
                    object defaultValue = t.Item5;
                    if (defaultValue == null)
                    {
                        defaultValue = "null";
                    }

                    if (t.Item1.Equals(primaryKeyFieldName))
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

        public async Task<bool> TableEraseExistingFieldAsync(string tableName, string fieldName)
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

                if (ts.primaryKeyFieldName.Equals(fieldName))
                {
                    return false; // Can not delete PrimaryKey Field
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                FieldSetting fs;
                if (!ts.tableSchemaDic.TryGetValue(ts.primaryKeyFieldName, out fs))
                {
                    return false;
                }
                var primaryKeyFieldIndex = fs.fieldIndex;

                if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
                {
                    return false;
                }

                var tasklist = new List<Task<RedisValue[]>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);

                // field에 저장된 값을 먼저 읽는다.
                var rva = new RedisValue[2];
                rva[0] = primaryKeyFieldIndex;
                rva[1] = fs.fieldIndex;
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashGetAsync(key, rva));
                }

                var tasklist2 = new List<Task<bool>>();
                foreach (var t in tasklist)
                {
                    var ret = await t;
                    if (fs.fieldMatchIndexFlag)
                    {
                        key = GetRedisKey_TableMatchIndexField(ts.tableID, fs.fieldIndex, ret[1].ToString());
                        tasklist2.Add(db.SetRemoveAsync(key, ret[0].ToString()));
                    }

                    if (fs.fieldRangeIndexFlag)
                    {
                        key = GetRedisKey_TableRangeIndexField(ts.tableID, fs.fieldIndex);
                        tasklist2.Add(db.SortedSetRemoveAsync(key, ret[0].ToString()));
                    }

                    key = GetRedisKey_TableRow(ts.tableID, ret[0].ToString());
                    tasklist2.Add(db.HashDeleteAsync(key, fs.fieldIndex));
                }

                foreach (var t in tasklist2)
                {
                    if (!await t) return false;
                }

                // 데이터를 다 지웠으니 테이블 스키마 제거
                ts.tableSchemaDic.Remove(fieldName);
                ts.fieldIndexNameDic.Remove(fs.fieldIndex.ToString());

                Int32 v;
                if (ts.matchIndexFieldDic.TryGetValue(fieldName, out v))
                {
                    ts.matchIndexFieldDic.Remove(fieldName);
                }

                if (ts.rangeIndexFieldDic.TryGetValue(fieldName, out v))
                {
                    ts.rangeIndexFieldDic.Remove(fieldName);
                }

                key = GetRedisKey_TableSchema(tableName);
                await db.HashDeleteAsync(key, fieldName);

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

        public async Task<bool> TableAddNewFieldAsync(string tableName, string fieldName, Type fieldType, bool matchIndexFlag, bool rangeIndexFlag, object defaultValue)
        {
            bool enterTableLock = false;
            try
            {
                if (defaultValue == null)
                {
                    return false;
                }

                var db = this.redis.GetDatabase();

                var ts = await TableGetSettingAsync(tableName);
                if (null == ts)
                {
                    return false;
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                var fs = new FieldSetting();
                fs.fieldIndex = ts.GetNextFieldIndex();
                fs.fieldType = fieldType;
                fs.fieldMatchIndexFlag = matchIndexFlag;
                fs.fieldRangeIndexFlag = rangeIndexFlag;
                fs.fieldDefaultValue = defaultValue;

                if (fs.fieldType == typeof(DateTime))
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

                ts.tableSchemaDic.Add(fieldName, fs);

                if (matchIndexFlag)
                {
                    ts.matchIndexFieldDic.Add(fieldName, fs.fieldIndex);
                }

                if (rangeIndexFlag)
                {
                    ts.rangeIndexFieldDic.Add(fieldName, fs.fieldIndex);
                }

                ts.fieldIndexNameDic.Add(fs.fieldIndex.ToString(), fieldName);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);
                
                var value = string.Format("{0},{1},{2},{3},{4},{5}", fs.fieldIndex.ToString(), fs.fieldType.ToString(), matchIndexFlag.ToString(), "False", rangeIndexFlag.ToString(), defaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, fieldName, value);

                // 테이블 스키마 수정 완료되었으니 기존에 존재하는 테이블 아이템에 새로 추가된 field를 defaultValue로 모두 입력한다.
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    tasklist.Add(db.HashSetAsync(key, fs.fieldIndex.ToString(), defaultValue.ToString()));

                    if (matchIndexFlag)
                    {
                        // make match index
                        key = GetRedisKey_TableMatchIndexField(ts.tableID, fs.fieldIndex, defaultValue.ToString());
                        tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                    }

                    if (rangeIndexFlag)
                    {
                        // make range index
                        key = GetRedisKey_TableRangeIndexField(ts.tableID, fs.fieldIndex);
                        var score = ConvertToScore(fs.fieldType, defaultValue.ToString());
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }

        // Add match index to unindexed existing table field
        public async Task<bool> TableAddMatchIndexAsync(string tableName, string fieldName)
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

                if (ts.primaryKeyFieldName.Equals(fieldName))
                {
                    return false; // Can not add index to primary key field
                }

                FieldSetting fs;
                if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
                {
                    return false;
                }

                if (fs.fieldMatchIndexFlag)
                {
                    return false; // Already indexed field. No need to add index.
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                fs.fieldMatchIndexFlag = true;
                ts.matchIndexFieldDic.Add(fieldName, fs.fieldIndex);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", fs.fieldIndex.ToString(), fs.fieldType.ToString(), fs.fieldMatchIndexFlag.ToString(), "False", fs.fieldRangeIndexFlag.ToString(), fs.fieldDefaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, fieldName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, fs.fieldIndex);
                    
                    // add index
                    key = GetRedisKey_TableMatchIndexField(ts.tableID, fs.fieldIndex, v.ToString());
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }

        // Remove match index to indexed existing table field
        public async Task<bool> TableRemoveMatchIndexAsync(string tableName, string fieldName)
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

                if (ts.primaryKeyFieldName.Equals(fieldName))
                {
                    return false; // Can not remove index to primary key field
                }

                FieldSetting fs;
                if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
                {
                    return false;
                }

                if (!fs.fieldMatchIndexFlag)
                {
                    return false; // Not indexed field. Could not remove index.
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                fs.fieldMatchIndexFlag = false;
                ts.matchIndexFieldDic.Remove(fieldName);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", fs.fieldIndex.ToString(), fs.fieldType.ToString(), fs.fieldMatchIndexFlag.ToString(), "False", fs.fieldRangeIndexFlag.ToString(), fs.fieldDefaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, fieldName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, fs.fieldIndex);

                    // remove index
                    key = GetRedisKey_TableMatchIndexField(ts.tableID, fs.fieldIndex, v.ToString());
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
                {
                    TableLockExit(tableName, "");
                }
            }
        }

        // Add range index to not sort-indexed existing table field
        public async Task<bool> TableAddRangeIndexAsync(string tableName, string fieldName)
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

                FieldSetting fs;
                if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
                {
                    return false;
                }

                if (fs.fieldRangeIndexFlag)
                {
                    return false; // Already range indexed field. 
                }

                bool pkFlag = false;
                if (ts.primaryKeyFieldName.Equals(fieldName))
                {
                    pkFlag = true;
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                fs.fieldRangeIndexFlag = true;
                ts.rangeIndexFieldDic.Add(fieldName, fs.fieldIndex);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", fs.fieldIndex.ToString(), fs.fieldType.ToString(), fs.fieldMatchIndexFlag.ToString(), pkFlag.ToString(), fs.fieldRangeIndexFlag.ToString(), fs.fieldDefaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, fieldName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, fs.fieldIndex);

                    // add range index
                    key = GetRedisKey_TableRangeIndexField(ts.tableID, fs.fieldIndex);
                    var score = ConvertToScore(fs.fieldType, v.ToString());
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
        public async Task<bool> TableRemoveRangeIndexAsync(string tableName, string fieldName)
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

                FieldSetting fs;
                if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
                {
                    return false;
                }

                if (!fs.fieldRangeIndexFlag)
                {
                    return false; // Not range indexed field. 
                }

                bool pkFlag = false;
                if (ts.primaryKeyFieldName.Equals(fieldName))
                {
                    pkFlag = true;
                }

                enterTableLock = true;
                await TableLockEnterAsync(tableName, "");

                fs.fieldRangeIndexFlag = false;
                ts.rangeIndexFieldDic.Remove(fieldName);

                var tableSchemaName = GetRedisKey_TableSchema(tableName);

                var value = string.Format("{0},{1},{2},{3},{4},{5}", fs.fieldIndex.ToString(), fs.fieldType.ToString(), fs.fieldMatchIndexFlag.ToString(), pkFlag.ToString(), fs.fieldRangeIndexFlag.ToString(), fs.fieldDefaultValue.ToString()); // fieldIndex, Type, IndexFlag, primaryKeyFlag, sortFlag
                await db.HashSetAsync(tableSchemaName, fieldName, value);

                // 
                var tasklist = new List<Task<bool>>();
                var key = GetRedisKey_TablePrimaryKeyList(tableName);
                var pvks = await db.SetMembersAsync(key);
                foreach (var primaryKeyValue in pvks)
                {
                    key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue.ToString());
                    var v = await db.HashGetAsync(key, fs.fieldIndex);

                    // remove range index
                    key = GetRedisKey_TableRangeIndexField(ts.tableID, fs.fieldIndex);
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

        // 테이블 row를 추가하고 추가된 row의 자동 증가 _id값을 얻는다.
        public async Task<Int64> TableInsertRowAsync(string tableName, Dictionary<string, string> fieldValues)
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

                if (!ts.primaryKeyFieldName.Equals("_id"))
                {
                    // Insert하려는 row의 primary key field가 이미 존재하면 insert는 할수 없음
                    // get primaryKey value of insert row
                    if (!fieldValues.TryGetValue(ts.primaryKeyFieldName, out primaryKeyValue))
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

                        if (e.Value.fieldMatchIndexFlag)
                        {
                            // make index
                            key = GetRedisKey_TableMatchIndexField(ts.tableID, e.Value.fieldIndex, value);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (e.Value.fieldRangeIndexFlag)
                        {
                            // sorted set index
                            key = GetRedisKey_TableRangeIndexField(ts.tableID, e.Value.fieldIndex);
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

        public async Task<bool> TableUpdateRowAsync(string tableName, Dictionary<string, string> updateFieldValues)
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
                if (!updateFieldValues.TryGetValue(ts.primaryKeyFieldName, out primaryKeyValue))
                {
                    // 업데이트하려는 값 정보에 PrimaryKey value가 없다. 
                    return false;
                }

                // 업데이트 하려는 row의 값중 인덱스, Sorted된 값을 찾는다. 이 값들은 읽어서 갱신해야 한다.
                var updatedFields = new Dictionary<string, Tuple<Int32, string>>(); // fieldName, Tuple<fieldIndex, updatedValue>

                foreach (var e in ts.matchIndexFieldDic)
                {
                    string value;
                    if (updateFieldValues.TryGetValue(e.Key, out value))
                    {
                        // update 하려는 값중에 인덱스가 걸려있는 값이 있다. 인덱스를 갱신해야 한다.
                        updatedFields.Add(e.Key, new Tuple<Int32, string>(e.Value, value));
                    }
                }

                foreach (var e in ts.rangeIndexFieldDic)
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
                        if (ts.matchIndexFieldDic.ContainsKey(e.Key))
                        {
                            // 원래 값으로 저장되어 있던 인덱스를 지우고 새 값으로 갱신
                            key = GetRedisKey_TableMatchIndexField(ts.tableID, e.Value.Item1, ret[index].ToString());
                            tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));

                            key = GetRedisKey_TableMatchIndexField(ts.tableID, e.Value.Item1, e.Value.Item2);
                            tasklist.Add(db.SetAddAsync(key, primaryKeyValue));
                        }

                        if (ts.rangeIndexFieldDic.ContainsKey(e.Key))
                        {
                            // SortedSet의 Score를 갱신
                            key = GetRedisKey_TableRangeIndexField(ts.tableID, e.Value.Item1);
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

        public async Task<bool> TableDeleteRowAsync(string tableName, string primaryKeyValue)
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
                foreach (var fieldIndex in ts.matchIndexFieldDic.Values)
                {
                    key = GetRedisKey_TableMatchIndexField(ts.tableID, fieldIndex, fvdic[fieldIndex.ToString()]); 
                    tasklist.Add(db.SetRemoveAsync(key, primaryKeyValue));
                }

                // sortedset 삭제
                foreach (var e in ts.rangeIndexFieldDic)
                {
                    key = GetRedisKey_TableRangeIndexField(ts.tableID, e.Value);
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
        // selectFields : 선택할 field name list. 만약 null이면 모든 field를 선택한다.
        public async Task<Dictionary<string, string>> TableSelectRowByPrimaryKeyFieldValueAsync(List<string> selectFields, string tableName, string primaryKeyValue)
        {
            var retdic = new Dictionary<string, string>();
            var ts = await TableGetSettingAsync(tableName);
            var key = GetRedisKey_TableRow(ts.tableID, primaryKeyValue);
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
        public async Task<List<Dictionary<string,string>>> TableSelectRowByMatchIndexFieldValueAsync(List<string> selectFields, string tableName, string fieldName, string fieldValue)
        {
            var retlist = new List<Dictionary<string, string>>();
            var ts = await TableGetSettingAsync(tableName);
            var db = this.redis.GetDatabase();

            FieldSetting fs;
            if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
            {
                return retlist;
            }
            
            var key = GetRedisKey_TableMatchIndexField(ts.tableID, fs.fieldIndex, fieldValue);
            var pkvs = await db.SetMembersAsync(key);
            
            if (null == selectFields)
            {
                // selectedFields가 null이면 모든 field를 읽는다.
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
                        dic.Add(selectFields[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist;
        }

        // sort된 field값이 lowValue와 highValue 사이에 있는 모든 row를 구한다.
        public async Task<List<Dictionary<string, string>>> TableSelectRowByRangeIndexFieldRangeAsync(List<string> selectFields, string tableName, string fieldName, string lowValue, string highValue)
        {
            var retlist = new List<Dictionary<string, string>>();
            var ts = await TableGetSettingAsync(tableName);
            var db = this.redis.GetDatabase();

            FieldSetting fs;
            if (!ts.tableSchemaDic.TryGetValue(fieldName, out fs))
            {
                return retlist;
            }

            var lv = ConvertToScore(fs.fieldType, lowValue);
            var hv = ConvertToScore(fs.fieldType, highValue);

            var key = GetRedisKey_TableRangeIndexField(ts.tableID, fs.fieldIndex);
            var primaryKeyValues = await db.SortedSetRangeByScoreAsync(key, lv, hv);

            if (null == selectFields)
            {
                // selectField가 null이면 모든 field를 읽는다.
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
                        dic.Add(selectFields[i], rva[i].ToString());
                    }
                    retlist.Add(dic);
                }
            }

            return retlist; 
        }
    }
        
}
