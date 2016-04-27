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

        private string GetRedisKey_TableMatchIndexColumn(Int32 tableID, Int32 columnIndex, string value)
        {
            return string.Format("I:{0}:{1}:{2}", tableID.ToString(), columnIndex.ToString(), value);
        }

        private string GetRedisKey_TableRangeIndexColumn(Int32 tableID, Int32 columnIndex)
        {
            return string.Format("S:{0}:{1}", tableID.ToString(), columnIndex.ToString());
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

        public async Task<TableSetting> TableGetSettingAsync(string tableName)
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

                    var cs = new ColumnSetting();
                    cs.indexNumber = Convert.ToInt32(tokens[0]);

                    switch (tokens[1])
                    {
                        case "System.Byte": cs.dataType = typeof(Byte); break;
                        case "System.Int16": cs.dataType = typeof(Int16); break;
                        case "System.UInt16": cs.dataType = typeof(UInt16);break;
                        case "System.Int32": cs.dataType = typeof(Int32); break;
                        case "System.UInt32": cs.dataType = typeof(UInt32); break;
                        case "System.Int64": cs.dataType = typeof(Int64);  break;
                        case "System.UInt64": cs.dataType = typeof(UInt64); break;
                        case "System.Single": cs.dataType = typeof(Single); break;
                        case "System.Double": cs.dataType = typeof(Double); break;
                        case "System.String": cs.dataType = typeof(String); break;
                        case "System.DateTime": cs.dataType = typeof(DateTime); break;
                    }

                    if (tokens[5].Equals("null"))
                    {
                        cs.defaultValue = null;
                    }
                    else
                    {
                        switch (tokens[1])
                        {
                            case "System.Byte": cs.defaultValue = Convert.ToByte(tokens[5]); break;
                            case "System.Int16": cs.defaultValue = Convert.ToInt16(tokens[5]); break;
                            case "System.UInt16": cs.defaultValue = Convert.ToUInt16(tokens[5]); break;
                            case "System.Int32": cs.defaultValue = Convert.ToInt32(tokens[5]); break;
                            case "System.UInt32": cs.defaultValue = Convert.ToUInt32(tokens[5]); break;
                            case "System.Int64": cs.defaultValue = Convert.ToInt64(tokens[5]); break;
                            case "System.UInt64": cs.defaultValue = Convert.ToUInt64(tokens[5]); break;
                            case "System.Single": cs.defaultValue = Convert.ToSingle(tokens[5]); break;
                            case "System.Double": cs.defaultValue = Convert.ToDouble(tokens[5]); break;
                            case "System.String": cs.defaultValue = Convert.ToString(tokens[5]); break;
                            case "System.DateTime":
                                if (tokens[5].ToLower().Equals("now"))
                                {
                                    cs.defaultValue = "now";
                                }
                                else if (tokens[5].ToLower().Equals("utcnow"))
                                {
                                    cs.defaultValue = "utcnow";
                                }
                                else
                                {
                                    cs.defaultValue = Convert.ToDateTime(tokens[5]);
                                }
                                break;
                        }
                    }
                    
                    cs.isMatchIndex = Convert.ToBoolean(tokens[2]);
                    if (cs.isMatchIndex)
                    {
                        ts.matchIndexColumnDic.Add(e.Name.ToString(), cs.indexNumber);
                    }

                    var fieldPrimaryKeyFlag = Convert.ToBoolean(tokens[3]);
                    if (fieldPrimaryKeyFlag)
                    {
                        ts.primaryKeyColumnName = e.Name;
                    }

                    cs.isRangeIndex = Convert.ToBoolean(tokens[4]);
                    if (cs.isRangeIndex)
                    {
                        ts.rangeIndexColumnDic.Add(e.Name.ToString(), cs.indexNumber);
                    }

                    ts.columnIndexNameDic.Add(cs.indexNumber.ToString(), e.Name.ToString());
                    ts.tableSchemaDic.Add(e.Name.ToString(), cs);
                }

                this.tableSettingDic.TryAdd(tableName, ts);
            }

            return ts;
        }
    }
        
}
