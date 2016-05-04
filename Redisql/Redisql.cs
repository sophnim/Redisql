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
    public class RedisqlEventArgs : EventArgs
    {
        public RedisqlEventType EventType { get; set; }
        public string Message { get; set; }

        public RedisqlEventArgs(RedisqlEventType eventType, string msg)
        {
            this.EventType = eventType;
            this.Message = msg;
        }
    }

    public partial class RedisqlCore
    {
        public event EventHandler OnEvent;

        private ConcurrentDictionary<string, TableSetting> tableSettingDic = new ConcurrentDictionary<string, TableSetting>();
        private ConnectionMultiplexer redis;

        public RedisqlCore(string redisIp, Int32 redisPort, string redisPassword)
        {
            string connString = null;
            if (string.IsNullOrEmpty(redisPassword))
            {
                connString = string.Format("{0}:{1}", redisIp, redisPort.ToString());
            }
            else
            {
                connString = string.Format("{0}:{1},password={2}", redisIp, redisPort.ToString(), redisPassword);
            }

            this.redis = ConnectionMultiplexer.Connect(connString);
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

        private bool CheckDataType(Type dataType, string value)
        {
            switch (dataType.ToString())
            {
                case "System.Byte":
                    {
                        Byte result;
                        if (!Byte.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.Int16":
                    {
                        Int16 result;
                        if (!Int16.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.UInt16":
                    {
                        UInt16 result;
                        if (!UInt16.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.Int32":
                    {
                        Int32 result;
                        if (!Int32.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.UInt32":
                    {
                        UInt32 result;
                        if (!UInt32.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.Single":
                    {
                        Single result;
                        if (!Single.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.Double":
                    {
                        Double result;
                        if (!Double.TryParse(value, out result)) return false;
                    }
                    break;

                case "System.DateTime":
                    {
                        DateTime result;
                        if (!DateTime.TryParse(value, out result)) return false;
                    }
                    break;
            }

            return true;
        }

        public T WaitTaskAndReturnTaskResult<T>(Task<T> task)
        {
            task.Wait();
            return task.Result;
        }

        private RedisValue[] GetSelectColumnIndexNumbers(TableSetting ts, List<string> selectColumnNames)
        {
            ColumnSetting cs;
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
                    // not existing column
                    throw new Exception(string.Format("Table '{0}' does not have '{1}' column", ts.tableName, selectColumnNames[i]));
                }
            }

            return rva;
        }

        public async Task<TableSetting> TableGetSettingAsync(string tableName)
        {
            TableSetting ts;
            var db = this.redis.GetDatabase();

            if (!this.tableSettingDic.TryGetValue(tableName, out ts))
            {
                // 아직 로드되지 않았다. redis로부터 읽어들인다.
                ts = new TableSetting();
                ts.tableName = tableName;

                // get table id
                var tableID = await db.HashGetAsync(Consts.RedisKey_Hash_TableNameIds, tableName);
                if (RedisValue.Null == tableID)
                {
                    return null;
                }

                ts.tableID = Convert.ToInt32(tableID.ToString());

                // read table schema
                var tableSchema = await db.HashGetAllAsync(RedisKey.GetRedisKey_TableSchema(tableName));
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
