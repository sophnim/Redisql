using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Redisql.Core;

namespace Redisql.Transaction
{
    public class TransactionTarget
    {
        public string tableName;
        public string primaryKeyValue;

        public TransactionTarget(string tableName, string primaryKeyValue)
        {
            this.tableName = tableName;
            this.primaryKeyValue = primaryKeyValue;
        }
    }

    public class RedisqlTransaction : IDisposable
    {
        bool isBegin = false;
        RedisqlCore redis;
        List<TransactionTarget> tranTargetList;
        List<Tuple<TableSetting, string>> enterSuccessList;

        public RedisqlTransaction(RedisqlCore redis, List<TransactionTarget> tranTargetList)
        {
            this.redis = redis;
            this.tranTargetList = tranTargetList;
            this.enterSuccessList = new List<Tuple<TableSetting, string>>();
        }

        ~RedisqlTransaction()
        {
            if (isBegin)
                TableLockExitAsync().Wait();
        }

        private async Task<bool> TableLockExitAsync()
        {
            var tasklist = new List<Task<bool>>();
            var len = this.enterSuccessList.Count;
            for (var i = 0; i < len; i++)
            {
                var ts = this.enterSuccessList[i].Item1;
                var primaryKeyValue = this.enterSuccessList[i].Item2;

                tasklist.Add(this.redis.TableLockExit(ts, primaryKeyValue));
            }

            foreach (var task in tasklist)
            {
                await task;
            }

            this.enterSuccessList.Clear();
            return true;
        }

        
        public async Task<bool> TryBeginTransactionAsync(int maxRetryCount = 1000)
        {
            if (isBegin)
                return false;

            var len = this.tranTargetList.Count;
            var tasklist1 = new List<Task<TableSetting>>();
            var tslist = new List<TableSetting>();
            for (var i = 0; i < len; i++)
            {
                var tranTarget = this.tranTargetList[i];
                tasklist1.Add(this.redis.TableGetSettingAsync(tranTarget.tableName));
            }

            for (var i = 0; i < len; i++)
            {
                var ts = await tasklist1[i];
                tslist.Add(ts);
            }

            var tasklist2 = new List<Task<bool>>();
            int retryCount = 0;
            while (true)
            {
                tasklist2.Clear();    
                for (var i = 0; i < len; i++)
                {
                    var tranTarget = this.tranTargetList[i];
                    tasklist2.Add(this.redis.TableLockTryEnterAsync(tslist[i], tranTarget.primaryKeyValue, 0));
                }

                for (var i = 0; i < len; i++)
                {
                    var tranTarget = this.tranTargetList[i];
                    var ret = await tasklist2[i];

                    if (ret)
                    {
                        this.enterSuccessList.Add(new Tuple<TableSetting, string>(tslist[i], tranTarget.primaryKeyValue));
                    }
                }
                
                if (this.enterSuccessList.Count != tranTargetList.Count)
                {
                    await TableLockExitAsync();
                    if (++retryCount >= maxRetryCount)
                    {
                        return false;
                    }
                    else
                    {
                        await Task.Delay(1);
                    }
                }
                else break;
            }

            isBegin = true;
            return true;
        }
        

        /*
        public async Task<bool> TryBeginTransactionAsync(int maxRetryCount = 100)
        {
            if (isBegin)
                return false;

            var len = this.tranTargetList.Count;
            int retryCount = 0;
            while (true)
            {
                for (var i = 0; i < len; i++)
                {
                    var tranTarget = this.tranTargetList[i];
                    var ts = await this.redis.TableGetSettingAsync(tranTarget.tableName);

                    if (!await this.redis.TableLockTryEnterAsync(ts, tranTarget.primaryKeyValue, 10))
                    {
                        break;
                    }
                    else
                    {
                        this.enterSuccessList.Add(new Tuple<TableSetting, string>(ts, tranTarget.primaryKeyValue));
                    }
                }

                if (this.enterSuccessList.Count != tranTargetList.Count)
                {
                    await TableLockExitAsync();
                    if (++ retryCount >= maxRetryCount)
                    {
                        return false;
                    }
                    else await Task.Delay(1);
                }
                else break;
            }

            isBegin = true;
            return true;
        }
        */

        public async Task<bool> EndTransactionAsync()
        {
            await TableLockExitAsync();
            isBegin = false;

            return true;
        }

        public bool TryBeginTransaction(int maxRetryCount = 1000)
        {
            var task = TryBeginTransactionAsync(maxRetryCount);
            task.Wait();
            return task.Result;
        }

        public void EndTransaction()
        {
            TableLockExitAsync().Wait();
            isBegin = false;
        }

        public void Dispose()
        {
            TableLockExitAsync().Wait();
            isBegin = false;
        }


        public async Task<Dictionary<string, string>> TableSelectRowAsync(List<string> selectColumnNames, string tableName, string primaryKeyColumnValue)
        {
            return await this.redis.TableSelectRowAsync(selectColumnNames, tableName, primaryKeyColumnValue, useTableLock: false);
        }

        public async Task<bool> TableUpdateRowAsync(string tableName, Dictionary<string, string> updateColumnNameValuePairs)
        {
            return await this.redis.TableUpdateRowAsync(tableName, updateColumnNameValuePairs, useTableLock:false);
        }

        public async Task<bool> TableDeleteRowAsync(string tableName, string primaryKeyValue, bool useTableLock = true)
        {
            return await this.redis.TableDeleteRowAsync(tableName, primaryKeyValue, useTableLock: false);
        }

        public Dictionary<string, string> TableSelectRow(List<string> selectColumnNames, string tableName, string primaryKeyColumnValue)
        {
            return this.redis.TableSelectRow(selectColumnNames, tableName, primaryKeyColumnValue, useTableLock: false);
        }

        public bool TableUpdateRow(string tableName, Dictionary<string, string> updateColumnNameValuePairs)
        {
            return this.redis.TableUpdateRow(tableName, updateColumnNameValuePairs, useTableLock:false);
        }

        public bool TableDeleteRow(string tableName, string primaryKeyValue)
        {
            return this.redis.TableDeleteRow(tableName, primaryKeyValue, useTableLock:false);
        }
    }
}
