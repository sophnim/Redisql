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
                TableLockExit();
        }

        private async void TableLockExit()
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
        }

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
                    TableLockExit();
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

        public bool TryBeginTransaction(int maxRetryCount = 100)
        {
            var task = TryBeginTransactionAsync(maxRetryCount);
            task.Wait();
            return task.Result;
        }

        public void EndTransaction()
        {
            TableLockExit();
            isBegin = false;
        }

        public void Dispose()
        {
            TableLockExit();
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
