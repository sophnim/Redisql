using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using System.Diagnostics;

using StackExchange.Redis;

namespace Redisql.Core
{
    public partial class RedisqlCore
    {
        // Clear all existing table lock
        public async Task<bool> TableLockClearAllAsync()
        {
            try
            {
                var pattern = string.Format("{0}:*", Consts.RedisKey_Prefix_TableLock);
                var db = this.redis.GetDatabase();
                var endpoints = this.redis.GetEndPoints();
                foreach (var ep in endpoints)
                {
                    var server = this.redis.GetServer(ep);
                    foreach (var key in server.Keys(pattern: pattern))
                    {
                        await db.KeyDeleteAsync(key);
                    }
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        // Tries to enter lock, If retry count over, return false
        public async Task<bool> TableLockTryEnterAsync(TableSetting tableSetting, string primaryKeyValue, int retryCount = 10)
        {
            try
            {
                var db = this.redis.GetDatabase();
                var key = RedisKey.GetRedisKey_TableLock(tableSetting.tableName, primaryKeyValue);
                var ts = new TimeSpan(0, 0, 300);

                int count = 0;
                while (!await db.StringSetAsync(key, Thread.CurrentThread.ManagedThreadId.ToString(), ts, When.NotExists))
                {
                    if (++count >= retryCount)
                    {
                        return false;
                    }
                    await Task.Delay(1);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        // wait until enter lock
        public async Task<bool> TableLockEnterAsync(TableSetting tableSetting, string primaryKeyValue)
        {
            try
            {
                var db = this.redis.GetDatabase();
                var key = RedisKey.GetRedisKey_TableLock(tableSetting.tableName, primaryKeyValue);
                var ts = new TimeSpan(0, 0, Consts.TableLockExpireSecond);

                var stw = Stopwatch.StartNew();
                bool ret;
                do
                {
                    ret = await db.StringSetAsync(key, Thread.CurrentThread.ManagedThreadId.ToString(), ts, When.NotExists);
                    if (false == ret)
                        await Task.Delay(1);

                    if (stw.ElapsedMilliseconds > Consts.TableLockWaitLongWarningDurationMiliseconds)
                    {
                        // maybe deadlock?
                        OnEvent(this, 
                            new RedisqlEventArgs(RedisqlEventType.Warn, 
                            string.Format("TableLockEnterAsync takes too long time: TableName={0} PrimaryKeyValue={1}  {2}ms",
                            tableSetting.tableName, primaryKeyValue, stw.ElapsedMilliseconds)));

                        stw.Restart();
                    }
                } while (false == ret);

                return true;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> TableLockExit(TableSetting tableSetting, string primaryKeyValue)
        {
            try
            {
                var db = this.redis.GetDatabase();
                var key = RedisKey.GetRedisKey_TableLock(tableSetting.tableName, primaryKeyValue);
                await db.KeyDeleteAsync(key);

                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
