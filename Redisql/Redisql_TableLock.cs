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
                    await Task.Delay(1);
                    if (++count >= retryCount)
                    {
                        return false;
                    }
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
                var ts = new TimeSpan(0, 0, 300);

                var stw = Stopwatch.StartNew();
                bool ret;
                do
                {
                    ret = await db.StringSetAsync(key, Thread.CurrentThread.ManagedThreadId.ToString(), ts, When.NotExists);
                    if (false == ret)
                        await Task.Delay(1);

                    if (stw.ElapsedMilliseconds > 3000)
                    {
                        // maybe deadlock?
                        Console.WriteLine("TableLockEnterAsync takes too long time: Deadlock is suspected!");
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

        public async Task<bool> TableLockExit(string tableName, string primaryKeyValue)
        {
            try
            {
                var db = this.redis.GetDatabase();
                var key = RedisKey.GetRedisKey_TableLock(tableName, primaryKeyValue);
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
