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
        private async Task<bool> TableLockEnterAsync(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = RedisKey.GetRedisKey_TableLock(tableName, primaryKeyValue);
            var ts = new TimeSpan(0, 0, 300);

            bool ret;
            do
            {
                ret = await db.StringSetAsync(key, Thread.CurrentThread.ManagedThreadId.ToString(), ts, When.NotExists);
                if (false == ret)
                    await Task.Delay(1);
            } while (false == ret);

            return true;
        }

        private void TableLockExit(string tableName, string primaryKeyValue)
        {
            var db = this.redis.GetDatabase();
            var key = RedisKey.GetRedisKey_TableLock(tableName, primaryKeyValue);
            db.KeyDeleteAsync(key, CommandFlags.FireAndForget);
        }
    }
}
