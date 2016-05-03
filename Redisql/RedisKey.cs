using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql.Core
{
    public class RedisKey
    {
        internal static string GetRedisKey_TableSchema(string tableName)
        {
            return string.Format("C:{0}", tableName);
        }

        internal static string GetRedisKey_TableMatchIndexColumn(Int32 tableID, Int32 columnIndex, string value)
        {
            return string.Format("I:{0}:{1}:{2}", tableID.ToString(), columnIndex.ToString(), value);
        }

        internal static string GetRedisKey_TableRangeIndexColumn(Int32 tableID, Int32 columnIndex)
        {
            return string.Format("S:{0}:{1}", tableID.ToString(), columnIndex.ToString());
        }

        internal static string GetRedisKey_TableRow(Int32 tableID, string primaryKeyValue)
        {
            return string.Format("W:{0}:{1}", tableID.ToString(), primaryKeyValue);
        }

        internal static string GetRedisKey_TablePrimaryKeyList(string tableName)
        {
            return string.Format("K:{0}", tableName);
        }

        internal static string GetRedisKey_TableLock(string tableName, string primaryKeyValue)
        {
            return string.Format("{0}:{1}:{2}", Consts.RedisKey_Prefix_TableLock, tableName, primaryKeyValue);
        }
    }
}
