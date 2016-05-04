using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql.Core
{
    public class Consts
    {
        public const string RedisKey_Hash_TableNameIds = "HS:Table_Name_Ids";
        public const string RedisKey_Hash_TableAutoIncrementColumnValues = "HS:Table_Auto_Increment_Column_Values";
        public const string RedisKey_String_TableNameIds = "ST:Table_Name_Ids";
        public const string RedisKey_Prefix_TableLock = "L";
        public const Int32 TableLockExpireSecond = 300;
        public const Int32 TableLockWaitLongWarningDurationMiliseconds = 3000;
    }
}
