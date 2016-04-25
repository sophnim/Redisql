using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql
{
    public class FieldSetting
    {
        public Int32 indexNumber;
        public Type dataType;
        public bool isMatchIndex;
        public bool isRangeIndex;
        public object defaultValue;
    }

    public class TableSetting
    {
        public int tableID;
        public string primaryKeyFieldName;
        public Dictionary<string, FieldSetting> tableSchemaDic = new Dictionary<string, FieldSetting>();
        public Dictionary<string, Int32> matchIndexFieldDic = new Dictionary<string, int>();
        public Dictionary<string, Int32> rangeIndexFieldDic = new Dictionary<string, int>();
        public Dictionary<string, string> fieldIndexNameDic = new Dictionary<string, string>();

        public Int32 GetNextFieldIndexNumber()
        {
            Int32 maxNum = 0;
            foreach (var fs in tableSchemaDic.Values)
            {
                if (maxNum < fs.indexNumber) maxNum = fs.indexNumber;
            }

            return maxNum + 1;
        }
    }
}
