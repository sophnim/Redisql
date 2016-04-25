using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql
{
    public class FieldSetting
    {
        public Int32 fieldIndex;
        public Type fieldType;
        public bool fieldMatchIndexFlag;
        public bool fieldRangeIndexFlag;
        public object fieldDefaultValue;
    }

    public class TableSetting
    {
        public int tableID;
        public string primaryKeyFieldName;
        public Dictionary<string, FieldSetting> tableSchemaDic = new Dictionary<string, FieldSetting>();
        public Dictionary<string, Int32> matchIndexFieldDic = new Dictionary<string, int>();
        public Dictionary<string, Int32> rangeIndexFieldDic = new Dictionary<string, int>();
        public Dictionary<string, string> fieldIndexNameDic = new Dictionary<string, string>();

        public Int32 GetNextFieldIndex()
        {
            Int32 maxFieldIndex = 0;
            foreach (var fs in tableSchemaDic.Values)
            {
                if (maxFieldIndex < fs.fieldIndex) maxFieldIndex = fs.fieldIndex;
            }

            return maxFieldIndex + 1;
        }
    }
}
