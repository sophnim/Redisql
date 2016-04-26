using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql
{
    public class ColumnSetting
    {
        internal Int32 indexNumber;
        internal Type dataType;
        internal bool isMatchIndex;
        internal bool isRangeIndex;
        internal object defaultValue;
    }

    public class TableSetting
    {
        public int tableID { get; internal set; }
        public string primaryKeyColumnName { get; internal set; }
        public Dictionary<string, ColumnSetting> tableSchemaDic { get; internal set; } = new Dictionary<string, ColumnSetting>();

        internal Dictionary<string, Int32> matchIndexColumnDic = new Dictionary<string, int>();
        internal Dictionary<string, Int32> rangeIndexColumnDic = new Dictionary<string, int>();
        internal Dictionary<string, string> columnIndexNameDic = new Dictionary<string, string>();

        internal Int32 GetNextColumnIndexNumber()
        {
            Int32 maxNum = 0;
            foreach (var cs in tableSchemaDic.Values)
            {
                if (maxNum < cs.indexNumber) maxNum = cs.indexNumber;
            }

            return maxNum + 1;
        }
    }
}
