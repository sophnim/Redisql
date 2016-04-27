# Redisql

Redis에 데이터를 읽고 쓸때 SQL 테이블처럼 할수 있게 만들어주는 라이브러리입니다. 아래의 간략한 사용법을 참고하세요:

Redisql 인스턴스를 생성합니다:

    Redisql redisql = new Redisql("127.0.0.1", 6379, "foobared"); // redis ip, port, password

테이블 구조를 정의한 후 테이블을 생성합니다.

    // 테이블 생성을 위해 필요한 파라미터 정보를 작성합니다. 
    var columnList = new List<Tuple<string, Type, bool, bool, object>>() // column name, type, make matchIndex, make rangeIndex, defaultValue
    {
        new Tuple<string, Type, bool, bool, object>("name", typeof(String), false, false, null), 
        new Tuple<string, Type, bool, bool, object>("age", typeof(Int32), true, true, 1), 
        new Tuple<string, Type, bool, bool, object>("class", typeof(Int32), false, false, 0), 
        new Tuple<string, Type, bool, bool, object>("birthdate", typeof(DateTime), false, false, "now") 
    };
    
    // 테이블을 생성합니다. 이때 테이블의 PrimaryKey는 'name' column으로 지정했습니다.
    redisql.TableCreate("Member_Table", "name", columnList);

테이블 행(row)단위로 데이터를 입력합니다.

    // 테이블에 입력할 row의 정보를 작성합니다. Dictionary<string,string>에 컬럼이름, 컬럼값을 입력합니다.
    var rowInfo = new Dictionary<string, string>() {
        { "name", "John Smith" },
        { "age", "43" },
        { "class", "1" },
        { "birthdate", DateTime.Now.ToString() }
    };
    
    // TableRowInsert를 호출해서 row를 테이블에 입력합니다.
    redisql.TableRowInsert("Member_Table", rowInfo);





Redis에 대한 접근은 Stackexchange.Redis를 사용하며 Redis의 자료구조인 Hash, Set, SortedSet, String을 사용해서 구현되었습니다.


