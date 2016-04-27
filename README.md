# Redisql

Redis에 데이터를 읽고 쓸때 SQL 테이블처럼 할수 있게 만들어주는 라이브러리입니다. 아래의 간략한 사용법을 참고하세요:

Redisql 인스턴스를 생성합니다:

    Redisql redisql = new Redisql.Redisql("127.0.0.1", 6379, "foobared"); // redis ip, port, password

테이블 구조를 정의한 후 테이블을 생성합니다.

    // 테이블 생성을 위해 필요한 파라미터 정보를 작성합니다. 
    var columnList = new List<Tuple<string, Type, bool, bool, object>>() // column name, type, make matchIndex, make rangeIndex, defaultValue
    {
        new Tuple<string, Type, bool, bool, object>("name", typeof(String), false, false, null), 
        new Tuple<string, Type, bool, bool, object>("age", typeof(Int32), true, true, 1), 
        new Tuple<string, Type, bool, bool, object>("gender", typeof(Int32), false, false, 0), 
        new Tuple<string, Type, bool, bool, object>("birthdate", typeof(DateTime), false, false, "now") 
    };
    // Create Table
    redisql.TableCreateAsync("Account_Table", "name", columnList).Wait();

테이블 행(row)단위로 데이터를 입력합니다.

테이블에 입력된 여러개의 row중에 

Redis에 대한 접근은 Stackexchange.Redis를 사용하며 Redis의 자료구조인 Hash, Set, SortedSet, String을 사용해서 구현되었습니다.


