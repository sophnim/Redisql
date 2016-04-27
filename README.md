# Redisql

Redis에 데이터를 읽고 쓸때 SQL 테이블처럼 할수 있게 만들어주는 라이브러리입니다. 아래의 간략한 사용법을 참고하세요:


테이블을 생성합니다.

테이블 행(row)단위로 데이터를 입력합니다.

테이블에 입력된 여러개의 row중에 

Redis에 대한 접근은 Stackexchange.Redis를 사용하며 Redis의 자료구조인 Hash, Set, SortedSet, String을 사용해서 구현되었습니다.


