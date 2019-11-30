# wodserver
Web of Data Server. A high performance, production ready, reference implementation of the specification.


immutable resources
r1:t1 => json
r2:t2 => json
r1:t3 => json

resource time
t1:r1
t2:r2
t3:r1

rels-out
r1:t1 => r2
(r2 @ t1)

r1:t3 => r2

lup r2:t3 -> nothing.

seek r2: compare and then compare time.

get latest

r1

seek r1: 


