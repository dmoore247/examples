
```mermaid
graph LR
  A(mytable) --> B(GROUP BY mykey)
  B --> C(ORDER BY count(*))
  C --> D{LIMIT 10}
  C --> E{SELECT}
  D --> F(count(*), mykey)
  E --> G(count(*), mykey)
  ```
