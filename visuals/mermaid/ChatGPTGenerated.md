
# ChatGPT Generated Diagrams

1. Generate the mermaid.js markdown compatible with Github represent this SQL Query: 

`SELECT count(*), mykey from mytable GROUP BY mykey ORDER BY count(*) DESC LIMIT 10`

---
```mermaid
flowchart LR;
  A(mytable) --> B("GROUP BY mykey");
  B --> C("ORDER BY count(1)");
  C --> D{"LIMIT 10"};
  C --> E{"SELECT"};
  D --> F("count(1), mykey");
  E --> G("count(1), mykey");
```
---
2. One possible way to generate the mermaid.js markdown compatible with Github to represent the logical execution plan for this SQL query is:
```mermaid
flowchart TD;
  A(mytable) --> B("GROUP BY mykey")
  B --> C("ORDER BY count(*)")
  C --> D("LIMIT 10")
  D --> E("SELECT count(*), mykey")
```
---
Generate a mermaid js diagram of the dependency graph for this SQL command:

```sql
CREATE TABLE output AS
SELECT * 
FROM header h
JOIN detail d
  ON h.header_id = d.header_id;
```

```mermaid
graph LR;
  A("header") --> B("JOIN");
  C("detail") --> B;
  B --> D("CREATE TABLE output");
```

```mermaid
graph LR;
  A("header") -->|header_id| B("JOIN");
  C("detail") -->|header_id| B;
  B --> D("CREATE TABLE output");
```
