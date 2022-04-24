# Mermaid visuals

See [mermaid-js](https://mermaid-js.github.io/mermaid/#/)
Mermaid lets you create diagrams and visualizations using text and code.

It is a Javascript based diagramming and charting tool that renders Markdown-inspired text definitions to create and modify diagrams dynamically.

If you are familiar with Markdown you should have no problem learning Mermaid's Syntax.

 
```mermaid
flowchart TD; 
A["fa:fa-twitter Twitter"] --> B; 
B(fa:fa-database) --> C --> E; 
B --> D --> E;  
E --> A;
```


```mermaid
graph LR;
  A --> B;
  B --> C --> E;
  B --> D --> E;
  E --> A;
```

```mermaid
graph TD
    A[Christmas] -->|Get money| B(Go shopping)
    B --> C{fa:fa-database Let me think}
    C -->|One| D[Laptop]
    C -->|Two| E[iPhone]
    C -->|Three| F[fa:fa-car Car]
```
```mermaid
gantt
    section Section
    Completed :done,    des1, 2014-01-06,2014-01-08
    Active        :active,  des2, 2014-01-07, 3d
    Parallel 1   :         des3, after des1, 1d
    Parallel 2   :         des4, after des1, 1d
    Parallel 3   :         des5, after des3, 1d
    Parallel 4   :         des6, after des4, 1d
```

