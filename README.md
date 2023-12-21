<p align="center">
  <img src="https://github.com/Vedadiyan/genql/blob/master/.github/logo.png" />
</p>

<h1 align="center">General Querying Language</h1>

# Table of Contents
- [Introduction](#introduction)
    - [Use Cases](#use-cases)
- [Getting Started](#getting-started)
- [Basic SQL Syntax Overview](#basic-sql-syntax-overview)
    - [Non Columnar Group By](#non-columnar-group-by)
    - [Common Table Expressions](#common-table-expressions)
        - [Direct Selection from CTEs](#direct-selection-from-ctes)
    - [Function Execution Strategies](#function-execution-strategies)
        - [ASYNC Execution](#async-execution)
        - [SPIN Execution](#spin-execution)
        - [SPINASYNC Execution](#spinasync-execution)
        - [ONCE Execution](#once-execution)
        - [GLOBAL Execution](#global-execution)
        - [SCOPED Execution](#scoped-execution)
        - [Default Execution Strategy](#default-execution-strategy)
     - [Backward Navigation](#backward-navigation)
 - [Selector Language Guide](#selector-language-guide)
    - [Query Syntax](#query-syntax)
        - [Get a Key's Value](#get-a-keys-value)
        - [Get an Array Element](#get-an-array-element)
        - [Multi-dimensional Arrays](#multi-dimensional-arrays)
        - [Keep Array Structure](#keep-array-structure)
        - [Iterate Through Arrays](#interate-through-arrays)
        - [Array Slices](#array-slices)
        - [Reshape Data](#reshape-data)
        - [Escape Keys](#escape-keys)
        - [Continue With](#continue-with)
        - [Top Level Functions](#top-level-functions)
    - [Examples](#examples)
    - [Tips](#tips)
    

# Introduction
In today's data-driven world, navigating diverse data formats and extracting insights can be a complex challenge. GenQL offers a powerful solution by providing a **standardized SQL interface** that abstracts away the complexities of heterogeneous data sources and transformations. By leveraging familiar SQL syntax, GenQL empowers users of all skill levels to efficiently interact with data, regardless of its underlying format. In simple words, it lets you run SQL queries on any data format, join data from different sources, and map the results to a standardized data model.

Underneath, GenQL uses the SQL dialect and capabilities supported by MySQL and MariaDB databases. However, GenQL builds upon this foundation by adding new specialized flavors to standard SQL:
-   A selector language to specify data access patterns
-   Configurable execution policies for SQL functions
-   Direct selection from common table expressions
-   Ability to output result sets in non-columnar formats
-   Ability to navigate back and forth in complex and multi-dimensional data

This documentation serves as a comprehensive guide to the capabilities and functionalities of GenQL. It will equip you to:

-   **Understand the core principles** behind GenQL and its role in simplifying data access and manipulation.
-   **Master the syntax** and extensions introduced by GenQL, enabling you to craft powerful queries for various use cases.
-   **Explore the built-in features** for efficient data migration, dynamic data translation, and comprehensive analytics.
-   **Integrate GenQL seamlessly** into your existing workflows and applications.

## Use Cases 
The true magic of GenQL lies in its ability to **transform diverse data challenges into tangible solutions**. This chapter delves into the practical side of the equation, showcasing how GenQL can be your **one-stop shop for conquering real-world data scenarios**.
-   Imagine you have all sorts of data in different formats, like spreadsheets, text files, or even social media posts. GenQL can act as a universal translator, letting you run the same queries on all of it, saving you time and effort.
-   Think of GenQL as a Swiss Army knife for data. Whether you're moving information from one system to another, automatically converting between formats, or crunching numbers for insights, GenQL can be your trusty tool.
-  Migrating data from an old system to a new one can be a headache, but GenQL can streamline the process by easily translating information, even if the formats are different.
-   Building a website or app that pulls data from multiple sources, like social media and internal databases, is often complex. GenQL can simplify this by providing a single interface to query and combine all the data.
-   Data analysis often involves manipulating and transforming data before you can make sense of it. GenQL gives you powerful tools to clean, filter, and reshape your data, making it ready for analysis.
- With GenQL, you don't need to learn different query languages for each type of data. It speaks the language of SQL, which is already familiar to many people, making it easy to pick up and use.
-   GenQL saves you time and effort by automating many data handling tasks, freeing you to focus on the bigger picture and getting insights from your data.
-   By making data manipulation more accessible, GenQL empowers people of all skill levels to work with data and extract valuable information, even without being programming experts.

# Getting Started
Here's a step-by-step guide to integrate GenQL into your projects:

Execute the following command `go get github.com/vedadiyan/genql`. This command downloads and installs the GenQL package, making its functionalities available for use within your Go code.

In your Go code, include this import statement `import "github.com/vedadiyan/genql"` to access and utilize the functions and features provided by the GenQL package. 

Example:

    package main

    import (
        "encoding/json"
        "fmt"
        "log"

        "github.com/vedadiyan/genql"
    )

    func main() {
        const sampleJson = `{"data": [{"value": "Hello World"}]}`
        data := make(map[string]any)
        err := json.Unmarshal([]byte(sampleJson), &data)
        if err != nil {
            log.Fatalln(err)
        }
        query, err := genql.New(data, `SELECT * FROM "root.data"`, Wrapped(), PostgresEscapingDialect())
        if err != nil {
            log.Fatalln(err)
        }
        result, err := query.Exec()
        if err != nil {
            log.Fatalln(err)
        }
        fmt.Printf("%v\r\n", result)
    }

# Basic SQL Syntax Overview
Structured Query Language (SQL) serves as the common standard for database query languages. SQL allows users to retrieve, manipulate, and transform data stored across various relational database systems. This section provides a high-level reference of basic SQL statements and clauses supported within the GenQL framework. 

- ✅ Subqueries
- ✅ Select Expressions
- ❌ Multiple Object Selection (e.g. SELECT FROM obj1, obj2 not supported)
- ✅ Case When
- ✅ Aliases
- ✅ Like Expressions
- ✅ Functions
- 🆒 Function Execution Strategies
- 🆒 Multi-Dimensional Selectors (please refer to the selector language guide)
- ✅ Limit
- ✅ Group By
- ❎ Joins
    - ✅ Inner, Left, Right Joins
    - ⭕ Full Outer Join (MySQL limitation)
    - ❌ Natural Joins (not planned)
- ⭕ Cross Apply (MySQL limitation)
- ✅ Unions
- ✅ CTEs
- ✅ Having
- ✅ Order By

While GenQL specializes in non-relational data, it adopts much of ANSI SQL syntax and capabilities for querying, joining, filtering, and shaping heterogeneous data collections. Familiarity with essential SQL semantics paves the way for effectively composing GenQL queries.


    SELECT
        [ALL | DISTINCT]  -- Optional: Specify whether to return all rows or distinct values
        select_expr [[AS] alias] [, select_expr ...]  -- Expressions or columns to select, with optional aliases
        FROM table_references  -- Required: Specifies the tables or data sources to query
        [
            JOIN | INNER JOIN | LEFT JOIN | RIGHT JOIN  -- Optional: Join types for combining tables
            table_reference [[AS] alias]  -- Table to join, with optional alias
            ON join_condition  -- Condition for joining tables
        ]
        [WHERE where_condition]  -- Optional: Filters rows based on specified conditions
        [GROUP BY {col_name} [, col_name ...] ]  -- Optional: Groups rows based on specified columns
        [HAVING where_condition]  -- Optional: Filters groups based on specified conditions
        [ORDER BY {col_name} [ASC | DESC] [, col_name ...] ]  -- Optional: Sorts results based on specified columns
        [LIMIT {[offset,] row_count | row_count OFFSET offset}]  -- Optional: Limits the number of returned rows
        [UNION [ALL] select_statement]  -- Optional: Combines results of multiple queries

## Non Columnar Group By 
The GROUP BY clause in GenQL works in a non columnar way making the `PARTITION` functionality redundant. When a `GROUP BY` is executed by GenQL, alongside normal grouping and including the group keys in the result set, the whole group is also included in the result under which is accessible through the `*` key.

## Common Table Expressions 
Common Table Expressions (CTEs) are powerful temporary named result sets that enable modularizing complex queries in GenQL. CTEs are materialized subqueries that allow breakdown of multi-layered transformations into simpler building blocks. By assigning result data sets to inline view names, CTEs unlock capabilities like:

 - Query re-use without repetitive subquery definitions
 - Granular query development in straightforward steps
 - Logical grouping of nested derived tables
 - Backward-compatible conversion of subqueries from legacy dialects

In effect, CTEs make it possible to reference modular query parts similar to how programming functions compartmentalize code - benefiting abstraction, reuse, and nested hierarchies. The syntax below covers GenQL configuration supporting interoperable CTE specifications for streamlined data shaping without persistence.

    [
        WITH cte_name [(col_name [, col_name] ...)] 
        AS (SELECT query) [, cte_name2 AS (SELECT ...)]
    ]

### Direct Selection from CTEs

GenQL's SQL dialect provides a specialized shortcut syntax to directly interrogate column elements or dimensions from a Common Table Expression (CTE) result set projection without needing to repeat or re-query the entire CTE.

For example:

    WITH addresses AS (SELECT name, address FROM users)
    SELECT street FROM `addresses.address`

The key capability this unlocks is to reference any properties or dimensions included within the initial CTE column projection using the selector syntax on the CTE name.

This retrieves CTE result data without restating potentially complex underlying queries, joins, or filtering logic. Once a CTE result set is defined, its columns can be explored similarly to a view or table - but only within the enclosing SQL scope.


## Function Execution Strategies 
GenQL offers different ways to run functions, unlike SQL which focuses on processing columnar data. GenQL handles non-columnar, nested, and multi-dimensional data, making it ideal for dynamic data processing, translation, and transformation. Therefore, the default SQL function execution mechanism doesn't meet GenQL's needs.

GenQL provides six dynamic function execution strategies: ASYNC, SPIN, SPINASYNC, ONCE, GLOBAL, and SCOPED. You can specify these strategies without altering your custom function code. Here's the syntax:

    [(ASYNC | SPIN | SPINASYNC | ONCE | GLOBAL | SCOPED)?.functionName([, args])]

### Async Execution
Async execution is ideal for functions with high latency, especially those involving I/O operations. With Async, the query executor continues processing the next rows instead of waiting for the function call to finish. After processing all rows for a query, the query executor awaits all asynchronous function calls to complete. Once an async operation completes, its result instantaneously becomes available in the corresponding row.

As an example, a function that performs caching can be executed asynchronously. By running cache operations or other I/O workloads asynchronously, the query executor does not wait for each function to complete and it continues to the next row. This prevents high latency functions from delaying overall execution. Instead, asynchronous operations proceeds separately while main query logic computes on rows in parallel. This asynchronous approach increases throughput by avoiding having queries blocked waiting on results from long running functions.

### SPIN Execution
SPIN allows running a function without blocking the query or returning any result. When the executor encounters a SPIN function, it submits it to a background worker, returns immediately, and doesn't wait for its completion. This means the function call doesn't affect the corresponding row.

For example, a custom function handling logging could be executed in a way not waiting for the logging to finish. This allows continuing with rest of the query rather than waiting for logging completion. Doing logging separately prevents it from reducing speed of the main processing.

### SPINASYNC Execution
SPINASYNC is similar to SPIN in that it doesn't return any result, but the executor waits for it to finish when the entire query is processed. This is useful for tasks like inserting data into another database while reading rows.

For example, an insert function can be executed in a non-blocking way. By submitting inserts to a background worker, waits are avoided allowing the executor to continue to the next rows. This prevents any unwanted latency caused by the insert opration slowing down the overall execution. 

### ONCE Execution
This strategy runs a function once for all rows rather than each one. The function executes just once and further usages reuse the prior output. This allows including overall analysis without needing to recompute every time. For example, a Statistics function could be leveraged this way, executing once with reuse rather than per row.

### GLOBAL Execution
The GLOBAL strategy broadens function execution scope from only the current row out to the whole object. This allows non-aggregation functions to also operate on the whole object rather than row-by-row.

### SCOPED Execution
The SCOPED strategy forces the executor to be scoped to the current row only. Standard SQL functions often process the entire dataset by default. An instance is how SUM calculates the total of a numeric column across all rows initially. However the SCOPED strategy alters the execution scope only to data within the current row instead.

### Default Execution Strategy
When no strategy is specified, GenQL automatically chooses the SCOPED strategy unless the function is considered by default as an aggregate function in SQL.

## Backward Navigation 
GenQL by default scopes the select statement, subqueries, joins, or function calls to the current row. However, if this is not a desired behavior, backward navigation can be used to change the scope of the selection. This can be simply done by using `<-` operator. Each time the `<-` operator is used, the current row is navigated one step backward. 

For example, give the following dataset:

    {
        "users": [
            {
                "id": 1,
                "name": "Pouya"
            },
            {
                "id": 2,
                "name": "Dennis"
            }
        ],
        "meta": {
            "ip": "127.0.0.1"
        }
    }

It is possible to select both user data and and ip address at the same time:

    SELECT id, name, `<-root.meta.ip` AS ip FROM `root.users`

The `<-` operator can also be used repeatedly to move to further dimensions like `<-<-root.meta,ip`

# Selector Language Guide

Selectors allow you to query and retrieve values from datasets. They provide a powerful way to select keys, array indexes, slice arrays, and more.  

## Query Syntax

### Get a Key's Value  
Simply reference the key name to get its value. Selectors automatically handle arrays and nesting.    

`user.name`

### Get an Array Element   
Use brackets with the index.    

`users[0].name`  

### Multi-dimensional Arrays  
Use colons and `each` to skip dimensions and specify the index for the dimension you want.    

`data[each:each:0]`  

### Keep Array Structure     
Use the `keep` option to not flatten results.  

`data[keep=>0:1:2]`      

### Iterate Through Arrays  
Use `each` to iterate through a dimension's indexes.     

`users[each:0].name`

### Array Slices    
Select a slice with `start:end`. Omit either to go to array edge.       

`users[(5:10)]`      

### Reshape Data   
Pipe to keys to convert types or add new keys.         

`user{id|string, createdAt}`        

### Escape Keys  
Wrap special key names in single quotes.        

`'user.name'.key`   

### Continue With 
You can execute one selector and continue with the result. 

`data[each].user::[0]`

### Top Level Functions
Top level functions are extendable and few top level functions are built-in. 

`mix=>data[each].x[each].y`

## Examples        

Get third element for each user:         

`users[each:0:0].email`  

Convert ID to string and add a new key:          

`user{id|number, active|string}`        

## Tips  

- Use `each` to flatten arrays 
- You can convert and select in one query   
- Arrays must be sliced before selecting value


