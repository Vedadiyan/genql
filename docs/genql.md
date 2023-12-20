# GenQL (Generic Query Language)

GenQL implements MySQL query dialect for complex data structures.

It enables high performance SQL-like querying of large, complex data. GenQL was originally created to integrate with Protobuf for automatic mapping between Protobuf messages and JSON data.

# SQL Interpretation

GenQL uses a modified sqlparser from Vitess, guaranteeing correctly interpreted SQL.

# Usage

GenQL remodels JSON into desired data structures. For instance, when consuming a third-party API, use GenQL to reshape the API response to match internal data models. The GenQL output can then automatically map to internal structures.

# 📌 Supported Features

- ✅ Subqueries
- ✅ Select Expressions
- ❌ Multiple Object Selection (e.g. SELECT FROM obj1, obj2 not supported)
- ✅ Case When
- ✅ Aliases
- ✅ Like Expressions
- ✅ Aggregate Functions (extendable)
- 🆒 Singleton Functions (one execution per query)
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