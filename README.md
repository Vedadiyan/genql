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


