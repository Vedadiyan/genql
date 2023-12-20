# Selector Language Guide

Selectors allow you to query and retrieve values from JSON-like data structures. They provide a powerful way to select keys, array indexes, slice arrays, and more.  

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