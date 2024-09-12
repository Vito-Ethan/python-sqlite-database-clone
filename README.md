# python-sqlite-database-clone
This was a command line based SQLite clone made with Python for a database course. It features
a few of the basic SQL commands.

## How to use
- PYTHON 3.10 REQUIRED
- You can run the script with the following command: python3 main.py
- The program supports a few of the SQLite commands such as:
  - CREATE DATABASE
  - DROP DATABASE
  - USE <db_name>
  - CREATE TABLE <tb_name> (param1, param2)
  - select * FROM
    - select * FROM WHERE
    - select * from inner join
    - select * from left outer join
  - update <tb_name> set where
  - insert into <tb_name> 

## Lessons Learned
- At the time of making this program I did not know about or understand how compilers/interpreters worked internally.
  - For example, using a lexer, parser, evaluator, nfas, dfas, etc.
- This led to me crudely tokenizing my input (lexer) and parsing the tokens into a json object that represented the query I was working with. I would then process the query accordingly.
- If I ever were to attempt a project like this again I would use a lexer/parser to generate an AST for me to work with.

## Example
You can view the example_input.txt file to view some example commands and their expected outputs.
