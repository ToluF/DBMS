import json
import os
import string
from operator import itemgetter
import copy
"""
Name:Tolu Fashina
Netid:fashinaa
PID:A56871187
How long did this project take you? 5 days


Sources: https://www.w3schools.com/sql/default.asp
         https://www.geeksforgeeks.org/python-program-to-find-the-string-in-a-list/

"""
_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        
        self.filename = filename
        
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
            
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database


        if os.path.isfile(self.filename):
            self.database = self.load_database()


        self.db_clone = None
        self.db_rollback = None
        self.start_transaction = False
        self.auto_commit = True
        self.lock = True
        self.SL = False
        self.RL = False
        self.XL = False
        self.view_val = False
        self.view_name = None
        self.view_query = None
        
  
    def load_database(self):
        # if os.path.isfile(self.filename):
        #     self.database = None
            with open(self.filename, 'r') as file:
                data = json.load(file)

            # print(data)
            root_key = list(data.keys())[0]
            # print(root_key)
            db = Database(root_key)


            db.tables = {}
            for tab_name, tab_data in data[root_key].items():
                table_name = tab_data['Name']
            
                # table_column = tab_data['Columns']
                table_column_name_type = tab_data['Column_name_types_pairs']
                table_default = tab_data['Defaults']
                db.tables[table_name] = Table(table_name,table_column_name_type,table_default)
                table = db.tables[table_name]
                table.rows = tab_data['Rows']

                assert table.name == table_name
            
            return db
        

    def executemany(self,statement, parameters):
        if len(parameters) != 0: 
            tokens = tokenize(statement)
            last_semicolon = tokens.pop()
            if tokens[0] == "INSERT":
                start = tokens.index("(")+1 # get row_value from tokens
                end = tokens.index(")")
                val = tokens[start:end] #[?,?,?]
                def_par_idx = 0
                def_par_val = None
                def_param = False

                for i in val:
                    if i == ",":
                        val.remove(i)                
                for i,v in enumerate(val):
                    if type(i) == int:
                        def_param = True
                        def_par_val = v
                        def_par_idx = i
                    elif type(i) == float:
                        def_param = True
                        def_par_val = v
                        def_par_idx = i
                    elif type(i) == string and (i != "?" and i != ","):
                        def_param= True
                        def_par_val = v
                        def_par_idx = i

                new_tokens = tokens[0:start-1]
            
            
                for param in parameters:
                    if def_param is True:
                        param = param[:def_par_idx] + (def_par_val,) + param[def_par_idx:]
                        
                    new_tokens.append(param)
            
            params = True
            # print(new_tokens)

            self.execute(statement,params,new_tokens)

    def execute(self, statement, params=False, new_statement=[]):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            
            if tokens[0] == "VIEW":
                view_val = True
                pop_and_check(tokens, "VIEW") 
                self.view_name = tokens.pop(0)
                pop_and_check(tokens, "AS")
                self.view_query = tokens
                return 
  

            pop_and_check(tokens, "TABLE")
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                tab_name = tokens.pop(0)
                self.database.check(tab_name)
                # table_name = tokens.pop(0)
                pop_and_check(tokens, "(")
                column_name_type_pairs = []
                while True:
                    column_name = tokens.pop(0)
                    column_type = tokens.pop(0)
                    assert column_type in {"TEXT", "INTEGER", "REAL"}
                    column_name_type_pairs.append((column_name, column_type))
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                self.database.create_new_table(table_name, column_name_type_pairs)
                

            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            defaults = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                if tokens[0] == "DEFAULT":
                    pop_and_check(tokens, "DEFAULT")
                    val = tokens.pop(0)
                    defaults.append((column_name, val))
                # print(defaults)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs,defaults)

        def insert(tokens,params = False):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            col_name = []
            row_contents = []
            default = False
            if tokens[0] == "DEFAULT":
                default = True
                pop_and_check(tokens, "DEFAULT")
                pop_and_check(tokens, "VALUES")
            if len(tokens) != 0:
                if tokens[0] != "VALUES":  #if insert statement has includes columns
                    pop_and_check(tokens, "(")
                    while True:
                        item = tokens.pop(0)
                        col_name.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            break
                        assert comma_or_close == ','
                    
                pop_and_check(tokens, "VALUES")
                if params is True and tokens[0] != "(":
                    param_rows = []
                    while True:
                        if not tokens:
                            break
                        row = tokens.pop(0)
                        param_rows.append(row)
                    return self.database.insert_into(table_name, param_rows,col_name,default, params)
                # print(param_rows)
                pop_and_check(tokens, "(")
                
                # print(tokens)
                while True:
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
            
            if len(tokens) != 0:   #  checks if insert statement has multiple values
                while len(tokens) > 0:
                    pop_and_check(tokens, ",")
                    pop_and_check(tokens, "(")
                    while True:
                        item = tokens.pop(0)
                        row_contents.append(item)
                        comma_or_close = tokens.pop(0)
                        # print(comma_or_close)
                        if comma_or_close == ")":
                            break
                        assert comma_or_close == ','
            # print(row_contents)
            if self.RL is True and self.start_transaction is True:
                self.db_clone = self.database
                return self.db_clone.insert_into(table_name, row_contents,col_name,default,params)
            else:
                return self.database.insert_into(table_name, row_contents,col_name,default,params)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            distinct = False
            output_columns = []
            if tokens[0] == "DISTINCT":
                pop_and_check(tokens, "DISTINCT")
                distinct = True
            
            elif tokens[0] == "MAX":
                mx = tokens.pop(0)
                tokens.pop(0)
                col_name = tokens.pop(0)
                tokens.pop(0)
                min_or_max = [mx,col_name]
                pop_and_check(tokens, "FROM")
                tab_nam = tokens.pop(0)
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                order_val = tokens.pop(0)
                return self.database.select_min_max(min_or_max,tab_nam,order_val)
            
            elif tokens[0] == "MIN":
                mn = tokens.pop(0)
                tokens.pop(0)
                col_name = tokens.pop(0)
                tokens.pop(0)
                min_or_max = [mn,col_name]
                pop_and_check(tokens, "FROM")
                tab_nam = tokens.pop(0)
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                order_val = tokens.pop(0)
                return self.database.select_min_max(min_or_max,tab_nam,order_val)

                
            while True:
                col = tokens.pop(0)
                if tokens[0] == ".":  #checks if columnhas table id
                    dot = tokens.pop(0)
                    nme = tokens.pop(0)
                    output_columns.append(col+dot+nme)
                else:
                    output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table_name = tokens.pop(0)
            if table_name == self.view_name:
                view_tokens = copy.deepcopy(self.view_query)
                return select(view_tokens)
            clause = []
            join_terms = []
            join = False
            table_name_2 = ""
            if tokens[0] == "LEFT":
                join = True
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                table_name_2 = tokens.pop(0)
                pop_and_check(tokens, "ON")
                col = tokens.pop(0)
                if tokens[0] == ".":
                    dot = tokens.pop(0)
                    nme = tokens.pop(0)
                    join_terms.append(col+dot+nme)
                else:
                    join_terms.append(col)
                join_operator = tokens.pop(0)
                join_terms.append(join_operator)
                col_2 = tokens.pop(0)
                if tokens[0] == ".":
                    dot = tokens.pop(0)
                    nme = tokens.pop(0)
                    join_terms.append(col_2+dot+nme)
                else:
                    join_terms.append(col_2)

            if tokens[0] == "WHERE":
                pop_and_check(tokens, "WHERE")
                operaters = [">", "<", "=", "!="]
                stmt = tokens.pop(0)
                if tokens[0] == ".":
                    dot = tokens.pop(0)
                    nme = tokens.pop(0)
                    clause.append(stmt+dot+nme)
                else:
                    clause.append(stmt)
                if tokens[0] in operaters:
                    clause.append(tokens[0])
                    tokens.pop(0)
                elif tokens[0] == "IS":
                    clause.append(tokens[0])
                    tokens.pop(0)
                    if tokens[0] == "NOT":
                        clause.append(tokens[0])
                        tokens.pop(0)
                val = tokens.pop(0)
                clause.append(val)
              
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            desc = False
            desc_lst = []
            while True: 
                # print(tokens)
                col = tokens.pop(0)
                    
                if not tokens:
                    order_by_columns.append(col)
                    break
                elif len(tokens) > 0:
                    if tokens[0] == ".":
                        dot = tokens.pop(0)
                        nme = tokens.pop(0)
                        order_by_columns.append(col+dot+nme)

                    elif tokens[0] == "DESC" and len(tokens) >= 0:
                        pop_and_check(tokens, "DESC")
                        desc = True
                        desc_lst.append((col,desc))
        
                        
                    else:
                        order_by_columns.append(col)
                # if not tokens:
                #     break
                
                if len(tokens) == 0:
                    break
                else:
                    pop_and_check(tokens, ",")
            # print(desc_lst)
            if self.SL is True and self.start_transaction is True:
                self.db_clone = copy.deepcopy(self.database)
                return self.db_clone.select(
                output_columns, table_name, table_name_2,
                order_by_columns,clause,distinct,
                join, join_terms, desc,desc_lst)
            
            return self.database.select(
                output_columns, table_name, table_name_2,
                order_by_columns,clause,distinct,
                join, join_terms,desc,desc_lst)
        
        def update(tokens):
            """
            Updates table rows
            """
            terms = []
            clause = []
            pop_and_check(tokens,"UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens,"SET")
            while True:
                update_terms = tokens[0:3]
                terms.append(update_terms)
                tokens.pop(0)
                tokens.pop(0)
                tokens.pop(0)
                if len(tokens) == 0 or tokens[0] == "WHERE":
                    break
                else:
                    pop_and_check(tokens, ",")

            # print(terms)
            if len(tokens) !=0:
                if tokens[0] == "WHERE":
                    pop_and_check(tokens, "WHERE")
                    operaters = [">", "<", "=", "!="]
                    stmt = tokens.pop(0)
                    if tokens[0] == ".":
                        dot = tokens.pop(0)
                        nme = tokens.pop(0)
                        clause.append(stmt+dot+nme)
                    else:
                        clause.append(stmt)
                    if tokens[0] in operaters:
                        clause.append(tokens[0])
                        tokens.pop(0)
                    elif tokens[0] == "IS":
                        clause.append(tokens[0])
                        tokens.pop(0)
                        if tokens[0] == "NOT":
                            clause.append(tokens[0])
                            tokens.pop(0)
                    val = tokens.pop(0)
                    clause.append(val)
            # print(clause)
            if self.RL is True and self.start_transaction is True:
                self.db_clone = self.database
                return self.db_clone.update_table(table_name,terms,clause)
            return self.database.update_table(table_name,terms,clause)
        
        def delete(tokens):
            """
            Delete rows from table
            """
            clause = []
            pop_and_check(tokens,"DELETE")
            pop_and_check(tokens,"FROM")
            table_name = tokens.pop(0)
            if len(tokens) !=0:           
                if tokens[0] == "WHERE":
                    pop_and_check(tokens, "WHERE")
                    operaters = [">", "<", "=", "!="]
                    stmt = tokens.pop(0)
                    if tokens[0] == ".":
                        dot = tokens.pop(0)
                        nme = tokens.pop(0)
                        clause.append(stmt+dot+nme)
                    else:
                        clause.append(stmt)
                    if tokens[0] in operaters:
                        clause.append(tokens[0])
                        tokens.pop(0)
                    elif tokens[0] == "IS":
                        clause.append(tokens[0])
                        tokens.pop(0)
                        if tokens[0] == "NOT":
                            clause.append(tokens[0])
                            tokens.pop(0)
                    val = tokens.pop(0)
                    clause.append(val)

            if self.RL is True and self.start_transaction is True:
                self.db_clone = self.database
                return self.db_clone.delete_from(table_name,clause)    
            return self.database.delete_from(table_name,clause)
        
        def drop(tokens):
            pop_and_check(tokens,"DROP")
            pop_and_check(tokens,"TABLE")
            if tokens[0] == "IF":
                pop_and_check(tokens,"IF") 
                pop_and_check(tokens,"EXISTS")
                table_name = tokens.pop(0)
                return self.database.drop_table(table_name)
            else:
                table_name = tokens.pop(0)
                return self.database.drop_table(table_name)

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT","UPDATE", "DELETE", "BEGIN", "COMMIT", "DROP", "ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            if self.start_transaction:
                if self.lock == True:
                    self.lock = False
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
                else: 
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
                    #             if self.SL == True or self.RL == True: 
                    # raise Exception("INVALID LOCK")
            if params is True and len(new_statement)!=0:
                # new_tokens = tokenize(new_statement)
                # new_tokens.pop()
                insert(new_statement,params)
            else:
                insert(tokens,params=False)
            return []
        elif tokens[0] == "UPDATE":
            if self.start_transaction:
                if self.lock == True:
                    self.lock = False
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
                else: 
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            if self.start_transaction:
                if self.lock == True:
                    self.lock = False
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
                else: 
                    if self.RL == True or self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.RL = True
            delete(tokens)
            return []
        elif tokens[0] == "DROP":
            drop(tokens)
            return []
        elif tokens[0] == "BEGIN":
            self.auto_commit = False
            self.start_transaction = True
            self.db_rollback = copy.deepcopy(self.database)
            if tokens[1] == "DEFFERED":
                self.lock = True
            elif tokens[1] == "IMMEDIATE":
                self.RL = True
            elif tokens[1] == "EXCLUSIVE":
                if self.SL == True or self.RL == True: 
                    raise Exception("INVALID LOCK")
                else:
                    self.XL = True
            else:
                self.lock = True
            return []
        elif tokens[0] == "COMMIT":
            self.auto_commit = True
            if self.start_transaction is False:
                raise Exception("INVALID TRANSACTION")
            if self.RL is True:
                self.RL = False
                self.XL = True
                self.database = self.db_clone
                self.XL = False
                self.start_transaction = False
                self.lock = False
                self.SL = False
            else:
                self.database = self.db_clone
                self.start_transaction = False
                self.RL = False                
                self.XL = False
                self.lock = False
                self.SL = False
            return []
        
        elif tokens[0] == "ROLLBACK":
            self.database = self.db_rollback
            self.start_transaction = False
            self.lock = False
            self.SL = False
            self.XL = False
            self.RL = False
            self.auto_commit = True
            return[]

        elif tokens[0] == "SELECT":
            if self.start_transaction:
                if self.lock == True:
                    self.lock = False
                    if self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.SL = True
                else: 
                    if self.XL == True:
                        raise Exception("INVALID LOCK") 
                    else:
                        self.SL = True
            return select(tokens)
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        return self.database.save_file()
        



def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}


    def check(self, table_name):
        if table_name in self.tables:
            raise Exception("TABLE ALREADY EXISTS")
        # self.tables.pop(table_name)
        return []

    def create_new_table(self, table_name, column_name_type_pairs,defaults):
        assert table_name not in self.tables
        table = Table(table_name, column_name_type_pairs,defaults)
        self.tables[table_name] = table
        return []


    def insert_into(self, table_name, row_contents,col_name,default,params):
        assert table_name in self.tables
        table = self.tables[table_name]
        if default == True:
            table.insert_defaults()
        elif params == True:
            table.insert_parameters(row_contents)
        else:
            table.insert_new_row(row_contents,col_name)
        return []
    
    def update_table(self,table_name, update_terms,clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update_rows(update_terms,clause=clause)
        return []
    
    def delete_from(self, table_name,clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete_rows(clause)
        return []
    
    def drop_table(self, table_name):
        if table_name not in self.tables:
            raise Exception("TABLE DOES NOT EXISTS")
        self.tables.pop(table_name)


    def select_min_max(self, min_or_max,table_name,order_val):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.get_min_max(min_or_max,order_val)
    
    def select(self, output_columns, table_name, table_name_2, order_by_columns, where_clause,distinct,join,join_terms,desc=False,desc_list=[]):
        assert table_name in self.tables
        table = self.tables[table_name]

        if join:
            assert table_name_2 in self.tables
            table_2 = self.tables[table_name_2]
            tab_name,table_col = join_terms[0].split(".")
            tab_name_2,table_col_2 = join_terms[2].split(".")
            table_row = table_2.get_row()
            return table.join_rows(table_row, tab_name, table_col, tab_name_2,table_col_2, order_by_columns,output_columns)
        
        return table.select_rows(output_columns, order_by_columns,where_clause,distinct,desc,desc_list)
    
    def save_file(self):
        data = {self.filename:{}}
        for tb in self.tables:
            table = self.tables[tb]
            table_val = table.convert_to_dict()
          
            data[self.filename] = {
                tb: 
                # {"Name": tb.name, "Columns": tb.column_names, "Column_Type": tb.column_types, "Defaults": tb.defaults, "Rows": tb.rows}
                table_val           
            }

        with open(self.filename, 'w') as f:
            json.dump(data, f, indent=4)

    


class Table:
    def __init__(self, name, column_name_type_pairs,defaults):
        self.name = name
        self.column_name_types_pairs = column_name_type_pairs
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.defaults = defaults
        self.rows = []
        

    def get_row(self):
        return self.rows
    
    def join_rows(self,table_2_row, tab_name, table_col, tab_name_2,table_col_2, order_by_columns,output_columns):
        def sort_rows(order_by_columns,tab_rows):
            new_order = []
            
            for col in order_by_columns:
                name_parts = col.split(".")
                if len(name_parts) == 2:
                    new_order.append(name_parts[1])
                elif len(name_parts) == 1:
                    new_order.append(name_parts[0])
            rows = []
            none_row = {}
            for row in tab_rows:
                if None not in row.values():
                    rows.append(row)
                else:
                    none_row = row
            
            sr = sorted(rows, key=itemgetter(*new_order))
          
            return sr
        
        for row_1 in self.rows:
            for row_2 in table_2_row:
                if row_1[table_col] == row_2[table_col_2]:
                    row_2.update(row_1)
    

        def generate_tuples(rows, output_columns):
            new_cols = []
            for col in output_columns:
                name_parts = col.split(".")
                if len(name_parts) == 2:
                    new_cols.append(name_parts[1])

                elif len(name_parts) == 1:
                    new_cols.append(name_parts[0])

            d = {}
            add_col = copy.copy(new_cols)
            add_col.append(table_col)
    
            if len(rows) != len(self.rows):
                for k in add_col:
                    d.update({k: None})
                rows.append(d)
            
            for row in rows:
                # for col in new_cols:
                #     if col not in:
                yield tuple(row[col] for col in new_cols)

        sorted_rows = sort_rows(order_by_columns,table_2_row)
        return generate_tuples(sorted_rows,output_columns)



    def convert_to_dict(self):
        table_dict =  {"Name": self.name, "Column_name_types_pairs": self.column_name_types_pairs, "Defaults": self.defaults, "Rows": self.rows}
        return table_dict
    
    def insert_parameters(self,param_rows):
        sub_rows = [list(i) for i in param_rows]
        for vals in sub_rows:
            row = dict(zip(self.column_names, vals)) 
            self.rows.append(row)

    def insert_defaults(self):
        idx = 0
        default_rows = [None] * len(self.column_names)
        sub_rows = []
        dfalt_val = [t[1] for t in self.defaults]
        dfalt_name = [t[0] for t in self.defaults]
        
        for col in self.column_names:
            if  col in dfalt_name:
                idx = self.column_names.index(dfalt_name[0])
                default_rows[idx]  = dfalt_val[0]
        sub_rows.append(default_rows)
                
        for vals in sub_rows:
            row = dict(zip(self.column_names, vals)) 
            self.rows.append(row)

    def insert_new_row(self, row_contents,col_name):
        # assert len(self.column_names) == len(row_contents)
        # for row in row_contents:
        
        if len(col_name) == 0: #checks for multiple columns
            num_col = len(self.column_names)
            sub_rows = [row_contents[i:i+num_col] for i in range(0, len(row_contents), num_col)] # splits rowcontent in terms of number of columns
            # print(sub_rows)
            for vals in sub_rows:
                row = dict(zip(self.column_names, vals)) 
                self.rows.append(row)
        else:
            num_col = len(col_name)
            sub_rows = [row_contents[i:i+num_col] for i in range(0, len(row_contents), num_col)]            
            col_idx = 0
            idx = 0
            dfalt_val = [t[1] for t in self.defaults]
            dfalt_name = [t[0] for t in self.defaults]
            if len(self.column_names) != len(row_contents):
                for col in self.column_names:
                    for vals in sub_rows:
                        if col not in col_name and col not in dfalt_name:
                            col_idx =  self.column_names.index(col)
                            col_name.insert(col_idx, col)
                            vals.insert(col_idx, None) 
                        elif col not in col_name and col in dfalt_name:
                            col_idx =  self.column_names.index(col)
                            col_name.insert(col_idx, col)
                            idx = self.column_names.index(dfalt_name[0])
                            vals.insert(idx, dfalt_val[0])
            # print(sub_rows)                   
            for vals in sub_rows:
                row = dict(zip(col_name, vals)) 
                self.rows.append(row)
        # print(self.rows)
         
    def update_rows(self, terms,clause=[]):
        def where(where_clause):
            where_rows = []
            name_parts = where_clause[0].split(".")
            if len(name_parts) == 2:
                where_clause[0] = name_parts[1]
            elif len(name_parts) == 1:
                where_clause[0] = name_parts[0] 
            for rows in self.rows:
                row_values = rows.get(where_clause[0])
                if (where_clause[1] == ">"and row_values is not None)  and row_values > where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "<"and row_values is not None)  and row_values < where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "="and row_values is not None)  and row_values == where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "!=" and row_values is not None)  and row_values != where_clause[2]:
                    where_rows.append(rows)
                elif where_clause[1] == "IS" and row_values is None:
                    where_rows.append(rows)
                elif len(where_clause) == 4: 
                    if where_clause[2] == "NOT" and row_values is not None:
                        where_rows.append(rows)
                # Return list of matching rows
            return where_rows
        # print(terms)
        if len(clause) > 0:
            
            where_rows = where(clause)
            # d = where(clause)
            for row in self.rows:
                for comp in where_rows: #compare rows form where clause to find row to change in self.rows
                    for term in terms:
                        if row == comp:
                            if term[0] in row:
                                row[term[0]] = term[2]
        else:
            for row in self.rows:
                for term in terms:
                    if term[0] in row:
                        row[term[0]] = term[2]

    def delete_rows(self,clause=[]):
        def where(where_clause):
            where_rows = []
            name_parts = where_clause[0].split(".")
            if len(name_parts) == 2:
                where_clause[0] = name_parts[1]
            elif len(name_parts) == 1:
                where_clause[0] = name_parts[0] 
            for rows in self.rows:
                row_values = rows.get(where_clause[0])
                if (where_clause[1] == ">"and row_values is not None)  and row_values > where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "<"and row_values is not None)  and row_values < where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "="and row_values is not None)  and row_values == where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "!=" and row_values is not None)  and row_values != where_clause[2]:
                    where_rows.append(rows)
                elif where_clause[1] == "IS" and row_values is None:
                    where_rows.append(rows)
                elif len(where_clause) == 4: 
                    if where_clause[2] == "NOT" and row_values is not None:
                        where_rows.append(rows)
                # Return list of matching rows
            return where_rows
        if len(clause) > 0:
            where_rows = where(clause)
            for row in self.rows[:]:
                for comp in where_rows:
                    if row == comp:
                        self.rows.remove(row)
        else:
            for row in self.rows[:]:
                self.rows.remove(row)
        

    def get_min_max(self, min_or_max, order_val):
        def sort_rows(order_col, min_max_rows):
                new_order = [order_col]
                return sorted(min_max_rows, key=itemgetter(*new_order))

        def generate_tuples(rows, output_columns):
            new_cols = []
            for col in output_columns:
                name_parts = col.split(".")
                if len(name_parts) == 2:
                    new_cols.append(name_parts[1])

                elif len(name_parts) == 1:
                    new_cols.append(name_parts[0])

            for row in rows:
                yield tuple(row[col] for col in new_cols)

        if min_or_max[0] == "MAX":
            key = min_or_max[1]
            key_lst = []
            key_lst.append(key)
            max_lst = []
            max_row = max(self.rows, key=lambda x: x[key])
            max_lst.append(max_row)
            sorted_rows = sort_rows(order_val, max_lst)
            return generate_tuples(sorted_rows,key_lst)
        

        elif min_or_max[0] == "MIN":
            key = min_or_max[1]
            key_lst = []
            key_lst.append(key)
            min_lst = []
            min_row = min(self.rows, key=lambda x: x[key])
            min_lst.append(min_row)
            sorted_rows = sort_rows(order_val, min_lst)
            return generate_tuples(sorted_rows, key_lst)

    def select_rows(self, output_columns, order_by_columns,where_clause,distinct,desc=False,desc_list=[]):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            for col in columns:
                name_parts = col.split(".")
                if len(name_parts) == 2:
                    assert name_parts[0] == self.name and name_parts[1] in self.column_names  

                elif len(name_parts) == 1:
                    assert name_parts[0] in self.column_names
            # assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns,desc,desc_list,where_rows,unique_val,distinct=False):
            if distinct and len(where_rows) == 0:
                new_order = []
                for col in order_by_columns:
                    name_parts = col.split(".")
                    if len(name_parts) == 2:
                        new_order.append(name_parts[1])
                    elif len(name_parts) == 1:
                        new_order.append(name_parts[0])

                # if desc == True and len(desc_list) != 0:


                return sorted(unique_val, key=itemgetter(*new_order))
            
            elif distinct and len(where_rows) > 0:
                new_order = []
                for col in order_by_columns:
                    name_parts = col.split(".")
                    if len(name_parts) == 2:
                        new_order.append(name_parts[1])
                    elif len(name_parts) == 1:
                        new_order.append(name_parts[0])
            
                return sorted(unique_val, key=itemgetter(*new_order))

            elif not distinct and len(where_rows) > 0:
                new_order = []
                for col in order_by_columns:
                    name_parts = col.split(".")
                    if len(name_parts) == 2:
                        new_order.append(name_parts[1])
                    elif len(name_parts) == 1:
                        new_order.append(name_parts[0])
                return sorted(where_rows, key=itemgetter(*new_order))
                           
            elif not distinct and len(where_rows) == 0:
                new_order = []

                for col in order_by_columns:
                    name_parts = col.split(".")
                    if len(name_parts) == 2:
                        new_order.append(name_parts[1])
                    elif len(name_parts) == 1:
                        new_order.append(name_parts[0])
                rows = []
                none_row = {}
                for row in self.rows:
                #     if None not in row.values(): 
                    rows.append(row)
                #     else:
                #         none_row = row

                
                if desc == True and len(desc_list) != 0:
                    desc_order = []
                    for tup in desc_list:
                        desc_order.append(tup[0])
                    sorted(rows, key=itemgetter(*desc_order), reverse=True)
                sr = sorted(rows, key=itemgetter(*new_order))
                # if len(none_row) !=0:
                #     sr.append(none_row)
                return sr
            
        
        def where(where_clause):
            where_rows = []
            name_parts = where_clause[0].split(".")
            if len(name_parts) == 2:
                where_clause[0] = name_parts[1]
            elif len(name_parts) == 1:
                where_clause[0] = name_parts[0] 
            for rows in self.rows:
                row_values = rows.get(where_clause[0])
                if (where_clause[1] == ">"and row_values is not None)  and row_values > where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "<"and row_values is not None)  and row_values < where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "="and row_values is not None)  and row_values == where_clause[2]:
                    where_rows.append(rows)
                elif (where_clause[1] == "!=" and row_values is not None)  and row_values != where_clause[2]:
                    where_rows.append(rows)
                elif where_clause[1] == "IS" and row_values is None:
                    where_rows.append(rows)
                elif len(where_clause) == 4: 
                    if where_clause[2] == "NOT" and row_values is not None:
                        where_rows.append(rows)
                # Return list of matching rows

            return where_rows
            
        def get_distinct_values(output_columns,where_vals,where = False):
            key = output_columns[0]
            if where:
                distinct_pairs = set()
                disctinct_rows_all = []
                disctinct_rows_specific = []
                for row in where_vals:
                    for k, v in row.items():
                        distinct_pairs.add((k, v))

                for k, v in distinct_pairs:   
                    disctinct_rows_all.append({k: v})

                for dra in disctinct_rows_all:
                    if key in dra:
                        disctinct_rows_specific.append(dra)
        
                return disctinct_rows_specific

            else:
                distinct_pairs = set()
                disctinct_rows_all = []
                disctinct_rows_specific = []
                for row in self.rows:
                    for k, v in row.items():
                        distinct_pairs.add((k, v))

                for k, v in distinct_pairs:   
                    disctinct_rows_all.append({k: v})

                for dra in disctinct_rows_all:
                    if key in dra:
                        disctinct_rows_specific.append(dra)
        
                return disctinct_rows_specific
        

        def generate_tuples(rows, output_columns):
            new_cols = []
            for col in output_columns:
                name_parts = col.split(".")
                if len(name_parts) == 2:
                    new_cols.append(name_parts[1])

                elif len(name_parts) == 1:
                    new_cols.append(name_parts[0])
            # print(rows)
            # print(output_columns)
            for row in rows:
                yield tuple(row[col] for col in new_cols)

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        where_rows = []
        if distinct and len(where_clause) == 0:
            unique_val = get_distinct_values(output_columns,where_vals=[],where=False)
            sorted_rows = sort_rows(order_by_columns,desc,desc_list,where_rows=[],unique_val=unique_val,distinct=True)
            return generate_tuples(sorted_rows, expanded_output_columns)

        elif distinct and len(where_clause) > 0:
            where_rows = where(where_clause)
            unique_val = get_distinct_values(output_columns,where_vals=where_rows,where=True)
            sorted_rows = sort_rows(order_by_columns, desc,desc_list, where_rows=[],unique_val=unique_val,distinct=True)
            return generate_tuples(sorted_rows, expanded_output_columns)

        elif not distinct and len(where_clause) > 0:
            where_rows = where(where_clause)
            sorted_rows = sort_rows(order_by_columns, desc,desc_list, where_rows=where_rows,unique_val=[],distinct=False)
            return generate_tuples(sorted_rows, expanded_output_columns)
        
        else:
            sorted_rows = sort_rows(order_by_columns, desc,desc_list, where_rows=[],unique_val=[],distinct=False)
            return generate_tuples(sorted_rows, expanded_output_columns)
        
        
def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            # if query[1] != "," and query[1] in (string.ascii_letters + "_"):
            #     print(query)
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)

            continue

        if query[0] in "(),;*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "<":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == ">":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "!":
            tokens.append(query[0]+query[1])
            query = query[2:]
            continue

        if query[0] == ".":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "?":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens
