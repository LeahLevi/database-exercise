import csv
import os
from dataclasses import dataclass
from typing import List, Any, Dict
from db_api import DBField, DBTable, SelectionCriteria

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DataBase:
    num_of_tables = 0  # : int
    tables_array = []  #: List[DBTable]

    # Put here any instance information needed to support the API
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        self.num_of_tables += 1
        self.tables_array.append(DBTable(table_name, fields, key_field_name))
        with open(f"{table_name}.csv", "w") as new_file:
            csv_writer = csv.writer(new_file)
            data_in_line = []
            for field in fields:
                data_in_line.append(field.name)
            csv_writer.writerow(data_in_line)
        return self.tables_array[self.num_of_tables - 1]

    def num_tables(self) -> int:
        return self.num_of_tables

    def get_table(self, table_name: str) -> DBTable:
        for table in self.tables_array:
            if table.name == table_name:
                return table
        raise FileNotFoundError("There is no table with this name")

    def get_index_table(self, table_name: str) -> int:
        for i, table in enumerate(self.tables_array):
            if table.name == table_name:
                return i

    def delete_table(self, table_name: str) -> None:
        os.remove(f"{table_name}.csv")
        index = self.get_index_table(table_name)
        for i in range(index, self.num_of_tables - 1):
            self.tables_array[i] = self.tables_array[i + 1]
        self.tables_array = self.tables_array[:self.num_of_tables - 1]
        self.num_of_tables -= 1

    def get_tables_names(self) -> List[Any]:
        names_list = []
        for table in self.tables_array:
            names_list.append(table.name)
        return names_list

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
