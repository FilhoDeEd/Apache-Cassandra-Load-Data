import json
import re
from cassandra.cluster import Cluster
from inserts_generator import inserts_generator
from cassandra.query import BatchStatement
from pandas import Series, isna


def getMainGenre(row: Series) -> str:

      if isna(row['genre']):
            return 'undefined'
      else:
            multval = row['genre'].split(',')
            multval = [value.strip() for value in multval]
            mainGenre = multval[0]
            mainGenre = f"'{mainGenre}'"
            return mainGenre

def getMainCountry(row: Series) -> str:

      if isna(row['country']):
            return 'undefined'
      else:
            multval = row['country'].split(',')
            multval = [value.strip() for value in multval]
            mainCountry = multval[0]
            mainCountry = f"'{mainCountry}'"
            return mainCountry

def parseBudgetToFloat(row: Series) -> str:

      if isna(row['budget']):
            return 'NULL'
      else:
            value = row['budget']
            value = re.sub(r'[^0-9]', '', value)
            value = float(value)
            return str(value)

def parseIncomeToFloat(row: Series) -> str:
      
      if isna(row['worldwide_gross_income']):
            return 'NULL'
      else:
            value = row['worldwide_gross_income']
            value = re.sub(r'[^0-9]', '', value)
            value = float(value)
            return str(value)
      

keyspace_name = 'mainkeyspace'
table_name = 'film_by_genre'
batchSize = 500
tuplesAmount = 25000
derivateFunctions = [getMainGenre, parseBudgetToFloat, parseIncomeToFloat]

def main():
      inserts_generator(table_name, 'csv/all_df.csv', f'insert_definitions/{table_name}_def.json',derivateFunctions)

      cluster = Cluster(['localhost'])
      session = cluster.connect()

      with open(f'insert_values/{table_name}.json') as file:
            insertStringsValues = json.load(file)

      with open(f'insert_definitions/{table_name}_def.json') as file:
            insertDefinition = json.load(file)

      columns = insertDefinition.keys()

      insertStringSchema = f'INSERT INTO {keyspace_name}.{table_name} ('

      for column in columns:
            insertStringSchema = insertStringSchema + column + ','

      insertStringSchema = insertStringSchema.rstrip(',')
      insertStringSchema = insertStringSchema + ') VALUES '

      index = 0
      batch = BatchStatement()
      for i in range(int(tuplesAmount/batchSize)):
            batch.clear()
            for j in range(batchSize):
                  insertString = insertStringSchema + insertStringsValues[index] + ';'
                  batch.add(insertString)
                  index += 1

            session.execute(batch)


if __name__ == '__main__': 
      main()
