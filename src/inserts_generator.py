import pandas as pd
import json
from typing import List, Callable


def inserts_generator(table_name: str, dataFrameCsvPath: str, tableDefinitionPath: str, derivateFunctions: List[Callable] = None):
    """
Gera strings de inserção para uma tabela a partir de um arquivo CSV e uma definição de tabela em formato JSON.
Ele também suporta funções derivadas para transformar os valores das colunas antes da inserção. Essas strings
de inserção podem ser usadas posteriormente para inserir os dados em uma tabela de um banco de dados, como o Cassandra.

• table_name:
    Nome da tabela que irá receber os inserts. Esse nome será utilizado para nomear o JSON que contém as strings de insert values.

• dataFrameCsvPath:
    Caminho para o csv que contém os dados. Garanta que o Pandas possa ler esse arquivo por meio do método pandas.read_csv().

• tableDefinitionPath:
    Caminho para o JSON que especifica a definição da tabela.

• derivateFunctions:
    As funções em derivateFunction devem receber um Pandas Series do mesmo tipo que o especificado em dataFrameCsvPath.
    Além disso, a função deve retornar exatamente a string a ser inclusa em nas strings de INSERT. Mesmo as aspas simples
    próprias de campos textuais devem ser inclusas nessa string de retorno. Você deve tratar possíveis NaN na sua função.
    Nesses casos, retorne a string NULL. A não ser que seu atributo derivado seja uma Cluster Key, nesse caso, retorne o
    que for adequado (talvez um valor padrão). 
    """

    dataFrame = pd.read_csv(dataFrameCsvPath)

    with open(tableDefinitionPath) as file:
        tableDefinition: dict[str, str] = json.load(file)

    columns = tableDefinition.keys()

    insertStrings = []

    for index, row in dataFrame.iterrows():

        insertString = '('

        for column in columns:

            type = tableDefinition[column]
            
            if 'der_' in type:

                dFunc_name = type.split('_')[1]
                dFunc = None

                for func in derivateFunctions:
                    if func.__name__ == dFunc_name:
                        dFunc = func
                        break
                    
                if dFunc == None:
                    raise ValueError(f"A função '{dFunc_name}' não foi encontrada na lista de funções.")

                value = dFunc(row)
                insertString = insertString + value + ','

            else:

                isClusterKey = False

                if 'clusK_' in type:

                    isClusterKey = True
                    type,defaultValue = type.split('_')[1:]

                if pd.isna(row[column]):
                    
                    if isClusterKey:

                        if type == 'str' or type == 'date':
                            insertString = insertString + f"'{defaultValue}'" + ','
                        else:
                            insertString = insertString + defaultValue + ','

                    else:
                        insertString = insertString + 'NULL' + ','

                elif type == 'set' or type == 'list':
                    
                    multval = row[column].split(',')
                    multval = [value.strip() for value in multval]
                    multval = [value.replace("'",'\u2019') for value in multval]

                    if type == 'set':
                        insertString = insertString + str(set(multval)) + ','
                    else:
                        insertString = insertString + str(multval) + ','

                elif type == 'date':

                    if '-' not in row[column]:
                        insertString = insertString + f"'{row[column]}-1-1'" + ','
                    else:
                        insertString = insertString + f"'{row[column]}'" + ','

                elif type == 'str':

                    stringText = row[column].replace("'",'\u2019')
                    insertString = insertString + f"'{stringText}'" + ','
                
                else:
                    insertString = insertString + str(row[column]) + ','

        insertString = insertString.rstrip(',')
        insertString = insertString + ')'
        insertStrings.append(insertString)
    
    json_insertStrings = json.dumps(insertStrings, separators=(",\n", ": "))

    with open(f'insert_values/{table_name}.json', 'w') as file:
        file.write(json_insertStrings)
