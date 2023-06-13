# Apache-Cassandra-Load-Data
O projeto consiste em uma solução para facilitar a inserção de dados em uma tabela do Cassandra a partir de um arquivo CSV. Ele utiliza o Pandas para ler o arquivo CSV e o módulo inserts_generator para gerar strings de inserção com base em definições de tabela fornecidas em um arquivo JSON.

A função inserts_generator recebe o nome da tabela, o caminho para o arquivo CSV contendo os dados, o caminho para o arquivo JSON com a definição da tabela e uma lista opcional de funções derivadas. As funções derivadas são responsáveis por transformar os valores das colunas do CSV em formatos específicos para inserção no Cassandra. Por exemplo, algumas funções são fornecidas no código de exemplo, como getMainGenre, parseBudgetToFloat e parseIncomeToFloat, que realizam transformações nos valores das colunas antes de serem inseridos.

## Função `inserts_generator`

A função `inserts_generator` é responsável por gerar strings de inserção (insert) para uma tabela de banco de dados a partir de um arquivo CSV e uma definição da tabela em formato JSON.

### Parâmetros

- `table_name` (str): Nome da tabela que irá receber os inserts. Esse nome será utilizado para nomear o JSON que contém as strings de insert values.
- `dataFrameCsvPath` (str): Caminho para o arquivo CSV que contém os dados. Garanta que o Pandas possa ler esse arquivo por meio do método `pandas.read_csv()`.
- `insertDefinitionPath` (str): Caminho para o arquivo JSON que especifica a definição da tabela.
- `derivateFunctions` (List[Callable], opcional): Uma lista de funções derivadas. As funções em `derivateFunctions` devem receber um Pandas Series do mesmo tipo que o especificado em `dataFrameCsvPath`. Além disso, a função deve retornar exatamente a string a ser inclusa nas strings de INSERT. Mesmo as aspas simples próprias de campos textuais devem ser inclusas nessa string de retorno. Você deve tratar possíveis valores NaN na sua função. Nesses casos, retorne a string `NULL`, a menos que o atributo derivado seja uma Cluster Key, nesse caso, retorne o valor adequado (talvez um valor padrão).

### Funcionamento

1. Importa os módulos necessários: `pandas` para manipulação de dados em formato tabular, `json` para trabalhar com arquivos JSON e `typing` para fornecer tipos de dados.
2. Carrega os dados do arquivo CSV especificado em `dataFrameCsvPath` usando o método `pd.read_csv()` do Pandas e armazena-os em um objeto DataFrame chamado `dataFrame`.
3. Abre o arquivo JSON especificado em `insertDefinitionPath` usando a função `open()` e carrega o conteúdo em um dicionário chamado `insertDefinition` usando a função `json.load()`.
4. Obtém as colunas da tabela a partir das chaves do dicionário `insertDefinition`.
5. Inicializa uma lista vazia chamada `insertStrings` que será usada para armazenar as strings de inserção geradas.
6. Itera sobre as linhas do DataFrame usando o método `iterrows()`, que retorna um iterador que produz um índice de linha e uma série contendo os valores de cada linha.
7. Inicializa uma string `insertString` que será usada para construir a string de inserção para cada linha.
8. Para cada coluna na tabela:
   - Verifica se o tipo da coluna contém a substring "der_" indicando que é um atributo derivado.
   - Se for um atributo derivado, procura uma função correspondente na lista `derivateFunctions` com base no nome especificado no tipo.
   - Se a função não for encontrada na lista, uma exceção `ValueError` é lançada.
   - A função derivada é chamada com a série atual da linha e o valor de retorno é adicionado à string de inserção.
   - Caso contrário, verifica se o tipo da coluna contém a substring "clusK_" indicando que é uma chave de cluster.
   - Se for uma chave de cluster, extrai o tipo e o valor padrão

 da substring.
   - Verifica se o valor na coluna é um valor nulo (`NaN`) e, se for o caso:
     - Se for uma chave de cluster, adiciona o valor padrão adequado à string de inserção.
     - Caso contrário, adiciona a string "NULL" à string de inserção.
   - Caso contrário, verifica o tipo da coluna e adiciona o valor correspondente à string de inserção. Os tipos podem ser "set", "list", "date", "str" ou outros, dependendo da definição da tabela.
   - A string de inserção é construída adicionando os valores separados por vírgula entre parênteses.
   - A string de inserção resultante é adicionada à lista `insertStrings`.
9. Converte a lista `insertStrings` em uma representação JSON usando a função `json.dumps()` com formatação personalizada para separadores.
10. Abre um arquivo no diretório "insert_values" com o nome da tabela especificada e a extensão ".json" e escreve a representação JSON das strings de inserção nesse arquivo.

Esse código pode ser utilizado para facilitar a geração de strings de inserção para um banco de dados a partir de dados em um arquivo CSV. As strings de inserção geradas podem ser usadas posteriormente para inserir os dados em uma tabela do banco de dados.
