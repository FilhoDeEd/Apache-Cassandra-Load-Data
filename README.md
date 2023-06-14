# Apache-Cassandra-Load-Data
O projeto consiste em uma solução para facilitar a inserção de dados em uma tabela do Cassandra a partir de um arquivo CSV. Ele utiliza o Pandas para ler o arquivo CSV e o módulo inserts_generator para gerar strings de inserção com base em definições de tabela fornecidas em um arquivo JSON.

## Requisitos para Executar o Software

### Banco de Dados Cassandra

Certifique-se de ter o Cassandra instalado e em execução localmente. O software depende de uma conexão com o Cassandra para realizar as operações de inserção.

### Ambiente Anaconda ou Miniconda (opcional)

Recomenda-se a criação de um ambiente isolado usando o Anaconda ou Miniconda para instalar as dependências necessárias. Isso ajuda a evitar conflitos com outras bibliotecas e versões do Python.

#### Criação do Ambiente

1. Abra o terminal ou prompt de comando e navegue até o diretório do projeto, onde o arquivo `environment.yml` está localizado.
2. Execute o seguinte comando para criar o ambiente:

`conda env create -f environment.yml`

3. Aguarde até que todas as dependências sejam instaladas.

#### Ativação do Ambiente

Após a conclusão da criação do ambiente, ative-o usando o seguinte comando:

`conda activate pyCassandra`

Agora você está pronto para executar o software com todas as dependências instaladas corretamente no ambiente.

### Obtendo os dados

Os dados para este projeto podem ser obtidos no site Kaggle. Você pode acessar os dados do conjunto de dados "IMDB Movies Dataset" por meio do seguinte link: [IMDB Movies Dataset](https://www.kaggle.com/datasets/soumyasacharya/imdb-movies-dataset).

Neste conjunto de dados, você encontrará informações sobre filmes, como títulos, gêneros, países de produção, duração, orçamento, receita bruta mundial, classificação, entre outros. Esses dados podem ser utilizados para realizar inserções em uma tabela de um banco de dados, como o Cassandra, utilizando o software mencionado neste projeto.

Certifique-se de baixar o conjunto de dados apropriado e adaptar o caminho do arquivo CSV no código, de acordo com o local onde você salvou os dados em sua máquina.

## Função `inserts_generator`

A função `inserts_generator` é responsável por gerar strings de inserção (insert) para uma tabela de banco de dados a partir de um arquivo CSV e uma definição da tabela em formato JSON.

### Parâmetros

- `table_name` (str): Nome da tabela que irá receber os inserts. Esse nome será utilizado para nomear o JSON que contém as strings de insert values.
- `dataFrameCsvPath` (str): Caminho para o arquivo CSV que contém os dados. Garanta que o Pandas possa ler esse arquivo por meio do método `pandas.read_csv()`.
- `tableDefinitionPath` (str): Caminho para o arquivo JSON que especifica a definição da tabela.
- `derivateFunctions` (List[Callable], opcional): Uma lista de funções derivadas. As funções em `derivateFunctions` devem receber um Pandas Series do mesmo tipo que o especificado em `dataFrameCsvPath`. Além disso, a função deve retornar exatamente a string a ser inclusa nas strings de insert values. Mesmo as aspas simples próprias de campos textuais devem ser inclusas nessa string de retorno. Você deve tratar possíveis valores NaN na sua função. Nesses casos, retorne a string `NULL`, a menos que o atributo derivado seja uma Cluster Key, nesse caso, retorne o valor adequado (talvez um valor padrão). O arquivo main.py possui exemplos de como definir essas funções.

### Funcionamento

1. Importa os módulos necessários: `pandas` para manipulação de dados em formato tabular, `json` para trabalhar com arquivos JSON e `typing` para fornecer tipos de dados.
2. Carrega os dados do arquivo CSV especificado em `dataFrameCsvPath` usando o método `pd.read_csv()` do Pandas e armazena-os em um objeto DataFrame chamado `dataFrame`.
3. Abre o arquivo JSON especificado em `tableDefinitionPath` usando a função `open()` e carrega o conteúdo em um dicionário chamado `tableDefinition` usando a função `json.load()`.
4. Obtém as colunas da tabela a partir das chaves do dicionário `tableDefinition`.
5. Inicializa uma lista vazia chamada `insertStrings` que será usada para armazenar as strings de inserção geradas.
6. Itera sobre as linhas do DataFrame usando o método `iterrows()`, que retorna um iterador que produz um índice de linha e uma série contendo os valores de cada linha.
7. Inicializa uma string `insertString` que será usada para construir a string de inserção para cada linha.
8. Para cada coluna na tabela:
   - Verifica se o tipo da coluna contém a substring "der_" indicando que é um atributo derivado.
   - Se for um atributo derivado, procura uma função correspondente na lista `derivateFunctions` com base no nome especificado no tipo.
   - Se a função não for encontrada na lista, uma exceção `ValueError` é lançada.
   - A função derivada é chamada com a série atual da linha e o valor de retorno é adicionado à string de inserção.
   - Caso contrário, verifica se o tipo da coluna contém a substring "clusK_" indicando que é uma chave de cluster.
   - Se for uma chave de cluster, extrai o tipo e o valor padrão da substring.
   - Verifica se o valor na coluna é um valor nulo (`NaN`) e, se for o caso:
     - Se for uma chave de cluster, adiciona o valor padrão adequado à string de inserção.
     - Caso contrário, adiciona a string "NULL" à string de inserção.
   - Caso contrário, verifica o tipo da coluna e adiciona o valor correspondente à string de inserção. Os tipos podem ser "set", "list", "date", "str" ou outros, dependendo da definição da tabela.
   - A string de inserção é construída adicionando os valores separados por vírgula entre parênteses.
   - A string de inserção resultante é adicionada à lista `insertStrings`.
9. Converte a lista `insertStrings` em uma representação JSON usando a função `json.dumps()` com formatação personalizada para separadores.
10. Abre um arquivo no diretório "insert_values" com o nome da tabela especificada e a extensão ".json" e escreve a representação JSON das strings de inserção nesse arquivo.

Esse código pode ser utilizado para facilitar a geração de strings de inserção para um banco de dados a partir de dados em um arquivo CSV. As strings de inserção geradas podem ser usadas posteriormente para inserir os dados em uma tabela do banco de dados.

## Exemplo de como compor o JSON de definição da tabela

### JSON da tabela film_by_genre

{<br>
    "main_genre": "der_getMainGenre",<br>
    "date_published": "date",<br>
    "title": "clusK_str_undefined",<br>
    "imdb_title_id": "str",<br>
    "genre": "list",<br>
    "country": "set",<br>
    "duration": "int",<br>
    "production_company": "str",<br>
    "director": "list",<br>
    "actors": "set",<br>
    "writer": "set",<br>
    "description": "str",<br>
    "original_title": "str",<br>
    "budget": "der_parseBudgetToFloat",<br>
    "worldwide_gross_income": "der_parseIncomeToFloat",<br>
    "metascore": "float",<br>
    "votes": "int",<br>
    "language": "list"<br>
}

O JSON de exemplo especifica a definição de uma tabela, onde cada atributo representa uma coluna na tabela real. A ordem e os nomes dos atributos no JSON devem corresponder exatamente aos da tabela. Aqui está uma descrição dos atributos do JSON:

- `"main_genre": "der_getMainGenre"`: A coluna "main_genre" será derivada usando a função "getMainGenre". Essa função recebe uma série Pandas e retorna o valor apropriado para a coluna. Procure pela definição dessa função no arquivo `main.py`.

- `"date_published": "date"`: A coluna "date_published" será do tipo "date".

- `"title": "clusK_str_undefined"`: A coluna "title" será uma chave de cluster do tipo texto (text ou varchar). Chaves de cluster não podem ser nulas, assim, se um NaN for econtrado no data frame, o valor inserido será o padrão. Nesse caso, o padrão é 'undefined'.

- `"imdb_title_id": "str"`: A coluna "imdb_title_id" será do tipo texto (text ou varchar).

- `"genre": "list"`: A coluna "genre" será do tipo lista de texto (list\&lttext>).

- `"country": "set"`: A coluna "country" será do tipo conjunto de texto (set\&lttext>).

- `"duration": "int"`: A coluna "duration" será do tipo inteiro (int).

- `"production_company": "str"`: A coluna "production_company" será do tipo texto (text ou varchar).

- `"director": "list"`: A coluna "director" será do tipo lista de texto (list\&lttext>).

- `"actors": "set"`: A coluna "actors" será do tipo conjunto de texto (set\&lttext>).

- `"writer": "set"`: A coluna "writer" será do tipo conjunto de texto (set\&lttext>).

- `"description": "str"`: A coluna "description" será do tipo texto (text ou varchar).

- `"original_title": "str"`: A coluna "original_title" será do tipo texto (text ou varchar).

- `"budget": "der_parseBudgetToFloat"`: A coluna "budget" será derivada usando a função "parseBudgetToFloat". Procure essa função no arquivo `main.py`.

- `"worldwide_gross_income": "der_parseIncomeToFloat"`: A coluna "worldwide_gross_income" será derivada usando a função "parseIncomeToFloat". Procure essa função no arquivo `main.py`.

- `"metascore": "float"`: A coluna "metascore" será do tipo float.

- `"votes": "int"`: A coluna "votes" será do tipo inteiro (int).

- `"language": "list"`: A coluna "language" será do tipo lista de texto (list\&lttext>).

Essa definição do JSON especifica a estrutura da tabela, incluindo os tipos de dados das colunas, bem como as funções derivadas, quando aplicável, para obter os valores das colunas.

A função inserts_generator foi projetada para lidar somente com colunas do tipo lista (list) e conjunto (set) contendo valores textuais. Não tente utilizá-la para gerar list\&ltint>, set\&ltfloat> ou qualquer outro tipo multivalorado sem antes alterar como a lógica da função funciona.

## Função `main`

A função `main` no arquivo `main.py` é responsável pela execução principal do seu software. Ela realiza as seguintes etapas:

1. Chama a função `inserts_generator` para gerar os valores dos inserts com base nos arquivos CSV e nas definições da tabela. Essa função cria um arquivo JSON contendo os valores de inserção para cada linha do CSV.

2. Cria uma conexão com o banco de dados Cassandra, usando a classe `Cluster` e o método `connect()`. Certifique-se de ter a biblioteca `cassandra-driver` instalada para poder usar essas funcionalidades.

3. Carrega os valores de inserção do arquivo JSON gerado anteriormente e as definições da tabela do arquivo JSON correspondente.

4. Monta a string de inserção da tabela definindo o esquema da consulta. Percorre as colunas da tabela e as adiciona à string, separadas por vírgula.

5. Inicia um loop para inserir os valores em lotes (batches) no banco de dados. A cada iteração do loop, cria um objeto `BatchStatement` vazio para armazenar as instruções de inserção em lote.

6. Dentro do loop, itera pelo número de inserções em cada lote (definido pela variável `batchSize`) e adiciona as instruções de inserção ao objeto `BatchStatement` usando o método `add()`.

7. Executa o lote de inserções chamando o método `execute()` na sessão do banco de dados, passando o objeto `BatchStatement` como parâmetro.

8. Repete as etapas 5 a 7 até que todas as inserções tenham sido realizadas.

Essa função permite a inserção eficiente de um grande número de registros no banco de dados Cassandra, dividindo-os em lotes menores. Isso ajuda a melhorar o desempenho e a evitar possíveis problemas de sobrecarga.

Certifique-se de ter configurado corretamente as informações de conexão com o banco de dados no código, como o endereço do cluster Cassandra, o nome do keyspace e o nome da tabela.
