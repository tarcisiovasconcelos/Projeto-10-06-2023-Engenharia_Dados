Documentação feita através do slide.pptx

equipe :
Gabriel Ramos Carodoso de Lima
Mylena Santos de Souza
Tarcisio Clovis Freitas de Vasconcelos

O projeto consiste em coletar dados da Organização Mundial da Saúde (OMS) e do Centro de Controle e Prevenção de Doenças dos Estados Unidos (CDC) sobre a Covid-19. Para isso, criamos um pipeline utilizando a ferramenta Apache Airflow, integrada ao PostgreSQL, e exibimos os dados com o Power BI.

Para realizar essa pipeline, desenvolvemos scripts que permitem a captura dos dados (Fetch), o processamento dos mesmos (Process) e a inserção dos dados tratados em um banco de dados criado no PostgreSQL, por meio de um script em Python (Insert).

Após a etapa de inserção, realizamos consultas no banco de dados, cumprindo o requisito de Merge nas tabelas, e geramos dois arquivos CSV. Esses arquivos serão utilizados posteriormente pelo Power BI para a sumarização dos dados.

#só existe uma dag chamada pipeline que ela chama as funções dos script como tasks então temos mais de 5 tasks nessa pipeline


Perguntas: 
- Qual é o número máximo de casos de COVID-19 nos EUA, segundo a OMS e o CDC, por ano, trimestre, mês e dia?

- Qual é o nível de divergência entre as fontes no mesmo período?

- Quais são os 10 estados onde ocorreram o maior número de casos nos EUA?

- Liste o número máximo de casos por estado.

