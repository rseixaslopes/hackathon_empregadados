# hackathon_empregadados
Projeto Pipeline de Dados em Python e Dashboards no Power BI

O  projeto consiste em extrair os dados de 2 documentos na cloud do cliente, sendo um no Microsoft Sharepoit e o outro no Google Docs.

Toda a ETL é executada pelo script /scripts/Injestao_Dados.py, que persiste os dados em arquvos parquet num Blob Azure.

O arquivo Relatorios_01_v6.pbix nativo do Microsoft Power BI contém: 
- As tabelas vinculadas aos arquivos de dados parquet, armazenados no Blob Azures;
- As tabelas de apoio, tais como a dimenção dcalendário, criada com a liguagem DAX;
- Os relacionamentos entre as tabelas fatos e dimensão;
- 5 Dasheboards gerenciais.

- ## VISÃO GERAL

- ![Visão Geral] 
