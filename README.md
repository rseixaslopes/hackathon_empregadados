# Hackathon Empregadados
Projeto de Pipeline de Dados em Python e Dashboards no Power BI

O  projeto consiste em extrair os dados financeiros e de vendas de duas empresas (ALUCAR e CONSIGCAR), que são alimentadas periodicamente em 2 documentos mantidos na cloud do cliente, sendo uma planilha salva no Microsoft Sharepoit e a outra planilha no Google Docs.

Toda a ETL é executada pelo script ../scripts/Injestao_Dados.py, que persiste as tabelas transformadas em arquvos parquet num Blob Azure.

O arquivo Relatorios_01_v6.pbix nativo do Microsoft Power BI contém: 
- As tabelas vinculadas aos arquivos de dados parquet, armazenados no Blob Azures;
- As tabelas de apoio, tais como a dimenção dcalendário, criada com a liguagem DAX;
- Os relacionamentos entre as tabelas fatos e dimensão;
- 5 Dasheboards gerenciais, sendo eles: VISÃO GERAL, FORECAST, PRL, PROMOÇÃO e DRE.


## VISÃO GERAL
Apresenta graficamente a situação financeira do ano até a data de atual: 
- O total das receitas mensais das duas empresas.
- O total das despesas mensais das duas empresas.
- O fluxo de caixa por empresa.
- O Fluxo de caixa consolidado do grupo
- ![Visão Geral](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/VisaiGeral.jpg)

<b>Também é possível filtrar determinados meses para detalhar os dados por semana, fazendo o drown drill nos gráficos.</b>
![Visão Geral Semanal](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/VisaiGeralSemanal.jpg)

## FORECAST
Esse dashboard representa:
- As receitas estimadas.
- As despesas estimadas.
- O fluxo de caixa projetado, onde juntamos os dados realizados e estimados no mesmo gráfico, representando os resultados esperados no ano.
- O fluxo de caixa projetado do grupo
![FORECAST](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/Forecast.jpg)

## PRL
Esse dashboard auxilia o gestor de ventas acompanhar as metas definidas para cada empresa e o resultado das equipes.
- Velocímetro de desempenho para cada empresa, até a data atual, representando o total de vendas, a primeira linha é a meta 1 e o final a meta 2 do mês corrente.
- Últimos 4 meses de vendas por vendedor.
- Vendas acumiladas por mês e as linhas referentes a meta 1 e a meta 2, para acompanhar a evolução e resultado das equipes
- TOP 5 dos melhores vendedores do mês atual
- Tabela das metas definidas e acumuladas mensalmente para cada empresa.
![PLR](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/PLR.jpg)

## PROMOÇÃO
Dashboard interativo, onde o gestor pode definir um período promocional e metas para estimular a equipe a melhorar os números de vendas.
Campos editáveis:
- Meta Individual.
- Meta Equipe.
- Meta Extraordinária.
- Período.

Gráficos:
  - Meta Individual. Exibe o total de vendas acumulada por dia e vendedor e a linha é a meta diária acumulada.
  - Meta Equipe. Exibe o total de vendas acumuladas por dia e empresa e a linha azul é a meta da equipe e a linha laranja é a meta extraordinária.
  - Vendas por vendedor.
  ![PROMOÇÃO](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/Promocao.jpg)

## DRE
Apresenta a matriz financeira das empresas durante o ano todo, podendo filtrar cada empresa.
![DRE](https://github.com/rseixaslopes/hackathon_empregadados/blob/main/images/DRE.jpg)
