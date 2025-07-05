import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem
from datetime import date
import requests
import numpy as np

# PARAMETERS

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)
dia_hoje = pd.Timestamp.now()
dia_hoje = dia_hoje.strftime('%Y-%m-%d')

mapa_meses = {
    'JANEIRO': 1, 'FEVEREIRO': 2, 'MARÇO': 3, 'ABRIL': 4,
    'MAIO': 5, 'JUNHO': 6, 'JULHO': 7, 'AGOSTO': 8,
    'SETEMBRO': 9, 'OUTUBRO': 10, 'NOVEMBRO': 11, 'DEZEMBRO': 12
}

# Extract DESPESAS

sheet_id = "1cucnW4yVosO5n5BFgwXYv6rVy8yj6NTasM83RTCMOug"
despesas = "1859279676"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={despesas}"
df = pd.read_csv(url,header=1)
df_filtrado = df[(df['DESPESAS'] != 'TOTAL') & (df['DESPESAS']!='DESPESAS')]
index_divisao = df[df['janeiro'].str.contains('CONSIGCAR', case=False, na=False)].index[0]


df_alucar_despesas = df_filtrado.iloc[:index_divisao]
df_alucar_despesas_filtrado = df_alucar_despesas[~df_alucar_despesas['DESPESAS'].isna()]
df_alucar_despesas_filtrado_melted = df_alucar_despesas_filtrado.melt(id_vars='DESPESAS', var_name='MÊS', value_name='VALOR')
df_alucar_despesas_filtrado_melted['EMPRESA']='ALUCAR'

df_consigcar_despesas = df_filtrado.iloc[index_divisao:]
df_consigcar_despesas_melted = df_consigcar_despesas.melt(id_vars='DESPESAS', var_name='MÊS', value_name='VALOR')
df_consigcar_despesas_melted['EMPRESA']='CONSIGCAR'

def categoria_despesa(despesa):
    if 'Gustavo' in despesa \
        or 'Fernanda' in despesa\
        or 'Marcelo' in despesa\
        or 'Leticia' in despesa\
        or 'Rafael' in despesa\
        or 'Rodrigo' in despesa\
        or 'Thiago' in despesa\
        or 'Lucas' in despesa\
        or 'Mariana' in despesa\
        or 'João' in despesa\
        or 'Ana' in despesa:
        return 'Despesas de Pessoal'
    elif 'Aluguel' in despesa\
        or 'energia elétrica' in despesa\
        or 'água e esgoto' in despesa\
        or 'telefonia' in despesa:
        return 'Despesas Administrativas'
    elif 'Impostos e taxas' in despesa:
        return 'Despesas Tributárias'
    elif 'matéria-prima' in despesa\
        or 'transporte e logística' in despesa\
        or 'equipamentos' in despesa\
        or 'mecanicos':
        return 'Despesas Operacionais'
    elif 'marketing' in despesa\
        or 'consultorias' in despesa\
        or 'publicidade' in despesa:
        return 'Despesas de Vendas e Marketing'
    elif 'taxas' in despesa or 'juros' in despesa:
        return 'Despesas Financeiras'
    elif 'Inteligência Artificial' in despesa\
        or 'CRM' in despesa\
        or 'softwares ' in despesa:
        return 'Despesas com Tecnologia'
    elif 'Seguros' in despesa\
        or 'Treinamento' in despesa\
        or 'viagens' in despesa:
        return 'Despesas Gerais'
    else:
        return 'Outras Despesas'

df_despesas_total = pd.concat([df_alucar_despesas_filtrado_melted, df_consigcar_despesas_melted], ignore_index=True)
df_despesas_total['VALOR'] = (df_despesas_total['VALOR']
    .str.replace('R\$', '', regex=True) 
    .str.replace('.', '', regex=False)  
    .str.replace(',', '.', regex=False) 
    .str.strip()                        
    .astype(float)                      
)

df_despesas_total['DATA'] = pd.to_datetime({'year': 2025, 'month': df_despesas_total['MÊS'].str.upper().map(mapa_meses), 'day': 1})
df_despesas_total['DATA'] = df_despesas_total['DATA']+ pd.offsets.MonthEnd(0)
df_despesas_total['CONTA'] = df_despesas_total['DESPESAS'].apply(categoria_despesa)
df_despesas_total['STATUS'] = np.where(df_despesas_total['DATA'] <= dia_hoje,'Quitado','Estimado')
df_despesas_final_ordenadas = df_despesas_total.sort_values(by=['EMPRESA','DATA','CONTA','DESPESAS'],ascending=(True,True,True,True))

# Extract RECEITAS

sheet_id = "1cucnW4yVosO5n5BFgwXYv6rVy8yj6NTasM83RTCMOug"
receita = "373473243"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={receita}"
df_receita = pd.read_csv(url,header=0)

df_alucar_receitas_filtrado = df_receita.loc[:, 'Nome (Alucar)':'Valor\nReceita']
df_alucar_receitas_filtrado = df_alucar_receitas_filtrado[(~df_receita['Data'].isna()) & (~df_receita['Ano'].isna())]
df_alucar_receitas_filtrado = df_alucar_receitas_filtrado.rename(columns={'Nome (Alucar)': 'RECEITA', 'Data': 'DATA', 'Valor\nReceita': 'VALOR_TOTAL'})

df_alucar_receitas_filtrado['VALOR_TOTAL'] = (df_alucar_receitas_filtrado['VALOR_TOTAL']
      .str.replace('R\$', '', regex=True)      
      .str.replace('.', '', regex=False)       
      .str.replace(',', '.', regex=False)      
      .str.strip()                            
      .astype(float)                          
)

df_alucar_receitas_filtrado['DATA'] = pd.to_datetime(df_alucar_receitas_filtrado['DATA'], format='%d/%m/%Y')
df_alucar_receitas_filtrado['EMPRESA']='ALUCAR'
df_alucar_receitas_filtrado = df_alucar_receitas_filtrado[['DATA','RECEITA','VALOR_TOTAL','EMPRESA']]
df_consigcar_receitas = df_receita.loc[:, 'Mes':'Valor']
df_consigcar_receitas = df_consigcar_receitas[['Mes','Ano','Faturamento\nConsigCar','Valor']]
df_consigcar_receitas['Valor'] = (df_consigcar_receitas['Valor']
      .str.replace('R\$', '', regex=True)  
      .str.replace('.', '', regex=False)   
      .str.replace(',', '.', regex=False)  
      .str.strip()        
      .astype(float)      
)   

df_consigcar_receitas_filtrado = df_consigcar_receitas[~df_consigcar_receitas['Faturamento\nConsigCar'].isna()].copy()
df_consigcar_receitas_filtrado = df_consigcar_receitas_filtrado[df_consigcar_receitas_filtrado['Valor'].notna()].copy()
df_consigcar_receitas_filtrado['EMPRESA'] = 'CONSIGCAR'
df_consigcar_receitas_filtrado.rename(columns={'Mes':'MES','Ano':'ANO','Faturamento\nConsigCar': 'RECEITA', 'Valor': 'VALOR_TOTAL'}, inplace=True)
df_consigcar_receitas_filtrado.reset_index(drop=True, inplace=True)
for i in range(1, len(df_consigcar_receitas_filtrado)):
    if pd.isna(df_consigcar_receitas_filtrado.loc[i, 'MES']):
        df_consigcar_receitas_filtrado.loc[i, 'MES'] = df_consigcar_receitas_filtrado.loc[i-1, 'MES'] + 1 
        df_consigcar_receitas_filtrado.loc[i, 'ANO'] = df_consigcar_receitas_filtrado.loc[i-1, 'ANO']     
    if df_consigcar_receitas_filtrado.loc[i, 'MES'] > 12:
        df_consigcar_receitas_filtrado.loc[i, 'MES'] = 1 
        df_consigcar_receitas_filtrado.loc[i, 'ANO'] += 1

df_consigcar_receitas_filtrado['DATA'] = pd.to_datetime({'year': df_consigcar_receitas_filtrado['ANO'], 'month': df_consigcar_receitas_filtrado['MES'], 'day': 1}) + pd.offsets.MonthEnd(0)
df_consigcar_receitas_filtrado['DATA'] = pd.to_datetime(df_consigcar_receitas_filtrado['DATA'], format='%d/%m/%Y')
df_consigcar_receitas_filtrado = df_consigcar_receitas_filtrado[['DATA','RECEITA','VALOR_TOTAL','EMPRESA']]

df_receitas = pd.concat([df_consigcar_receitas_filtrado,df_alucar_receitas_filtrado],ignore_index=True)
df_receitas['STATUS'] = np.where(df_receitas['DATA'] <= dia_hoje,'Quitado','Estimado')
df_receitas_ordenadas = df_receitas.sort_values(by=['EMPRESA','DATA','RECEITA'],ascending=(True,True,True))

# Extract VENDAS CONSIGCAR

url = "https://empregadados-my.sharepoint.com/:x:/g/personal/bianca_empregadados_com_br/EZYutqfo5ldNhDw2lMYRxrIBnpPI6c7OTjBBS_F5yz860Q?e=6qfMJG&download=1"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
response.raise_for_status()  

with open('planilha_temp.xlsx', 'wb') as f:
    f.write(response.content)

df_consigcar_vendas = pd.read_excel('planilha_temp.xlsx')
df_consigcar_vendas = df_consigcar_vendas[df_consigcar_vendas['Valor parcela'].notna()].copy()
df_consigcar_vendas.columns = df_consigcar_vendas.columns.str.strip()
df_consigcar_vendas['ValorParcela'] = (df_consigcar_vendas['Valor parcela']
      .str.replace('R\$', '', regex=True) 
      .str.replace('.', '', regex=False)  
      .str.replace(',', '.', regex=False) 
      .str.strip()                        
      .astype(float)                      
)
df_consigcar_vendas['ValorTotal']=df_consigcar_vendas['ValorParcela']*df_consigcar_vendas['Quantidade de vezes']
df_consigcar_vendas['DATA'] = pd.to_datetime(df_consigcar_vendas['Data do Pagamento'], dayfirst=True, errors='coerce')
df_consigcar_vendas.rename(columns={'Tipo Produto': 'DESCRICAO','Nome':'CLIENTE', 'Quantidade de vezes': 'PARCELAS','ValorParcela':'VALOR_PARCELA','ValorTotal':'VALOR_TOTAL','Vendedor':'VENDEDOR'}, inplace=True)
df_consigcar_vendas = df_consigcar_vendas[['DATA','DESCRICAO','CLIENTE','VALOR_PARCELA','PARCELAS','VALOR_TOTAL','VENDEDOR']].sort_values(['DATA','CLIENTE'], ascending=[True, True])
df_consigcar_vendas['EMPRESA'] = 'CONSIGCAR'
df_alucar_vendas = df_alucar_receitas_filtrado[(df_alucar_receitas_filtrado['EMPRESA']=='ALUCAR') & (df_alucar_receitas_filtrado['DATA']<=dia_hoje)]
df_alucar_vendas = df_alucar_vendas.rename(columns={'RECEITA': 'CLIENTE'})
df_alucar_vendas['DESCRICAO'] = 'ALUGUEL'
df_alucar_vendas['VALOR_PARCELA'] = df_alucar_vendas['VALOR_TOTAL']
df_alucar_vendas['PARCELAS'] = 1
df_alucar_vendas['VENDEDOR'] = 'Balcão'
df_vendas = pd.concat([df_alucar_vendas,df_consigcar_vendas],ignore_index=True)


def gerar_parcelas(row):
    vencimentos = [row['DATA'] + pd.Timedelta(days=30 * i) for i in range(row['PARCELAS'])]
    pago = [v.date() <= date.today() for v in vencimentos]
    
    return pd.DataFrame({
        'EMPRESA': [row['EMPRESA']] * row['PARCELAS'],
        'DATA_VENDA': [row['DATA']] * row['PARCELAS'],
        'PARCELA': list(range(1, row['PARCELAS'] + 1)),
        'TOTAL_PARCELAS': [row['PARCELAS']] * row['PARCELAS'],
        'DESCRICAO': [row['DESCRICAO']] * row['PARCELAS'],
        'CLIENTE': [row['CLIENTE']] * row['PARCELAS'],
        'VALOR': [row['VALOR_PARCELA']] * row['PARCELAS'],
        'VENCIMENTO': vencimentos,
        'PAGO': pago
    })

df_contas_a_receber = pd.concat([gerar_parcelas(row) for _, row in df_consigcar_vendas.iterrows()], ignore_index=True)
df_contas_a_receber_ordenado = df_contas_a_receber.sort_values(by=['EMPRESA','VENCIMENTO'],ascending=[True,True])

# Extract METAS

sheet_id = "1cucnW4yVosO5n5BFgwXYv6rVy8yj6NTasM83RTCMOug"
metas = "835809915"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={metas}"
df_metas = pd.read_csv(url,header=2)

df_alucar_metas = df_metas.iloc[0:12, 1:5]
df_alucar_metas['Ano'] = df_alucar_metas['Ano'].ffill()
df_alucar_metas['Mês'] = df_alucar_metas['Mês'].astype(int)
df_alucar_metas['EMPRESA'] = 'ALUCAR'
df_alucar_metas = df_alucar_metas.rename(columns={'Ano': 'ANO', 'Mês':'MES', 'Meta\n1':'META_1', 'Meta\n2':'META_2'})

df_consigcar_metas = df_metas.iloc[0:12, 7:11]
df_consigcar_metas['Ano.1'] = df_consigcar_metas['Ano.1'].ffill()
df_consigcar_metas['Mês.1'] = df_consigcar_metas['Mês.1'].astype(int)
df_consigcar_metas['EMPRESA'] = 'CONSIGCAR'
df_consigcar_metas = df_consigcar_metas.rename(columns={'Ano.1': 'ANO', 'Mês.1':'MES', 'Meta\n1.1':'META_1', 'Meta\n2.1':'META_2'})
df_metas_totais = pd.concat([df_alucar_metas,df_consigcar_metas])
df_metas_totais['ANO'] = df_metas_totais['ANO'].astype(int)
df_metas_totais['META_1'] = df_metas_totais['META_1'].astype(int)
df_metas_totais['META_2'] = df_metas_totais['META_2'].astype(int)
df_metas_totais['DATA'] = pd.to_datetime({'year': df_metas_totais['ANO'], 'month': df_metas_totais['MES'], 'day': 1}) + pd.offsets.MonthEnd(0)

df_receitas_ordenadas['DATA'] = pd.to_datetime(df_receitas_ordenadas['DATA'])
df_analitico_receita_mensal = df_receitas_ordenadas.groupby([df_receitas_ordenadas['DATA'] + pd.offsets.MonthEnd(0), 'EMPRESA','STATUS']).agg(
    Valor_Total=('VALOR_TOTAL', 'sum'),
).reset_index()
df_analitico_receita_mensal_ordenado = df_analitico_receita_mensal.sort_values(by=['EMPRESA','DATA','STATUS'])

df_vendas['DATA'] = pd.to_datetime(df_vendas['DATA'])
df_analitico_vendedor_mensal = df_vendas.groupby([df_vendas['DATA'] + pd.offsets.MonthEnd(0), 'EMPRESA','VENDEDOR']).agg(
    Total_Vendas=('CLIENTE', 'count'), 
    Valor_Total=('VALOR_TOTAL', 'sum'),
    Valor_Primeira_Parcela=('VALOR_PARCELA', 'sum')
).reset_index()

df_analitico_vendedor_mensal['Vendas_Acumuladas'] = (
    df_analitico_vendedor_mensal.groupby(df_analitico_vendedor_mensal['DATA'].dt.year)['Total_Vendas'].cumsum()
)
df_analitico_vendedor_mensal['Ano'] = df_analitico_vendedor_mensal['DATA'].dt.year
df_analitico_vendedor_mensal['Mes'] = df_analitico_vendedor_mensal['DATA'].dt.month
df_analitico_vendedor_mensal_ordenado=df_analitico_vendedor_mensal.sort_values(by=['EMPRESA','DATA','Total_Vendas'],ascending=[True,True,False])

df_vendas_mensal = df_analitico_vendedor_mensal.groupby(['DATA','Ano','Mes', 'EMPRESA']).agg(
    Total_Vendas=('Total_Vendas', 'sum'), 
    Valor_Total=('Valor_Total', 'sum'),
).reset_index()
df_vendas_mensal['Vendas_Acumuladas'] = (
    df_vendas_mensal.groupby(['Ano','EMPRESA'])['Total_Vendas'].cumsum()
)
df_vendas_mensal = df_vendas_mensal.sort_values(by=['EMPRESA','DATA','Total_Vendas'])

df_vendas['MES_FIM'] = df_vendas['DATA'] + pd.offsets.MonthEnd(0)

df_analitico_vendedor_mensal_agg = df_vendas.groupby(['MES_FIM', 'EMPRESA', 'VENDEDOR']).agg(
    Total_Vendas=('CLIENTE', 'count'),  # Contagem de vendas
    Valor_Total=('VALOR_TOTAL', 'sum'),
    Valor_Primeira_Parcela=('VALOR_PARCELA', 'sum')
).reset_index()

all_meses = df_vendas['MES_FIM'].unique()

all_empresas_vendedores = df_vendas[['EMPRESA', 'VENDEDOR']].drop_duplicates()

df_meses = pd.DataFrame({'MES_FIM': all_meses})
df_completo_combinacoes = pd.merge(
    df_meses.assign(key=1),
    all_empresas_vendedores.assign(key=1),
    on='key'
).drop('key', axis=1)

df_analitico_vendedor_mensal_completo = pd.merge(
    df_completo_combinacoes,
    df_analitico_vendedor_mensal_agg,
    on=['MES_FIM', 'EMPRESA', 'VENDEDOR'],
    how='left'
).fillna({
    'Total_Vendas': 0,
    'Valor_Total': 0,
    'Valor_Primeira_Parcela': 0
})
df_analitico_vendedor_mensal_completo['Vendas_Acumuladas'] = (
    df_analitico_vendedor_mensal_completo.groupby(df_analitico_vendedor_mensal_completo['MES_FIM'].dt.year)['Total_Vendas'].cumsum()
)

df_despesas_final_ordenadas['DATA'] = pd.to_datetime(df_despesas_final_ordenadas['DATA'])

df_analitico_despesa_mensal = df_despesas_final_ordenadas.groupby([df_despesas_final_ordenadas['DATA'] + pd.offsets.MonthEnd(0), 'EMPRESA','CONTA','STATUS']).agg(
    Valor_Total=('VALOR', 'sum')
).reset_index()

df_analitico_despesa_mensal_ordenado = df_analitico_despesa_mensal[['DATA','EMPRESA','CONTA','Valor_Total','STATUS']].sort_values(['EMPRESA','DATA','CONTA'], ascending=[True,True,True])

df_dre_despesas = df_analitico_despesa_mensal_ordenado[df_analitico_despesa_mensal_ordenado['STATUS'] == 'Quitado']
df_dre_despesas['MES_ANO'] = df_dre_despesas['DATA'].dt.strftime('%b/%y').str.lower()
df_dre_despesas['TIPO'] = 'Despesa'

df_dre_receitas = df_analitico_receita_mensal_ordenado[df_analitico_receita_mensal_ordenado['STATUS'] == 'Quitado']
df_dre_receitas['MES_ANO'] = df_dre_receitas['DATA'].dt.strftime('%b/%y').str.lower()
df_dre_receitas['TIPO'] = 'Receita'
df_dre_receitas['CONTA'] = 'Receita Bruta'

df_dre = pd.concat([df_dre_despesas,df_dre_receitas],ignore_index=True)
df_dre = df_dre[['DATA','MES_ANO','EMPRESA','CONTA','Valor_Total','TIPO']].sort_values(['EMPRESA','DATA','TIPO','CONTA'], ascending=[True,True,False,True])

# Load BLOB AZURE

account_name = "projetosempregadados"
container = "hackathon01"
sas_token = "?sp=racwl&st=2025-05-10T12:08:43Z&se=2029-12-31T20:08:43Z&spr=https&sv=2024-11-04&sr=c&sig=5HrRO9hzqAjQ%2F%2FbEzydCGuOjATx8KTwzpLtnhdItqsM%3D"
fs = AzureBlobFileSystem(account_name=account_name, sas_token=sas_token)

def salvar_parquet_blob(df, nome_arquivo):
    caminho = f"{container}/{nome_arquivo}.parquet"
    with fs.open(caminho, 'wb') as f:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f)


salvar_parquet_blob(df_receitas_ordenadas.reset_index(drop=True), 'receitas')
salvar_parquet_blob(df_despesas_final_ordenadas.reset_index(drop=True), 'despesas')
salvar_parquet_blob(df_vendas.reset_index(drop=True), 'vendas')
salvar_parquet_blob(df_contas_a_receber_ordenado.reset_index(drop=True), 'contas_a_receber')
salvar_parquet_blob(df_metas_totais.reset_index(drop=True),'metas')
salvar_parquet_blob(df_vendas_mensal.reset_index(drop=True),'receita_mensal')
salvar_parquet_blob(df_analitico_vendedor_mensal_completo.reset_index(drop=True),'vendendor_mensal')
salvar_parquet_blob(df_analitico_despesa_mensal_ordenado.reset_index(drop=True),'despesa_mensal')
salvar_parquet_blob(df_dre.reset_index(drop=True),'dre')
salvar_parquet_blob(df_vendas_mensal.reset_index(drop=True),'vendas_mensal_acumuladas')

print('Script concluído!')
