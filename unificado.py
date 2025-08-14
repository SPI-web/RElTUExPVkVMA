import pandas as pd
from datetime import datetime, timedelta
import json
import requests
import base64
from io import BytesIO
import schedule
import time
import os
from dotenv import load_dotenv
import urllib3
import warnings

# Silenciar warnings desnecessários
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Carrega variáveis de ambiente
load_dotenv()
token = os.getenv('GITHUB_TOKEN')

def executar_script():
    print(f"\n\uD83D\uDE80 Executando script em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        with open('sites_municipios.json', 'r', encoding='utf-8') as f:
            lista_sites_municipios = json.load(f)

        df_ddd = pd.DataFrame(lista_sites_municipios)
        df_ddd.rename(columns={"MUNICÍPIO": "MUNICIPIO", "CN": "DDD"}, inplace=True)

        # Mensal
        url_mensal = "https://maestro.vivo.com.br/movel/downloads/cdt_sites_mensal_hmm.xlsx"
        res_m = requests.get(url_mensal, timeout=60, verify=False)
        res_m.raise_for_status()
        df_mensal = pd.read_excel(BytesIO(res_m.content), sheet_name="VIVO")
        df_mensal = df_mensal[df_mensal['REGIONAL'] == 'SP']
        df_mensal['DATA'] = pd.to_datetime(df_mensal['ANO'].astype(str) + '-' + df_mensal['MES'].astype(str) + '-01')
        data_corte = datetime.today().replace(day=1) - pd.DateOffset(months=4)
        df_mensal = df_mensal[df_mensal['DATA'] >= data_corte]
        df_mensal['MÊS'] = df_mensal['DATA'].dt.strftime('%b')
        ordem_meses = df_mensal['DATA'].dt.strftime('%b').sort_values().unique().tolist()
        df_mensal['MÊS'] = pd.Categorical(df_mensal['MÊS'], categories=ordem_meses, ordered=True)
        pivot_mensal = df_mensal.pivot_table(index='SITE', columns='MÊS', values='DISPONIBILIDADE_GERAL', observed=False)
        pivot_mensal = pivot_mensal.fillna(0).round(2).astype(str)

        # Diário
        url_diario = "https://maestro.vivo.com.br/movel/downloads/cdt_diario_site_hmm.xlsx"
        res_d = requests.get(url_diario, timeout=60, verify=False)
        res_d.raise_for_status()
        df_diario = pd.read_excel(BytesIO(res_d.content), sheet_name="DISPONIBILIDADE")
        df_diario = df_diario[df_diario['UF'] == 'SP']
        ontem = datetime.today() - timedelta(days=1)
        dias = [(ontem - timedelta(days=i)).date() for i in range(8)]
        df_diario = df_diario[df_diario['DATA_REFERENCIA'].dt.date.isin(dias)]
        df_diario['DIA'] = df_diario['DATA_REFERENCIA'].dt.strftime('%d/%m/%Y')
        pivot_diario = df_diario.pivot_table(index='SITE', columns='DIA', values='DISP_GERAL')
        pivot_diario = pivot_diario.fillna(0).round(2).astype(str)

        # Junta tudo
        df_final = pd.concat([pivot_mensal, pivot_diario], axis=1).reset_index()
        df_final = df_final.merge(df_mensal[['SITE', 'MUNICIPIO']], on='SITE', how='left')
        df_final = df_final.drop_duplicates(subset=['SITE', 'MUNICIPIO'])
        df_final = df_final[df_final.apply(lambda row: any(
            (row['SITE'] == entry['SITE'] and row['MUNICIPIO'] == entry['MUNICÍPIO'])
            for entry in lista_sites_municipios), axis=1)]
        df_final = df_final.merge(df_ddd[['SITE', 'MUNICIPIO', 'DDD']], on=['SITE', 'MUNICIPIO'], how='left')
        df_final = df_final.fillna("0")

        meses_ordenados = ordem_meses
        dias_ordenados = sorted([col for col in pivot_diario.columns], key=lambda d: datetime.strptime(d, "%d/%m/%Y"))
        df_final = df_final[['DDD', 'MUNICIPIO', 'SITE'] + meses_ordenados + dias_ordenados]
        dados_json = df_final.to_dict(orient="records")

        # GitHub
        repositorio = 'GrupoTel-web/RElTUExPVkVMA'
        caminho_arquivo = 'dados.json'
        url = f'https://api.github.com/repos/{repositorio}/contents/{caminho_arquivo}'
        headers = {'Authorization': f'token {token}'}

        novo_conteudo_json = json.dumps(dados_json, ensure_ascii=False, separators=(',', ':'))
        res_get = requests.get(url, headers=headers, timeout=60)
        sha_arquivo = res_get.json().get('sha', '')

        conteudo_base64 = base64.b64encode(novo_conteudo_json.encode('utf-8')).decode('utf-8')
        res_put = requests.put(
            url,
            headers=headers,
            json={
                'message': 'Atualização automática via script',
                'content': conteudo_base64,
                'sha': sha_arquivo
            },
            timeout=60
        )

        if res_put.status_code in (200, 201):
            print("✅ Arquivo atualizado com sucesso no GitHub!")
        else:
            print("❌ Erro ao atualizar:", res_put.text)

    except Exception as e:
        print("💥 Erro durante a execução do script:", str(e))


# Agendamentos
schedule.every().day.at("08:00").do(executar_script)
schedule.every().day.at("09:00").do(executar_script)

# Execução imediata na primeira vez
executar_script()

# Loop infinito
print("\n\uD83D\uDD52 Aguardando os horários agendados... (Ctrl+C para sair)")
while True:
    schedule.run_pending()
    time.sleep(1)
