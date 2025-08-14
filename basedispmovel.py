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
    print(f"\n🚀 Executando script em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        with open('sites_municipios.json', 'r', encoding='utf-8') as f:
            lista_sites_municipios = json.load(f)

        df_ddd = pd.DataFrame(lista_sites_municipios)
        df_ddd.rename(columns={"MUNICÍPIO": "MUNICIPIO", "CN": "DDD"}, inplace=True)

        # Mensal
        url_mensal = "https://maestro.vivo.com.br/movel/downloads/cdt_sites_mensal_hmm.xlsx"
        res_m = requests.get(url_mensal, timeout=100, verify=False)
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
        res_d = requests.get(url_diario, timeout=100, verify=False)
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

        # Adiciona coluna de projeção média do arquivo Excel
        try:
            path_dashboard = 'C:/Users/0102062092/Downloads/Carimbos_VSCode/Dispmóvel/Dashboard Executivo Celldowntime.xlsx'
            df_dashboard = pd.read_excel(path_dashboard, engine='openpyxl')
            # Renomeia para facilitar o merge
            df_dashboard = df_dashboard.rename(columns={'DISP PROJETADA': 'PROJECAO_MEDIA'})
            # Mantém apenas as colunas necessárias
            df_dashboard = df_dashboard[['SITE', 'PROJECAO_MEDIA']]
            # Remove duplicatas se houver
            df_dashboard = df_dashboard.drop_duplicates(subset=['SITE'])
            # Faz o merge
            df_final = df_final.merge(df_dashboard, on='SITE', how='left')
            df_final['PROJECAO_MEDIA'] = df_final['PROJECAO_MEDIA'].fillna("0")
        except Exception as e:
            print(f"⚠️ Erro ao ler/mesclar Dashboard Executivo Celldowntime.xlsx: {e}")
            df_final['PROJECAO_MEDIA'] = "0"

        # Remove o site "SRJ"
        df_final = df_final[df_final['SITE'] != 'SRJ']

        meses_ordenados = ordem_meses
        dias_ordenados = sorted([col for col in pivot_diario.columns], key=lambda d: datetime.strptime(d, "%d/%m/%Y"))
        # Adiciona PROJECAO_MEDIA ao final das colunas
        df_final = df_final[['DDD', 'MUNICIPIO', 'SITE'] + meses_ordenados + dias_ordenados + ['PROJECAO_MEDIA']]
        dados_json = df_final.to_dict(orient="records")

        # GitHub
        repositorio = 'GrupoTel-web/RElTUExPVkVMA'
        caminho_arquivo = 'dados.json'
        url = f'https://api.github.com/repos/{repositorio}/contents/{caminho_arquivo}'
        headers = {'Authorization': f'token {token}'}

        novo_conteudo_json = json.dumps(dados_json, ensure_ascii=False, separators=(',', ':'))

        sucesso = False
        while not sucesso:
            try:
                res_get = requests.get(url, headers=headers, timeout=100)
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
                    timeout=100
                )

                if res_put.status_code in (200, 201):
                    print(f"\n✅ Arquivo atualizado com sucesso no GitHub em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    sucesso = True
                else:
                    print("❌ Erro ao atualizar:", res_put.text)
                    print("🔄 Tentando novamente em 5 segundos...")
                    time.sleep(5)
            except Exception as e:
                print("💥 Erro durante a tentativa de atualização:", str(e))
                print("🔄 Tentando novamente em 5 segundos...")
                time.sleep(5)

    except Exception as e:
        print("💥 Erro durante a execução do script:", str(e))

if __name__ == "__main__":
    executar_script()  