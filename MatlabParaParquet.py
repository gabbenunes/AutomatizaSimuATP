# -*- coding: utf-8 -*-
"""
Converte todos os arquivos .mat da pasta 'ArquivosMatlab'
para arquivos .parquet salvos em 'ArquivosParquet',
mantendo o mesmo nome base.
"""

from pathlib import Path
from scipy.io import loadmat
import pandas as pd
import numpy as np

# Caminhos das pastas
BASE = Path(__file__).resolve().parent
PASTA_MAT = BASE / "ArquivosMAT"
PASTA_PARQ = BASE / "ArquivosParquet"

# Garante que a pasta de saída exista
PASTA_PARQ.mkdir(parents=True, exist_ok=True)

# Loop pelos arquivos .mat
for arquivo_mat in PASTA_MAT.glob("*.mat"):
    try:
        print(f"Lendo {arquivo_mat.name}...")
        dados = loadmat(arquivo_mat)

        # Remove chaves desnecessárias do MATLAB
        dados = {k: v for k, v in dados.items() if not k.startswith("__")}

        # Converte para DataFrame
        # Se tiver apenas uma variável principal, usa ela diretamente
        if len(dados) == 1:
            nome_var, conteudo = next(iter(dados.items()))
            if isinstance(conteudo, np.ndarray):
                df = pd.DataFrame(conteudo)
            else:
                df = pd.DataFrame([conteudo])
        else:
            # Junta todas as variáveis em um DataFrame
            df = pd.DataFrame({k: v.flatten() if isinstance(v, np.ndarray) else [v] for k, v in dados.items()})

        # Nome do arquivo de saída
        arquivo_parquet = PASTA_PARQ / (arquivo_mat.stem + ".parquet")

        # Salva em formato parquet
        df.to_parquet(arquivo_parquet, index=False)
        print(f"✅ Convertido: {arquivo_mat.name} → {arquivo_parquet.name}")

    except Exception as e:
        print(f"❌ Erro ao converter {arquivo_mat.name}: {e}")
