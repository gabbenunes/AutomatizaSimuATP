# -*- coding: utf-8 -*-
"""
Classe para leitura e plotagem de variáveis a partir de arquivos .parquet.
Autor: Gabriela Nunes Lopes
"""

import os
import pandas as pd
import matplotlib.pyplot as plt


class ParquetPlotter:
    """Classe responsável por ler um arquivo Parquet e plotar variáveis de interesse."""

    def __init__(self, caminho_arquivo: str):
        if not os.path.isfile(caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
        self.caminho_arquivo = caminho_arquivo
        self.df = None
        print(f"📂 Arquivo definido: {os.path.basename(caminho_arquivo)}")

    def carregar(self):
        """Carrega o arquivo Parquet em um DataFrame Pandas."""
        print(f"🔍 Lendo arquivo {self.caminho_arquivo} ...")
        self.df = pd.read_parquet(self.caminho_arquivo)
        print(f"✅ Arquivo carregado com sucesso ({len(self.df)} linhas, {len(self.df.columns)} colunas).")

    def listar_variaveis(self):
        """Lista as variáveis disponíveis no arquivo."""
        if self.df is None:
            raise RuntimeError("O arquivo ainda não foi carregado. Use .carregar() antes.")
        colunas = list(self.df.columns)
        print("\n📊 Variáveis disponíveis:")
        for i, col in enumerate(colunas):
            print(f"  {i:02d}: {col}")
        return colunas

    def plotar(self, nome_variavel: str):
        """Plota a variável selecionada versus o tempo."""
        if self.df is None:
            raise RuntimeError("O arquivo ainda não foi carregado. Use .carregar() antes.")

        # Identifica a coluna de tempo
        if "time" in self.df.columns:
            eixo_tempo = "time"
        elif "tempo" in self.df.columns:
            eixo_tempo = "tempo"
        else:
            raise KeyError("Nenhuma coluna 'time' ou 'tempo' encontrada no arquivo.")

        if nome_variavel not in self.df.columns:
            raise KeyError(f"A variável '{nome_variavel}' não existe neste arquivo.")

        tempo = self.df[eixo_tempo]
        valores = self.df[nome_variavel]

        print(f"📈 Plotando variável '{nome_variavel}' ...")
        plt.figure(figsize=(10, 5))
        plt.plot(tempo, valores, label=nome_variavel, color='tab:blue', linewidth=1.2)
        plt.xlabel("Tempo (s)")
        plt.ylabel(nome_variavel)
        plt.title(f"Forma de onda - {nome_variavel}")
        plt.grid(True)
        plt.legend(loc="best", fontsize="small")
        plt.tight_layout()
        plt.show()


# ===========================================================
# MAIN - Escolha do arquivo e variável
# ===========================================================
if __name__ == "__main__":
    # Caminho do arquivo Parquet
    arquivo_parquet = r"step3_ParquetOutput\original.parquet"

    # Variável de interesse a ser plotada (exemplo)
    variavel_escolhida = "GERA-SUBA (I-bran)"   # altere para qualquer outra (ex: 'V1a', 'BUZ1A', etc.)

    # Execução
    plotter = ParquetPlotter(arquivo_parquet)
    plotter.carregar()
    plotter.listar_variaveis()
    plotter.plotar(variavel_escolhida)
