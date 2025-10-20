# -*- coding: utf-8 -*-
"""
Automatização de Simulações de FAIs no ATP pelo Python (versão OOP)
Autora: Gabriela Nunes  |  e-mail: nuneslopesgabriela@gmail.com
Data original: 12/12/2022
Update: 09/10/2025

Descrição:
    - Mantém 100% da lógica do script original, apenas organizado em POO.
    - loop_completo() executa todos os passos na mesma ordem.
    - Pode ser executado diretamente: python este_arquivo.py
"""

from __future__ import annotations

# Imports das funções originais ------------------------------
# Fallback para facilitar execução local sem mexer em sys.path:
try:
    from Functions.lib_automatiza34barras import (
        mudacarregamento,
        salvacartaonapasta,
        faino34barras_HIFreal,
        muda_potencia_GD,
    )
except ModuleNotFoundError:
    # Se o pacote "Functions" não estiver no PYTHONPATH, tenta importar o módulo local.
    from Functions.lib_automatiza34barras import (
        mudacarregamento,
        salvacartaonapasta,
        faino34barras_HIFreal,
        muda_potencia_GD,
    )

# Bibliotecas padrão ----------------------------------------
import time
from typing import List
import numpy as np
import pandas as pd
from os import listdir  # (mantido pois está no original, mesmo não usado no trecho)


class Automatiza34Barras:
    """
    Classe que encapsula a automação, mantendo a lógica original.
    """

    def __init__(self) -> None:
        # ---------------------------
        # Configurações iniciais
        # ---------------------------
        # (mantidas as mesmas listas e valores do código fornecido)
        self.Fases: List[str] = ['FaseA/', 'FaseB/', 'FaseC/']

        self.barrasfasea: List[int] = [
            2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 14, 15, 16, 17,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 33, 34
        ]
        self.barrasfaseb: List[int] = [
            2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 17, 18,
            19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
        ]
        self.barrasfasec: List[int] = [
            2, 3, 4, 5, 6, 7, 8, 11, 14, 16, 17, 19,
            20, 21, 22, 24, 25, 26, 27, 28, 29, 31, 32, 33, 34
        ]

        # Carregamento e GD
        self.carregamento: List[float] = [1, 0.3]  # definir os carregamentos simulados
        self.Pgd: List[float] = [0.995, 0.665, 0.335]  # níveis de penetração da GD (ativa)
        self.Qgd: List[float] = [0.0995, 0.0665, 0.0335]  # 10% da ativa

        # Caminhos/linhas (preenchidos no código, como pedido)
        self.path: str = (
            r'C:\\Users\\nunes\\OneDrive\\Documentos\\trabalho\\prof ufmg\\Pesquisa\\'
            r'SimulaATPcomPython\\Teste_2\\SystemLines\\'
        )
        self.linhatsimu: int = 4          # linha onde se modifica o tempo de simulação
        self.linhamudasolo: int = 308     # linha do sinal que entra no modelo ATP
        self.linhaschgd: int = 1192       # primeira linha (3 fases) da chave que desconecta a GD
        self.linhascap2: int = 1069       # primeira linha das chaves do capacitor 2
        self.linhaPGD: int = 12           # linha onde insere P (ativa) da GD
        self.noI: int = 1195              # início do modelo FAI na barra 802
        self.noF: int = 944               # fim do modelo FAI
        self.linhach: int = 1196          # linha entre o modelo e a chave do modelo da FAI

        # Cartão original (xlsx) usado como base
        self.cartao_xlsx: str = 'TestSystem\\34barrasGS848_FAIreal_ComMedidores.xlsx'

        # Dataframes de linhas a modificar (carregados no setup)
        self.Linhascd: pd.DataFrame | None = None
        self.Linhascp: pd.DataFrame | None = None
        self.LinhasBarras: pd.DataFrame | None = None

        # Cartões de trabalho
        self.CartaoA: List[List[str]] | None = None  # cópia do cartão original
        # Obs.: CartaoB/C/D/E são variáveis locais durante o loop, como no script

    # ======================================================================
    # Métodos auxiliares (apenas “organizam” o fluxo; não alteram a lógica)
    # ======================================================================

    def carregar_sistemas(self) -> None:
        """Carrega as planilhas de linhas do cartão do ATP que serão modificadas."""
        self.Linhascd = pd.read_excel(self.path + 'linhascargasdistribuidas.xlsx')
        self.Linhascp = pd.read_excel(self.path + 'linhascargaspontuais.xlsx')
        self.LinhasBarras = pd.read_excel(self.path + 'LinhasHIFReal.xlsx')

    def carregar_cartao_original(self) -> None:
        """Carrega o cartão ATP base (xlsx) e converte para lista de listas."""
        CartaoATPOriginal = pd.read_excel(self.cartao_xlsx)
        self.CartaoA = CartaoATPOriginal.values.tolist()

    @staticmethod
    def selecionar_barras_por_fase(f: int,
                                   barrasfasea: List[int],
                                   barrasfaseb: List[int],
                                   barrasfasec: List[int]) -> List[int]:
        """Retorna a lista de barras conforme a fase f (1,2,3)."""
        if f == 1:
            return barrasfasea
        elif f == 2:
            return barrasfaseb
        elif f == 3:
            return barrasfasec
        # Mantém comportamento implícito (não ocorre no fluxo atual)
        return []

    @staticmethod
    def desconectar_gd_conectar_cap2(CartaoB: List[List[str]],
                                     linhaschgd: int,
                                     linhascap2: int) -> List[List[str]]:
        """
        Mantém a mesma sequência de substituições do código original para
        simular o sistema sem GD: ajusta 3 linhas consecutivas em chaves e capacitor.
        """
        for i in [0, 1, 2]:
            # Desconectando a GD
            linhamudar = CartaoB[linhaschgd + i]
            novalinha = [w.replace('-1.', '10.') for w in linhamudar]
            CartaoB[linhaschgd + i] = novalinha

            # Conectando o capacitor 2
            linhamudar = CartaoB[linhascap2 + i]
            novalinha = [w.replace('1.E3', ' -1.') for w in linhamudar]
            CartaoB[linhascap2 + i] = novalinha
        return CartaoB

    # =========================
    # Pipeline 
    # =========================
    def loop_completo(self) -> None:

        assert self.Linhascd is not None and self.Linhascp is not None and self.LinhasBarras is not None, \
            "Linhascd/Linhascp/LinhasBarras não carregadas. Chame carregar_sistemas()."
        assert self.CartaoA is not None, "Cartão base não carregado. Chame carregar_cartao_original()."

        CartaoA = self.CartaoA  # referência local

        # lista de fases e suas barras correspondentes (mantém exatamente o mesmo range)
        for f in [1, 2]:  # range(1, len(Fases))  --> aqui só 1 e 2, como no código
            fase = self.Fases[f - 1]

            barrassimu = self.selecionar_barras_por_fase(
                f, self.barrasfasea, self.barrasfaseb, self.barrasfasec
            )

            for cgd in [1]:  # range(1,2) -> apenas com GD (1)
                CartaoB = CartaoA

                # Se quiser simular sem GD (cgd == 2) — lógica preservada
                if cgd == 2:
                    CartaoB = self.desconectar_gd_conectar_cap2(
                        CartaoB, self.linhaschgd, self.linhascap2
                    )

                # Nível de penetração da GD (usa só o primeiro: Pgd[:1])
                for pn, valor in enumerate(self.Pgd[:1], start=1):
                    CartaoC = CartaoB

                    # Se pn > 1, muda P/Q da GD (no fluxo atual, não entra)
                    if pn > 1:
                        CartaoC = muda_potencia_GD(
                            CartaoC,
                            self.linhaPGD,
                            self.Pgd,
                            self.Qgd,
                            pn,
                            '0.995',
                            '0.0995'
                        )

                    # Carregamento 
                    for car in [1, 2]:
                        CartaoD = CartaoC  # cópia lógica

                        if car > 1:
                            CartaoD = mudacarregamento(
                                CartaoC,
                                self.carregamento[car - 1],
                                self.Linhascd,
                                self.Linhascp,
                                self.path
                            )

                        # Próximas barras 
                        for pb in [2]:
                            CartaoE = CartaoD
                            str_proxbarra = str(barrassimu[pb - 1])
                            proxbarra = barrassimu[pb - 1]
                            barra = 2  # a barra em que já está é a 2

                            # Para cada solo de FAI (usa somente 2: [2])
                            # Para FAI com modelo, vai modificar aqui a substituição apenas do solo, pela configuração do parametrizamodelo
                            for solos in [2]:
                                CartaoE = faino34barras_HIFreal(
                                    CartaoD, CartaoE, barra, proxbarra,
                                    self.LinhasBarras, f,
                                    self.noI, self.noF,
                                    solos,
                                    self.linhatsimu, self.linhamudasolo, self.linhach
                                )

                                salvacartaonapasta(
                                    CartaoE,
                                    'atpalterado.atp',
                                    str_proxbarra,
                                    fase,
                                    cgd,
                                    pn,
                                    car,
                                    solos
                                )

    # ----------------------------
    # Conveniência: executar tudo
    # ----------------------------
    def run(self) -> None:
        """Executa o setup e o loop completo."""
        t0 = time.time()
        self.carregar_sistemas()
        self.carregar_cartao_original()
        self.loop_completo()
        print(f"Concluído em {time.time() - t0:.2f}s.")


# ===========================================================
# Main (permite rodar direto: python este_arquivo.py)
# ===========================================================
if __name__ == "__main__":
    app = Automatiza34Barras()
    app.run()
