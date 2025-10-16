# -*- coding: utf-8 -*-
"""
Automatização de Simulações de FAIs no ATP pelo Python (versão OOP)
Autora: Gabriela Nunes  |  e-mail: nuneslopesgabriela@gmail.com
15/10/2025

Descrição:
    - Atualiza os valores de resistencia inicial, final e de tau e salva o novo cartao com as alterações.
"""


from pathlib import Path
import itertools
import re

class CriaCartaoATP:
    def __init__(self) -> None:
        # Caminho do cartão-base .atp (ajuste se necessário)
        self.cartao_base_path = Path(__file__).resolve().parent / "SistemaTeste_FMG_FAIcasoVarTAU.atp"

        # Pasta de saída
        self.pasta_saida = Path("cartao")
        self.pasta_saida.mkdir(parents=True, exist_ok=True)

        # Linhas alvo (1-based) informadas: 66, 67, 68
        # Convertemos para 0-based ao usar em listas
        self.idx_tau_0  = 66 - 1
        self.idx_rini_0 = 67 - 1
        self.idx_rfim_0 = 68 - 1

        # Faixas de valores
        self.Rini_values = list(range(200, 801, 50))  # 200..800
        self.Rfim_values = list(range(50, 101, 10))   # 50..100
        self.tau_values  = list(range(50, 251, 25))   # 50..250

        # Carrega o arquivo base em modo binário para preservar EOL por linha
        self.encoding = "utf-8"
        self.linhas_base_bytes = self._ler_arquivo_bytes(self.cartao_base_path)

    # ---------- utilitários de I/O ----------

    def _ler_arquivo_bytes(self, caminho: Path) -> list[bytes]:
        """Lê o arquivo inteiro como bytes e devolve uma lista de linhas (bytes) mantendo o EOL de cada linha."""
        data = caminho.read_bytes()
        # splitlines(keepends=True) em bytes preserva o EOL exato de cada linha
        return data.splitlines(keepends=True)

    def _salvar_cartao_bytes(self, linhas_bytes: list[bytes], nome_base: str) -> None:
        caminho = (self.pasta_saida / nome_base).with_suffix(".atp")
        with open(caminho, "wb") as f:
            for b in linhas_bytes:
                f.write(b)

    # ---------- substituição robusta ----------

    @staticmethod
    def _separa_eol(line_b: bytes) -> tuple[bytes, bytes]:
        """Separa conteúdo e EOL da linha em bytes. Retorna (conteudo_sem_eol, eol)."""
        # Possíveis finais: \r\n, \n, \r, ou nenhum (última linha pode não ter EOL)
        if line_b.endswith(b"\r\n"):
            return line_b[:-2], b"\r\n"
        elif line_b.endswith(b"\n"):
            return line_b[:-1], b"\n"
        elif line_b.endswith(b"\r"):
            return line_b[:-1], b"\r"
        else:
            return line_b, b""

    @staticmethod
    def _formata_novo_valor(novo_valor, num_original_str: str) -> str:
        """
        Formata novo_valor usando:
        - mesmo separador decimal (vírgula ou ponto) do original;
        - mesma quantidade de casas decimais do original;
        - se o original era inteiro (sem decimais), grava inteiro.
        """
        usa_virgula = "," in num_original_str
        if usa_virgula:
            parte_dec = num_original_str.split(",")[1] if "," in num_original_str else ""
        else:
            parte_dec = num_original_str.split(".")[1] if "." in num_original_str else ""

        casas = len(parte_dec)

        # garante float numérico mesmo se vier string com vírgula
        novo_float = float(str(novo_valor).replace(",", "."))

        if casas > 0:
            novo_str = f"{novo_float:.{casas}f}"
        else:
            novo_str = str(int(round(novo_float)))

        if usa_virgula:
            novo_str = novo_str.replace(".", ",")
        return novo_str

    @staticmethod
    def _troca_numero_apos_igual_em_str(linha_str: str, novo_valor) -> str:
        """
        Em uma string de linha, substitui SOMENTE o primeiro número após '='.
        Preserva espaços entre '=' e o número e todo o restante do texto.
        """
        m = re.search(r"=(\s*)(-?\d+(?:[.,]\d+)?)", linha_str)
        if not m:
            return linha_str  # sem mudança
        espacos = m.group(1)
        num_original = m.group(2)

        novo_num = CriaCartaoATP._formata_novo_valor(novo_valor, num_original)

        # Reconstrói: tudo antes do número + novo número + tudo depois
        inicio = linha_str[:m.start(2)]
        fim = linha_str[m.end(2):]
        return inicio + novo_num + fim

    def _troca_na_linha_idx(self, linhas_bytes: list[bytes], idx0: int, novo_valor) -> None:
        """
        Troca o número após '=' apenas na linha de índice idx0 (0-based),
        preservando o EOL daquela linha e sem mexer nas demais.
        """
        if not (0 <= idx0 < len(linhas_bytes)):
            return  # índice fora do range: não faz nada

        conteudo, eol = self._separa_eol(linhas_bytes[idx0])

        # decodifica conteúdo (sem EOL) -> str
        try:
            linha_str = conteudo.decode(self.encoding)
        except UnicodeDecodeError:
            # fallback mínimo para latin-1 se necessário
            linha_str = conteudo.decode("latin-1")

        # aplica substituição apenas no primeiro número após '='
        nova_str = self._troca_numero_apos_igual_em_str(linha_str, novo_valor)

        # re-encode com o mesmo encoding
        try:
            nova_bytes = nova_str.encode(self.encoding)
        except UnicodeEncodeError:
            nova_bytes = nova_str.encode("latin-1")

        # remonta com o EOL original intacto
        linhas_bytes[idx0] = nova_bytes + eol

    # ---------- pipeline principal ----------

    def gerar(self):
        for Rini, Rfim, tau in itertools.product(self.Rini_values, self.Rfim_values, self.tau_values):
            # cópia byte-a-byte do cartão base (preserva tudo)
            linhas_mod = list(self.linhas_base_bytes)

            # troca somente nas linhas alvo (0-based)
            self._troca_na_linha_idx(linhas_mod, self.idx_tau_0,  tau)
            self._troca_na_linha_idx(linhas_mod, self.idx_rini_0, Rini)
            self._troca_na_linha_idx(linhas_mod, self.idx_rfim_0, Rfim)

            # salva
            nome = f"FAI_TAU{tau}_Rini{Rini}_Rfim{Rfim}"
            self._salvar_cartao_bytes(linhas_mod, nome)


if __name__ == "__main__":
    CriaCartaoATP().gerar()
