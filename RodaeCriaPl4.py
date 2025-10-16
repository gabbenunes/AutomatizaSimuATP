# -*- coding: utf-8 -*-
"""
Executa todos os .atp da pasta 'cartoes' (ou 'cartao' / 'Cartoes')
usando o runATP.exe que está na mesma pasta deste script.

ATUALIZAÇÕES:
- Para cada .atp, cria uma subpasta única em TmpATP/, copia o .atp e roda lá dentro.
- Se gerar .pl4, move para ./ArquivoPL4.
- Se falhar, faz uma CÓPIA do .atp original em ./cartoesnaorodados.
- Apaga a subpasta temporária criada (por job) e, ao final, limpa todas as subpastas de TmpATP.

Requisitos: psutil, tqdm
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import shutil
import psutil
import time
import os
from tqdm import tqdm
import uuid

# Caminho absoluto do executável local
RUNATP_PATH = Path(__file__).resolve().parent / "runATP.exe"

NUMTHREADS = max(1, os.cpu_count() // 2)
TIMEOUT_S = None  # sem timeout explícito (mantido)

BASE_DIR = Path(__file__).resolve().parent
CANDIDATAS = [BASE_DIR / "cartoes", BASE_DIR / "cartao", BASE_DIR / "Cartoes"]
PASTA_CARTOES = next((p for p in CANDIDATAS if p.exists()), BASE_DIR / "cartoes")
PASTA_CARTOES.mkdir(parents=True, exist_ok=True)

PASTA_PL4 = BASE_DIR / "ArquivoPL4"
PASTA_PL4.mkdir(parents=True, exist_ok=True)

# Nova pasta temporária para execução isolada de cada cartão
PASTA_TMP_ATP = BASE_DIR / "TmpATP"
PASTA_TMP_ATP.mkdir(parents=True, exist_ok=True)

# Nova pasta para armazenar CÓPIAS dos cartões que não rodaram
PASTA_CARTOES_NAO_RODADOS = BASE_DIR / "cartoesnaorodados"
PASTA_CARTOES_NAO_RODADOS.mkdir(parents=True, exist_ok=True)


def conta_tpbig():
    c = 0
    for proc in psutil.process_iter(attrs=["name"]):
        try:
            if proc.info["name"] and proc.info["name"].lower() == "tpbig.exe":
                c += 1
        except psutil.Error:
            pass
    return c


def _cria_subpasta_temp(stem: str) -> Path:
    """
    Cria uma subpasta única dentro de TmpATP/ para rodar um cartão em isolamento.
    Usa UUID para evitar colisões em paralelo.
    """
    sub = PASTA_TMP_ATP / f"{stem}__{uuid.uuid4().hex[:8]}"
    sub.mkdir(parents=True, exist_ok=False)
    return sub


def _limpa_dir(p: Path):
    """Apaga diretório p de forma resiliente."""
    if p.exists():
        try:
            shutil.rmtree(p, ignore_errors=True)
        except Exception:
            # tenta de novo com passos
            for _ in range(3):
                try:
                    shutil.rmtree(p, ignore_errors=True)
                    break
                except Exception:
                    time.sleep(0.2)


def _copia_cartao_nao_rodado(atp_original: Path):
    """
    Faz uma CÓPIA do .atp original para a pasta cartoesnaorodados/.
    Se já existir um arquivo com o mesmo nome, sobrescreve.
    """
    destino = PASTA_CARTOES_NAO_RODADOS / atp_original.name
    try:
        if destino.exists():
            destino.unlink()
        shutil.copy2(str(atp_original), str(destino))
    except Exception as e:
        print(f"[AVISO] Falhou ao copiar cartão não rodado '{atp_original.name}' -> '{destino}': {e}")


def run_atp_arquivo(atp_path: Path):
    """
    Executa 'runATP.exe <arquivo.atp>' em UMA SUBPASTA TEMPORÁRIA EXCLUSIVA,
    move o .pl4 gerado para PASTA_PL4 e apaga a subpasta criada.

    Retorna o Path do .pl4 de destino se sucesso; caso contrário, None.
    """
    stem = atp_path.stem
    subtemp = _cria_subpasta_temp(stem)

    try:
        # Copia o .atp para a subpasta isolada
        atp_copia = subtemp / atp_path.name
        shutil.copy2(str(atp_path), str(atp_copia))

        # Executa o runATP dentro da subpasta
        cmd = [str(RUNATP_PATH), atp_copia.name]
        subprocess.run(cmd, cwd=str(subtemp), timeout=TIMEOUT_S, shell=False)

        # Verifica se gerou o .pl4 dentro da subpasta
        pl4_local = atp_copia.with_suffix(".pl4")
        if pl4_local.exists():
            # Move o .pl4 gerado para a pasta de saída
            destino = PASTA_PL4 / pl4_local.name
            try:
                if destino.exists():
                    destino.unlink()
                shutil.move(str(pl4_local), str(destino))
                return destino
            except Exception as e:
                raise RuntimeError(f"Falha ao mover {pl4_local} -> {destino}: {e}")
        else:
            # Não gerou .pl4 — registra o cartão na pasta de não rodados
            _copia_cartao_nao_rodado(atp_path)
            return None

    finally:
        # Apaga a subpasta criada para este job (limpeza por job)
        _limpa_dir(subtemp)


def mover_pl4(pl4_src: Path, destino_dir: Path):
    """
    (Mantida por compatibilidade com a sua lógica, mas não é mais usada
     pois já movemos o .pl4 dentro de run_atp_arquivo)
    """
    destino = destino_dir / pl4_src.name
    try:
        if destino.exists():
            destino.unlink()
        shutil.move(str(pl4_src), str(destino))
        return destino
    except Exception as e:
        raise RuntimeError(f"Falha ao mover {pl4_src} -> {destino}: {e}")


def main():
    if not RUNATP_PATH.exists():
        print(f"[ERRO] Não encontrei o executável: {RUNATP_PATH}")
        return

    print(f"Usando: {RUNATP_PATH}")

    atp_files = sorted(PASTA_CARTOES.glob("*.atp"))
    if not atp_files:
        print(f"Nenhum .atp encontrado em: {PASTA_CARTOES}")
        return

    print(f"Encontrados {len(atp_files)} arquivos .atp.")
    print(f"Executando com até {NUMTHREADS} threads...")

    results = []
    with ThreadPoolExecutor(max_workers=NUMTHREADS) as executor:
        futures = {}
        pbar = tqdm(total=len(atp_files), desc="Executando ATP", ncols=90)

        for atp_path in atp_files:
            # controla concorrência pelo número de tpbig.exe
            while conta_tpbig() >= NUMTHREADS:
                time.sleep(0.5)
            futures[executor.submit(run_atp_arquivo, atp_path)] = atp_path

        for fut in as_completed(futures):
            atp_path = futures[fut]
            try:
                pl4_destino = fut.result()
                if pl4_destino is None:
                    print(f"[AVISO] {atp_path.name}: não gerou .pl4 (cópia em '{PASTA_CARTOES_NAO_RODADOS.name}')")
                else:
                    # Já foi movido em run_atp_arquivo; apenas registra
                    results.append(pl4_destino)
            except subprocess.TimeoutExpired:
                print(f"[ERRO] Timeout em {atp_path.name}.")
                _copia_cartao_nao_rodado(atp_path)
            except Exception as e:
                print(f"[ERRO] {atp_path.name}: {e}")
                _copia_cartao_nao_rodado(atp_path)
            finally:
                pbar.update(1)

        pbar.close()

    # Aguarda encerrar tpbig.exe remanescentes
    while conta_tpbig() > 0:
        time.sleep(0.5)

    # Limpeza final: remove quaisquer subpastas remanescentes dentro de TmpATP/
    for sub in PASTA_TMP_ATP.iterdir():
        if sub.is_dir():
            _limpa_dir(sub)

    print(f"Concluído. {len(results)} arquivos .pl4 salvos em '{PASTA_PL4.name}'.")
    print(f"Cartões que falharam foram copiados para '{PASTA_CARTOES_NAO_RODADOS.name}'.")
    print(f"Pasta temporária limpa: '{PASTA_TMP_ATP.name}'.")
    

if __name__ == "__main__":
    main()
