# -*- coding: utf-8 -*-
"""
PL4 -> MAT (via Pl42mat.exe, estilo MATLAB)
- Roda Pl42mat.exe dentro de ArquivoPL4 (cwd)
- Não passa caminho de saída (como no MATLAB): Pl42mat cria <stem>.MAT no cwd
- Aguarda surgimento e estabilização do .MAT
- Move .MAT para TmpMAT/

Estrutura esperada:
BASE/
  |-- Pl42mat.exe
  |-- ArquivoPL4/*.pl4
  |-- TmpMAT/
  |-- logs_pl4/

Dependências: tqdm (opcional, só para barra de progresso)
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import logging
from datetime import datetime
from tqdm import tqdm
import time
import os
import sys
import shutil
import traceback

# =========================
# Pastas / Config
# =========================
BASE_DIR = Path(__file__).resolve().parent
PASTA_PL4 = BASE_DIR / "ArquivoPL4"
PASTA_TMP = BASE_DIR / "ArquivosMAT"
PASTA_LOGS = BASE_DIR / "logs_pl4"
PL42MAT = BASE_DIR / "Pl42mat.exe"   # ajuste caso esteja noutro lugar

PASTA_TMP.mkdir(parents=True, exist_ok=True)
PASTA_LOGS.mkdir(parents=True, exist_ok=True)

NUMTHREADS = max(1, os.cpu_count() // 2)  # paralelismo das conversões
TIMEOUT_PL42MAT = 180           # seg. para execução do Pl42mat
WAIT_APPEAR_TIMEOUT = 120       # seg. para o .MAT "aparecer"
STABILIZE_TRIES = 6             # quantas leituras de tamanho para estabilizar
STABILIZE_INTERVAL = 0.5        # segundos entre leituras de tamanho

LOG_FILE = PASTA_LOGS / f"pl4_to_mat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PL4_TO_MAT")


# =========================
# Utilidades
# =========================
def _find_generated_mat_in_dir(dir_path: Path, stem: str) -> Path | None:
    """
    Procura um arquivo .mat/.MAT gerado pelo Pl42mat para o 'stem' informado
    dentro de 'dir_path'. Busca case-insensitive.
    """
    candidates = [
        dir_path / f"{stem}.mat",
        dir_path / f"{stem}.MAT",
        dir_path / f"{stem}.Mat",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    # fallback: vasculha tudo e procura por match no stem (case-insensitive)
    lower_stem = stem.lower()
    for p in dir_path.glob("*"):
        if p.is_file() and p.suffix.lower() == ".mat" and p.stem.lower() == lower_stem:
            return p
    return None


def _wait_for_mat_creation_and_stabilize(dir_path: Path, stem: str, appear_timeout_s: int) -> Path:
    """
    Espera o .MAT aparecer e estabilizar o tamanho. Retorna o Path do .MAT.
    Levanta exceção se não aparecer até o timeout.
    """
    t0 = time.time()
    mat_path = None

    # 1) Espera aparecer
    while time.time() - t0 < appear_timeout_s:
        mat_path = _find_generated_mat_in_dir(dir_path, stem)
        if mat_path is not None:
            break
        time.sleep(0.5)

    if mat_path is None:
        raise TimeoutError(f".MAT não apareceu para '{stem}' em {appear_timeout_s}s (cwd={dir_path})")

    # 2) Estabiliza tamanho
    last_size = -1
    stable_count = 0
    for _ in range(STABILIZE_TRIES):
        size = mat_path.stat().st_size
        if size == last_size and size > 0:
            stable_count += 1
        else:
            stable_count = 0
        last_size = size
        if stable_count >= 2:  # precisou ficar igual em duas leituras consecutivas
            break
        time.sleep(STABILIZE_INTERVAL)

    if last_size <= 0:
        raise RuntimeError(f".MAT encontrado, mas com tamanho inválido: {mat_path} (size={last_size})")

    return mat_path


def _run_pl42mat_like_matlab(pl4_path: Path) -> Path:
    """
    Executa Pl42mat.exe *sem* argumento de saída (comportamento MATLAB),
    com cwd=PASTA_PL4, espera o <stem>.MAT aparecer e estabilizar,
    move para TmpMAT e retorna o path final do .mat em TmpMAT.
    """
    if not PL42MAT.exists():
        raise FileNotFoundError(f"Pl42mat.exe não encontrado em: {PL42MAT}")

    stem = pl4_path.stem

    # Antes de rodar, apaga resíduos antigos do mesmo .MAT no cwd para evitar confusão
    for old in (PASTA_PL4 / f"{stem}.mat", PASTA_PL4 / f"{stem}.MAT", PASTA_PL4 / f"{stem}.Mat"):
        if old.exists():
            try:
                old.unlink()
            except Exception:
                pass

    # --- MODO "igual ao MATLAB": sem passar arquivo de saída ---
    # Se sua versão precisar de flags (ex.: "/c" para todos os canais), ajuste:
    # cmd = [str(PL42MAT), "/c", pl4_path.name]
    cmd = [str(PL42MAT), pl4_path.name]

    logger.info(f"Rodando Pl42mat (cwd=ArquivoPL4): {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=str(PASTA_PL4),           # roda dentro de ArquivoPL4 (como no MATLAB)
        check=False,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_PL42MAT
    )

    # Mesmo que retorne rápido, aguardamos o .MAT aparecer/estabilizar
    if proc.returncode != 0:
        logger.warning(
            f"Pl42mat retornou código {proc.returncode} para {pl4_path.name}.\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    mat_in_cwd = _wait_for_mat_creation_and_stabilize(
        PASTA_PL4, stem, appear_timeout_s=WAIT_APPEAR_TIMEOUT
    )

    # Move p/ TmpMAT (arquivos com mesmo nome são sobrescritos)
    final_mat = PASTA_TMP / mat_in_cwd.name
    shutil.move(str(mat_in_cwd), str(final_mat))
    logger.info(f".MAT estabilizado e movido: {final_mat}")
    return final_mat


def _process_one(pl4: Path):
    """
    Pipeline: PL4 -> (Pl42mat) -> MAT (TmpMAT)
    """
    try:
        mat_path = _run_pl42mat_like_matlab(pl4)
        # Apenas retorna o .MAT final (sem parquet, sem leitura)
        return mat_path, "OK"
    except subprocess.TimeoutExpired:
        err = f"{pl4.name}: Timeout executando Pl42mat (>{TIMEOUT_PL42MAT}s)"
        logger.error(err)
        return None, err
    except Exception as e:
        logger.error(f"{pl4.name}: ERRO: {e}")
        logger.debug(traceback.format_exc())
        return None, str(e)


def main():
    logger.info("=" * 70)
    logger.info("PL4 -> MAT (modo MATLAB-like)")
    logger.info(f"Base: {BASE_DIR}")
    logger.info(f"PL42MAT: {PL42MAT}")
    logger.info(f"Threads: {NUMTHREADS}")
    logger.info("=" * 70)

    if not PASTA_PL4.exists():
        logger.error(f"Pasta não encontrada: {PASTA_PL4}")
        sys.exit(1)

    pl4_files = sorted(PASTA_PL4.glob("*.pl4"))
    if not pl4_files:
        logger.warning(f"Nenhum .pl4 encontrado em {PASTA_PL4}")
        sys.exit(0)

    logger.info(f"Encontrados {len(pl4_files)} arquivo(s) .pl4")
    print(f"\nLog detalhado: {LOG_FILE}\n")

    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=NUMTHREADS) as ex:
        futures = {ex.submit(_process_one, p): p for p in pl4_files}
        with tqdm(total=len(pl4_files), desc="Convertendo para .MAT", ncols=100) as bar:
            for fut in as_completed(futures):
                result, msg = fut.result()
                if result:
                    ok += 1
                    bar.set_postfix_str("✓")
                else:
                    fail += 1
                    bar.set_postfix_str("✗")
                bar.update(1)

    print("\n" + "=" * 70)
    print(f"RESULTADO: {ok} sucesso(s), {fail} falha(s)")
    print(f"Tmp MATs:  {PASTA_TMP}")
    print(f"Logs:      {PASTA_LOGS}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
