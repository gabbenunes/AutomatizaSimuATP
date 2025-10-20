#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extrator de arquivos PL4 do ATP com múltiplas abordagens e plotagem comparativa.

Este script:
1) Testa 7 abordagens diferentes para extrair dados de arquivos PL4
2) Salva os resultados em múltiplos formatos (pkl, npz, csv, json, hdf5, mat, txt)
3) Plota automaticamente todas as abordagens para comparação visual
"""

import os
import struct
import pickle
import json
import numpy as np
from typing import Dict


# ============================================================================
# AJUDANTES (NOVOS) — corte opcional antes de salvar o .parquet
# ============================================================================
def _calc_n_remover(fs_por_ciclo: int, freq_rede_hz: float, tempo_remover_s: float) -> int:
    """
    Fórmula pedida:
        N_remover = fs_por_ciclo * freq_rede_hz * tempo_remover_s
    Ex.: 128 * 60 * 1.0 = 7680
    """
    try:
        n = int(fs_por_ciclo * float(freq_rede_hz) * float(tempo_remover_s))
    except Exception:
        n = 0
    return max(n, 0)


def _apply_cut_full_dict(
    data_dict: Dict,
    *,
    remover_amostras: bool,
    fs_por_ciclo: int,
    freq_rede_hz: float,
    tempo_remover_s: float,
    remover_de: str,
) -> Dict:
    """
    Aplica corte em dicionário "completo": {'time': (N,), 'data': (n_var,N), 'labels': [...]}
    Não altera nada se o corte estiver desabilitado ou inválido.
    """
    if not (remover_amostras and tempo_remover_s > 0):
        return data_dict

    if "time" not in data_dict or "data" not in data_dict:
        return data_dict

    time = np.asarray(data_dict["time"])
    data = np.asarray(data_dict["data"])

    if time.ndim != 1 or data.ndim != 2 or data.shape[1] != time.shape[0]:
        return data_dict

    n_drop = _calc_n_remover(fs_por_ciclo, freq_rede_hz, tempo_remover_s)
    N = time.shape[0]
    if n_drop <= 0 or n_drop >= N:
        return data_dict

    if str(remover_de).lower() == "fim":
        time_new = time[:-n_drop]
        data_new = data[:, :-n_drop]
    else:
        time_new = time[n_drop:]
        data_new = data[:, n_drop:]

    out = dict(data_dict)
    out["time"] = time_new
    out["data"] = data_new
    out["n_samples"] = int(time_new.shape[0])
    if "delta_t" in out:
        dt = float(out["delta_t"])
        out["tmax"] = (out["n_samples"] - 1) * dt
    return out


def _apply_cut_interest_dict(
    data_dict: Dict,
    *,
    remover_amostras: bool,
    fs_por_ciclo: int,
    freq_rede_hz: float,
    tempo_remover_s: float,
    remover_de: str,
) -> Dict:
    """
    Aplica corte em dicionário de interesse: {'time'/ 'tempo': (N,), 'Var1': (N,), ...}
    Mantém chaves cujos valores não tenham comprimento N inalterados.
    """
    if not (remover_amostras and tempo_remover_s > 0):
        return data_dict

    time_key = "time" if "time" in data_dict else ("tempo" if "tempo" in data_dict else None)
    if time_key is None:
        return data_dict

    t = np.asarray(data_dict[time_key])
    if t.ndim != 1:
        return data_dict

    N = t.shape[0]
    n_drop = _calc_n_remover(fs_por_ciclo, freq_rede_hz, tempo_remover_s)
    if n_drop <= 0 or n_drop >= N:
        return data_dict

    def corta(x: np.ndarray) -> np.ndarray:
        if str(remover_de).lower() == "fim":
            return x[:-n_drop]
        return x[n_drop:]

    out = {}
    for k, v in data_dict.items():
        arr = np.asarray(v)
        if arr.ndim == 1 and arr.shape[0] == N:
            out[k] = corta(arr)
        else:
            out[k] = v
    return out


def readpl4(filepath: str) -> Dict:
    """
    Baseada na biblioteca readPL4 (método confiável)
    Usa o formato exato do ATP/EMTP conforme documentação
    """
    print(" Método readPL4 (baseado em mmap)")
    
    import mmap
    
    misc = {
        "deltat": 0.0,
        "nvar": 0,
        "pl4size": 0,
        "steps": 0,
        "tmax": 0.0,
    }
    
    with open(filepath, "rb") as f:
        pl4 = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # Lê metadados do cabeçalho PL4
        misc["deltat"] = struct.unpack("<f", pl4[40:44])[0]
        misc["nvar"] = struct.unpack("<L", pl4[48:52])[0] // 2
        misc["pl4size"] = struct.unpack("<L", pl4[56:60])[0] - 1
        
        misc["steps"] = (
            (misc["pl4size"] - 5 * 16 - misc["nvar"] * 16)
            // ((misc["nvar"] + 1) * 4)
        )
        misc["tmax"] = (misc["steps"] - 1) * misc["deltat"]
        
        # Lê cabeçalho de variáveis
        var_info = []
        for i in range(misc["nvar"]):
            pos = 5 * 16 + i * 16
            h = struct.unpack("3x1c6s6s", pl4[pos : pos + 16])
            var_info.append({
                "TYPE": int(h[0]),
                "FROM": h[1].decode('utf-8', errors='ignore').strip(),
                "TO": h[2].decode('utf-8', errors='ignore').strip()
            })
        
        # Calcula bytes nulos extras
        expsize = (5 + misc["nvar"]) * 16 + misc["steps"] * (misc["nvar"] + 1) * 4
        nullbytes = 0
        if misc["pl4size"] > expsize:
            nullbytes = misc["pl4size"] - expsize
        
        # Lê dados numéricos usando memmap
        data = np.memmap(
            f,
            dtype=np.float32,
            mode="r",
            shape=(misc["steps"], misc["nvar"] + 1),
            offset=(5 + misc["nvar"]) * 16 + nullbytes,
        )
        
        # Copia para array normal (memmap pode causar problemas)
        data_array = np.array(data)
        
        # Separa tempo e variáveis
        time = data_array[:, 0]
        data_matrix = data_array[:, 1:].T
        
        # Converte tipos
        type_map = {4: "V-node", 7: "E-bran", 8: "V-bran", 9: "I-bran"}
        for v in var_info:
            v["TYPE_NAME"] = type_map.get(v["TYPE"], f"Type-{v['TYPE']}")
        
        # Cria labels
        labels = [f"{v['FROM']}-{v['TO']} ({v['TYPE_NAME']})" for v in var_info]
        
        return {
            'data': data_matrix,
            'time': time,
            'method': 'readpl4_mmap_based',
            'n_channels': misc["nvar"],
            'n_samples': misc["steps"],
            'delta_t': misc["deltat"],
            'tmax': misc["tmax"],
            'var_info': var_info,
            'measurement_nodes': [v['FROM'] for v in var_info],
            'type_signals': [v['TYPE_NAME'] for v in var_info],
            'labels': labels
        }


# ============================================================================
# SALVAMENTO EM MÚLTIPLOS FORMATOS
# ============================================================================

def save_results_pkl(data_dict: Dict, output_folder: str, base_name: str, approach_num: int):
    """Salva resultados em múltiplos formatos"""
    
    prefix = f"approach_{approach_num}_{base_name}"
    
    # 1. Pickle (.pkl)
    pkl_path = os.path.join(output_folder, f"{prefix}.pkl")
    with open(pkl_path, 'wb') as f:
        pickle.dump(data_dict, f)
    print(f"    ✓ Salvo: {pkl_path}")

def save_results_parquet(
    output_path: str,
    data_dict: dict,
    *,
    usar_variaveis_interesse: bool | None = None,
    remover_amostras: bool = False,
    fs_por_ciclo: int = 128,
    freq_rede_hz: float = 60.0,
    tempo_remover_s: float = 0.0,
    remover_de: str = "inicio",
):
    """
    Salva o resultado do PL4 em formato .parquet, aceitando:
      A) {'time','data','labels',...}  (resultado completo do readpl4)
      B) {'time/tempo','V1a','V1b',...} (variáveis de interesse)

    Compatível com chamadas diretas:
        save_results_parquet("saida/arquivo.parquet", resultado)
    ou:
        save_results_parquet("saida/arquivo.parquet", medicoes, usar_variaveis_interesse=True)

    Também aplica corte opcional:
        N_remover = fs_por_ciclo * freq_rede_hz * tempo_remover_s
        remover_de in {"inicio","fim"}
    """
    import os
    import numpy as np
    import pandas as pd
    from collections import Counter

    parquet_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    # ---------------- helpers internos ----------------
    def _calc_n_remover(fs_pc: int, f_hz: float, t_s: float) -> int:
        try:
            n = int(fs_pc * float(f_hz) * float(t_s))
        except Exception:
            n = 0
        return max(n, 0)

    def _apply_cut_full_dict(d: dict) -> dict:
        if not (remover_amostras and tempo_remover_s > 0):
            return d
        if "time" not in d or "data" not in d:
            return d
        time = np.asarray(d["time"])
        data = np.asarray(d["data"])
        if time.ndim != 1 or data.ndim != 2 or data.shape[1] != time.shape[0]:
            return d
        n_drop = _calc_n_remover(fs_por_ciclo, freq_rede_hz, tempo_remover_s)
        N = time.shape[0]
        if n_drop <= 0 or n_drop >= N:
            return d
        if str(remover_de).lower() == "fim":
            time_new = time[:-n_drop]
            data_new = data[:, :-n_drop]
        else:
            time_new = time[n_drop:]
            data_new = data[:, n_drop:]
        out = dict(d)
        out["time"] = time_new
        out["data"] = data_new
        out["n_samples"] = int(time_new.shape[0])
        if "delta_t" in out:
            dt = float(out["delta_t"])
            out["tmax"] = (out["n_samples"] - 1) * dt
        return out

    def _apply_cut_interest_dict(d: dict) -> dict:
        if not (remover_amostras and tempo_remover_s > 0):
            return d
        time_key = "time" if "time" in d else ("tempo" if "tempo" in d else None)
        if time_key is None:
            return d
        t = np.asarray(d[time_key])
        if t.ndim != 1:
            return d
        N = t.shape[0]
        n_drop = _calc_n_remover(fs_por_ciclo, freq_rede_hz, tempo_remover_s)
        if n_drop <= 0 or n_drop >= N:
            return d

        def corta(x: np.ndarray) -> np.ndarray:
            return x[:-n_drop] if str(remover_de).lower() == "fim" else x[n_drop:]

        out = {}
        for k, v in d.items():
            arr = np.asarray(v)
            if arr.ndim == 1 and arr.shape[0] == N:
                out[k] = corta(arr)
            else:
                out[k] = v
        return out
    # ---------------------------------------------------

    try:
        # detectar tipo (completo ou interesse)
        if usar_variaveis_interesse is None:
            modo_interesse = not ("data" in data_dict and "labels" in data_dict)
        else:
            modo_interesse = bool(usar_variaveis_interesse)

        # aplicar corte
        if modo_interesse:
            data_dict = _apply_cut_interest_dict(data_dict)
        else:
            data_dict = _apply_cut_full_dict(data_dict)

        # ---------- Caso A: completo ----------
        if not modo_interesse:
            time = np.ravel(data_dict.get("time", []))
            data = np.asarray(data_dict["data"])
            labels = list(data_dict["labels"])
            counts = Counter(labels)
            if any(v > 1 for v in counts.values()):
                seen = {}
                new_labels = []
                for lbl in labels:
                    if counts[lbl] > 1:
                        seen[lbl] = seen.get(lbl, 0) + 1
                        new_labels.append(f"{lbl}_{seen[lbl]}")
                    else:
                        new_labels.append(lbl)
                labels = new_labels
                print("    ⚙️ Rótulos duplicados (modo completo) — renomeados automaticamente.")
            df = pd.DataFrame(data.T, columns=labels)
            df.insert(0, "time", time)

        # ---------- Caso B: interesse ----------
        else:
            time_key = "time" if "time" in data_dict else "tempo"
            time = np.ravel(data_dict[time_key])
            cols = [k for k in data_dict.keys() if k != time_key]
            frame = {}
            for k in cols:
                v = np.ravel(np.asarray(data_dict[k]))
                if len(v) != len(time):
                    raise ValueError(f"Tamanho inconsistente entre '{k}' ({len(v)}) e '{time_key}' ({len(time)}).")
                frame[k] = v
            counts = Counter(cols)
            if any(v > 1 for v in counts.values()):
                seen = {}
                new_cols = []
                for lbl in cols:
                    if counts[lbl] > 1:
                        seen[lbl] = seen.get(lbl, 0) + 1
                        new_cols.append(f"{lbl}_{seen[lbl]}")
                    else:
                        new_cols.append(lbl)
                frame = {new_cols[i]: frame[cols[i]] for i in range(len(cols))}
                print("    ⚙️ Rótulos duplicados (variáveis de interesse) — renomeados automaticamente.")
            df = pd.DataFrame(frame)
            df.insert(0, "time", time)

        # salvar parquet
        df.to_parquet(parquet_path, index=False)
        print(f"    ✓ Salvo: {parquet_path}")
        return parquet_path, df.shape[0], list(df.columns)

    except Exception as e:
        print(f"    ⚠️ Erro ao salvar .parquet: {e}")
        raise




def save_results_numpy(data_dict: Dict, output_folder: str, base_name: str):
    
    # 2. NumPy compressed (.npz)
    npz_path = os.path.join(output_folder, f".npz")
    np.savez_compressed(npz_path, **data_dict)
    print(f"    ✓ Salvo: {npz_path}")

def save_results_hdf5(data_dict: Dict, output_folder: str, base_name: str):
    # 5. HDF5 (requer h5py)
    try:
        import h5py
        h5_path = os.path.join(output_folder, f".h5")
        with h5py.File(h5_path, 'w') as f:
            for key, value in data_dict.items():
                if isinstance(value, np.ndarray):
                    f.create_dataset(key, data=value)
                elif isinstance(value, (int, float)):
                    f.attrs[key] = value
                elif isinstance(value, str):
                    f.attrs[key] = value
                elif isinstance(value, list):
                    f.attrs[key] = str(value)
        print(f"    ✓ Salvo: {h5_path}")
    except ImportError:
        print(f"    ⚠ HDF5 não disponível (instale h5py)")
    
def save_results_matlab(data_dict: Dict, output_folder: str, base_name: str):
    # 6. MATLAB (.mat)
    try:
        from scipy.io import savemat
        mat_path = os.path.join(output_folder, f".mat")
        mat_dict = {k: v for k, v in data_dict.items() if isinstance(v, np.ndarray)}
        savemat(mat_path, mat_dict)
        print(f"    ✓ Salvo: {mat_path}")
    except ImportError:
        print(f"    ⚠ MATLAB não disponível (instale scipy)")
