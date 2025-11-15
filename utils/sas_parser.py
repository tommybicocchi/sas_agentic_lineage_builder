# utils/sas_parser.py

"""
Utility di parsing per codice SAS (estrazione statica di dipendenze).

Questa prima versione implementa:
- regex di base per DATA, SET, MERGE, OUTPUT, PROC, %INCLUDE, %MACRO, macro calls.
- funzioni extract_* per input/output datasets, macro calls, include files, proc types.
- parse_dependencies(code) che aggrega il tutto in un unico dict.
"""

import re
from typing import Dict, List, Set


# Regex di base (grezze ma utili per iniziare).
# In seguito potremo raffinare molto di più.

# DATA step: data lib.ds; oppure data ds;
RE_DATA = re.compile(
    r"""
    ^\s*data       # inizio riga + parola DATA
    \s+
    (?P<targets>   # group con uno o più dataset separati da spazio
        [\w\.]+
        (?:\s+[\w\.]+)*
    )
    \s*;           # fino al ;
    """,
    re.IGNORECASE | re.MULTILINE | re.VERBOSE,
)

# SET statement: set lib.ds; oppure set ds;
RE_SET = re.compile(
    r"""
    ^\s*set
    \s+
    (?P<sources>
        [\w\.]+
        (?:\s+[\w\.]+)*
    )
    \s*;
    """,
    re.IGNORECASE | re.MULTILINE | re.VERBOSE,
)

# MERGE: merge lib.ds1 lib.ds2;
RE_MERGE = re.compile(
    r"""
    ^\s*merge
    \s+
    (?P<sources>
        [\w\.]+
        (?:\s+[\w\.]+)*
    )
    \s*;
    """,
    re.IGNORECASE | re.MULTILINE | re.VERBOSE,
)

# PROC con OUT=/DATA= (semplificato)
RE_PROC_OUT = re.compile(
    r"""
    ^\s*proc\s+(?P<proc>\w+).*?
    (?:\bout\s*=\s*(?P<out_ds>[\w\.]+)|
       \bdata\s*=\s*(?P<data_ds>[\w\.]+))
    """,
    re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE,
)

# PROC generico per identificare tipo (per proc_types)
RE_PROC = re.compile(
    r"""
    ^\s*proc\s+(?P<proc>\w+)
    """,
    re.IGNORECASE | re.MULTILINE | re.VERBOSE,
)

# %INCLUDE 'path' o "path"
RE_INCLUDE = re.compile(
    r"""
    %include
    \s+
    (?P<path>
        ['"][^'"]+['"]
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# %MACRO nome
RE_MACRO_DEF = re.compile(
    r"""
    %macro
    \s+
    (?P<name>\w+)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Chiamata di macro: %nome(...)
RE_MACRO_CALL = re.compile(
    r"""
    %(?P<name>\w+)\s*\(
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Indicatore di dipendenze dinamiche (molto semplice)
RE_DYNAMIC = re.compile(
    r"""
    [&]       # macro variables &var
    """,
    re.VERBOSE,
)


def _normalize_dataset_name(name: str) -> str:
    """
    Normalizzazione base del nome dataset:
    - strip spazi
    - rimuovi trailing punti o punti e virgola
    """
    if not name:
        return ""
    return name.strip().rstrip(";.").lower()


def extract_output_datasets(code: str) -> List[str]:
    """
    Estrae i dataset di output da DATA step e da pattern OUT=/DATA= in PROC.

    Parameters
    ----------
    code : str

    Returns
    -------
    List[str]
    """
    outputs: Set[str] = set()

    # DATA step
    for match in RE_DATA.finditer(code):
        targets = match.group("targets")
        if targets:
            for t in targets.split():
                norm = _normalize_dataset_name(t)
                if norm:
                    outputs.add(norm)

    # OUT= e DATA= in PROC o altrove (semplificato):
    # cerchiamo pattern out=lib.ds e data=lib.ds
    re_out = re.compile(r"\bout\s*=\s*([\w\.]+)", re.IGNORECASE)
    re_data = re.compile(r"\bdata\s*=\s*([\w\.]+)", re.IGNORECASE)

    for match in re_out.finditer(code):
        ds = match.group(1)
        norm = _normalize_dataset_name(ds)
        if norm:
            outputs.add(norm)

    for match in re_data.finditer(code):
        ds = match.group(1)
        norm = _normalize_dataset_name(ds)
        if norm:
            outputs.add(norm)

    return sorted(outputs)


def extract_input_datasets(code: str) -> List[str]:
    """
    Estrae i dataset di input da SET e MERGE.

    Parameters
    ----------
    code : str

    Returns
    -------
    List[str]
    """
    inputs: Set[str] = set()

    # SET
    for match in RE_SET.finditer(code):
        sources = match.group("sources")
        if sources:
            for s in sources.split():
                norm = _normalize_dataset_name(s)
                if norm:
                    inputs.add(norm)

    # MERGE
    for match in RE_MERGE.finditer(code):
        sources = match.group("sources")
        if sources:
            for s in sources.split():
                norm = _normalize_dataset_name(s)
                if norm:
                    inputs.add(norm)

    return sorted(inputs)


def extract_macro_calls(code: str) -> List[str]:
    """
    Estrae i nomi delle macro chiamate (%nome(...)).

    Parameters
    ----------
    code : str

    Returns
    -------
    List[str]
    """
    macros: Set[str] = set()

    for match in RE_MACRO_CALL.finditer(code):
        name = match.group("name")
        if name:
            macros.add(name.lower())

    return sorted(macros)


def extract_include_files(code: str) -> List[str]:
    """
    Estrae i path dei file inclusi tramite %INCLUDE.

    Parameters
    ----------
    code : str

    Returns
    -------
    List[str]
    """
    includes: Set[str] = set()

    for match in RE_INCLUDE.finditer(code):
        path = match.group("path")
        if path:
            # rimuovi apici
            cleaned = path.strip().strip("'\"")
            if cleaned:
                includes.add(cleaned)

    return sorted(includes)


def extract_proc_types(code: str) -> List[str]:
    """
    Estrae i tipi di PROC (es. sql, sort, means).

    Parameters
    ----------
    code : str

    Returns
    -------
    List[str]
    """
    procs: Set[str] = set()

    for match in RE_PROC.finditer(code):
        p = match.group("proc")
        if p:
            procs.add(p.lower())

    return sorted(procs)


def detect_dynamic_dependencies(code: str) -> bool:
    """
    Ritorna True se il codice contiene indicazioni forti di dipendenze dinamiche,
    ad esempio macro variabili (&var).

    Parameters
    ----------
    code : str

    Returns
    -------
    bool
    """
    return bool(RE_DYNAMIC.search(code))


def parse_dependencies(code: str) -> Dict[str, object]:
    """
    Funzione di alto livello che estrae tutte le informazioni di dipendenza
    richieste dal ParserAgent per la tabella raw_dependencies.

    Returns
    -------
    dict con chiavi:
      - input_datasets: List[str]
      - output_datasets: List[str]
      - macro_calls: List[str]
      - include_files: List[str]
      - has_dynamic_deps: bool
      - proc_types: List[str]
    """
    if not code:
        code = ""

    inputs = extract_input_datasets(code)
    outputs = extract_output_datasets(code)
    macros = extract_macro_calls(code)
    includes = extract_include_files(code)
    procs = extract_proc_types(code)
    dynamic = detect_dynamic_dependencies(code)

    return {
        "input_datasets": inputs,
        "output_datasets": outputs,
        "macro_calls": macros,
        "include_files": includes,
        "has_dynamic_deps": dynamic,
        "proc_types": procs,
    }