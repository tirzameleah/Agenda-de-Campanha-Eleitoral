#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_agendas.py
====================

Pipeline único de agenda de campanha (eleições estaduais 2026): busca notícias
no Google Notícias, monta um rascunho curado por dia/candidato e confere
inconsistências antes de virar PPTX/PDF.

Une em um arquivo só o que antes eram três scripts separados:
buscar_agendas_candidatos.py + montar_agenda_curada.py + conferir_recorte.py.

COMO USAR
---------
No VS Code, edite as duas datas em CONFIGURAÇÃO logo abaixo e dê Run (F5) —
não precisa passar nada no terminal.

Ou por linha de comando, se preferir:
    python pipeline_agendas.py --inicio 2026-09-29 --fim 2026-10-02

PARA MUDAR O RECORTE DA SEMANA
-------------------------------
Só mexer nestas duas linhas, logo abaixo:
    INICIO = "2026-09-29"
    FIM    = "2026-09-29"
Exemplo "29 à 02/10" -> INICIO = "2026-09-29", FIM = "2026-10-02".

O QUE MUDOU EM RELAÇÃO AOS 3 SCRIPTS ORIGINAIS (revisão + eficiência)
-----------------------------------------------------------------------
- A lista CANDIDATOS existia duplicada em dois arquivos (buscar e montar);
  se um fosse editado e o outro não, c1/c2 podiam sair trocados sem
  avisar ninguém. Agora há uma lista só, usada em todo o pipeline.
- As buscas no Google Notícias eram sequenciais (26 candidatos x 3 queries,
  1s de pausa cada = na prática minutos). Agora rodam em paralelo por
  candidato (PARALELISMO workers), mantendo a mesma pausa entre as 3
  queries de cada candidato — mesmas queries, mesmo filtro de data, mesma
  deduplicação por link: só menos tempo de parede, sem perder cobertura.
- Falhas de rede (SSL/DNS, vimos casos reais em execuções anteriores para
  Lorenzo Pazolini e Requião Filho) agora tentam de novo (TENTATIVAS) antes
  de desistir daquela query, em vez de perder o candidato silenciosamente.
- A pasta de saída agora é relativa à localização deste arquivo, não à
  pasta em que o terminal estava aberto — evita o "cadê a pasta saida_*"
  que já aconteceu nesta conversa quando os scripts foram movidos.
- O nome da pasta de saída é gerado sozinho a partir das datas
  (ex.: saida_29set_02out), então não precisa digitar --saida toda vez.

O que NÃO mudou: as queries de busca, os critérios de relevância, o filtro
de data e todas as checagens de qualidade do conferir_recorte original
continuam exatamente iguais — a precisão da pesquisa não foi tocada.

LEMBRETE
--------
Isto é busca automatizada + pré-seleção por relevância, não curadoria
manual. O campo "analise" sai em branco de propósito. O agenda_curada.json
gerado aqui é o que alimenta o preenchimento do PPTX que depois vira PDF —
revise o conteúdo antes desse próximo passo.
"""

import argparse
import csv
import json
import re
import sys
import threading
import time
import unicodedata
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import feedparser
import requests

# ============================================================
# CONFIGURAÇÃO — mude aqui o recorte da semana
# ============================================================
INICIO = "2026-09-29"      # data inicial do recorte (YYYY-MM-DD)
FIM = "2026-09-29"         # data final do recorte (YYYY-MM-DD)
PASTA_SAIDA = None          # None = gera sozinho (ex.: saida_29set_02out)
TOP_N_POR_DIA = 3           # quantas notícias por candidato/dia entram no rascunho
PARALELISMO = 6             # buscas de candidatos em paralelo
PAUSA_SEG = 0.6             # pausa entre as 3 queries de cada candidato
TENTATIVAS = 2              # tentativas por query antes de desistir dela

# Depois que você já rodou a busca uma vez e editou o agenda_curada.json à
# mão: mude para True e dê Run de novo — pula a busca e só confere o que
# você editou. Volte para False quando for buscar um recorte novo.
SO_CONFERIR = True

# ============================================================

LIMITE_ANALISE = 520     # caracteres que cabem na caixa de análise
LIMITE_ITEM = 170        # caracteres confortáveis num card de dia

MESES_ABREV_NUM = {1: "JAN", 2: "FEV", 3: "MAR", 4: "ABR", 5: "MAI", 6: "JUN",
                    7: "JUL", 8: "AGO", 9: "SET", 10: "OUT", 11: "NOV", 12: "DEZ"}
MESES_ABREV = {v: k for k, v in MESES_ABREV_NUM.items()}
MESES = {"janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
         "maio": 5, "junho": 6, "julho": 7, "agosto": 8, "setembro": 9,
         "outubro": 10, "novembro": 11, "dezembro": 12}

# --------------------------------------------------------------------------
# BASE DE CANDIDATOS (fonte única — usada na busca e na montagem do rascunho)
# --------------------------------------------------------------------------
CANDIDATOS = [
    {"uf": "SP", "estado": "São Paulo", "nome": "Tarcísio de Freitas", "partido": "REP"},
    {"uf": "SP", "estado": "São Paulo", "nome": "Fernando Haddad", "partido": "PT"},

    {"uf": "RJ", "estado": "Rio de Janeiro", "nome": "Eduardo Paes", "partido": "PSD"},
    {"uf": "RJ", "estado": "Rio de Janeiro", "nome": "Douglas Ruas", "partido": "PL"},

    {"uf": "MG", "estado": "Minas Gerais", "nome": "Cleitinho", "partido": "REP"},
    {"uf": "MG", "estado": "Minas Gerais", "nome": "Patrus Ananias", "partido": "PT"},

    {"uf": "GO", "estado": "Goiás", "nome": "Daniel Vilela", "partido": "MDB"},
    {"uf": "GO", "estado": "Goiás", "nome": "Marconi Perillo", "partido": "PSDB"},

    {"uf": "RS", "estado": "Rio Grande do Sul", "nome": "Zucco", "partido": "PL"},
    {"uf": "RS", "estado": "Rio Grande do Sul", "nome": "Juliana Brizola", "partido": "PDT"},

    {"uf": "PE", "estado": "Pernambuco", "nome": "Raquel Lyra", "partido": "PSD"},
    {"uf": "PE", "estado": "Pernambuco", "nome": "João Campos", "partido": "PSB"},

    {"uf": "BA", "estado": "Bahia", "nome": "ACM Neto", "partido": "UNIÃO"},
    {"uf": "BA", "estado": "Bahia", "nome": "Jerônimo Rodrigues", "partido": "PT"},

    {"uf": "ES", "estado": "Espírito Santo", "nome": "Ricardo Ferraço", "partido": "MDB"},
    {"uf": "ES", "estado": "Espírito Santo", "nome": "Lorenzo Pazolini", "partido": "REP"},

    {"uf": "MA", "estado": "Maranhão", "nome": "Eduardo Braide", "partido": "PSD"},
    {"uf": "MA", "estado": "Maranhão", "nome": "Orleans Brandão", "partido": "MDB"},

    {"uf": "PA", "estado": "Pará", "nome": "Hana Ghassan", "partido": "MDB"},
    {"uf": "PA", "estado": "Pará", "nome": "Dr. Daniel Santos", "partido": "PODE"},

    {"uf": "SC", "estado": "Santa Catarina", "nome": "Jorginho Mello", "partido": "PL"},
    {"uf": "SC", "estado": "Santa Catarina", "nome": "João Rodrigues", "partido": "PT"},

    {"uf": "PR", "estado": "Paraná", "nome": "Sergio Moro", "partido": "PL"},
    {"uf": "PR", "estado": "Paraná", "nome": "Requião Filho", "partido": "PDT"},

    {"uf": "CE", "estado": "Ceará", "nome": "Ciro Gomes", "partido": "PSDB"},
    {"uf": "CE", "estado": "Ceará", "nome": "Elmano de Freitas", "partido": "PT"},
]

PALAVRAS_AGENDA = [
    "agenda", "caminhada", "carreata", "motociata", "comício", "sabatina",
    "debate", "entrevista", "visita", "reunião", "encontro", "panfletagem",
    "campanha", "candidato", "candidata", "governador", "governadora",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PesquisaAgendasCandidatos/1.0)"
}

# Não são "nomes próprios de agenda": ignorados na checagem de análise x grade.
STOP = {
    "foco", "semana", "segunda", "terça", "terca", "quarta", "quinta", "sexta",
    "sábado", "sabado", "domingo", "janeiro", "fevereiro", "março", "marco",
    "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro",
    "novembro", "dezembro", "estado", "capital", "interior", "região",
    "regiao", "campanha", "governo", "agenda", "candidato", "candidata",
    "atenção", "atencao", "nenhuma", "sem", "não", "nao", "a", "o", "e",
    "norte", "sul", "leste", "oeste", "centro", "baixada", "agreste",
    "sertão", "sertao", "litoral", "fronteira", "entorno", "vale",
    "triângulo", "triangulo", "serra", "grande", "zona", "metropolitana",
    "região metropolitana", "regiao metropolitana", "fronteira oeste",
    "baixada fluminense", "entorno do df", "norte de minas", "vale do aço",
    "vale do aco", "sul de minas", "zona norte", "zona oeste", "zona leste",
    "zona sul", "grande florianópolis", "grande florianopolis",
}

FRASES_AUSENCIA = [
    "sem agenda divulgada", "não informou agenda", "nao informou agenda",
    "agenda não confirmada", "agenda nao confirmada", "não enviou agenda",
    "nao enviou agenda", "não divulgou agenda", "nao divulgou agenda",
    "sem compromissos",
]


@dataclass
class Noticia:
    uf: str
    estado: str
    candidato: str
    partido: str
    titulo: str
    link: str
    fonte: str
    data_publicacao: str  # ISO yyyy-mm-dd
    relevancia: int = 0


# ==================== ETAPA 1 — BUSCA (ex-buscar_agendas_candidatos.py) ====================

def montar_queries(nome: str, estado: str) -> list:
    nome_aspas = f'"{nome}"'
    return [
        f"{nome_aspas} agenda campanha {estado}",
        f"{nome_aspas} governador {estado}",
        f"{nome_aspas} caminhada OR carreata OR sabatina OR comício",
    ]


def buscar_google_news_rss(query: str, dias_janela: int) -> list:
    url = (
        "https://news.google.com/rss/search?"
        + urllib.parse.urlencode({
            "q": f"{query} when:{dias_janela}d",
            "hl": "pt-BR",
            "gl": "BR",
            "ceid": "BR:pt",
        })
    )
    ultimo_erro = None
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return feedparser.parse(resp.content).entries
        except requests.RequestException as exc:
            ultimo_erro = exc
            if tentativa < TENTATIVAS:
                time.sleep(1.5 * tentativa)
    print(f"  [aviso] falha ao buscar '{query}' após {TENTATIVAS} tentativa(s): {ultimo_erro}")
    return []


def extrair_data_iso(entry) -> str:
    if getattr(entry, "published_parsed", None):
        dt = datetime(*entry.published_parsed[:6])
        return dt.date().isoformat()
    return ""


def extrair_fonte(entry) -> str:
    src = getattr(entry, "source", None)
    if src and getattr(src, "title", None):
        return src.title
    if " - " in entry.title:
        return entry.title.rsplit(" - ", 1)[-1]
    return ""


def calcular_relevancia(titulo: str, nome: str) -> int:
    score = 0
    titulo_low = titulo.lower()
    if nome.lower() in titulo_low:
        score += 3
    for palavra in PALAVRAS_AGENDA:
        if palavra in titulo_low:
            score += 1
    return score


def dentro_do_periodo(data_iso: str, inicio: date, fim: date) -> bool:
    if not data_iso:
        return False
    try:
        d = datetime.strptime(data_iso, "%Y-%m-%d").date()
    except ValueError:
        return False
    return inicio <= d <= fim


def buscar_tudo(inicio: date, fim: date) -> list:
    hoje = date.today()
    dias_janela = max((hoje - inicio).days + 2, 3)
    total = len(CANDIDATOS)
    concluidos = 0
    lock = threading.Lock()

    def tarefa(cand):
        nonlocal concluidos
        vistos_links = set()
        itens = []
        for query in montar_queries(cand["nome"], cand["estado"]):
            for entry in buscar_google_news_rss(query, dias_janela):
                link = getattr(entry, "link", "")
                if not link or link in vistos_links:
                    continue
                vistos_links.add(link)

                data_iso = extrair_data_iso(entry)
                if not dentro_do_periodo(data_iso, inicio, fim):
                    continue

                titulo = getattr(entry, "title", "").strip()
                itens.append(Noticia(
                    uf=cand["uf"], estado=cand["estado"], candidato=cand["nome"],
                    partido=cand["partido"], titulo=titulo, link=link,
                    fonte=extrair_fonte(entry), data_publicacao=data_iso,
                    relevancia=calcular_relevancia(titulo, cand["nome"]),
                ))
            time.sleep(PAUSA_SEG)
        with lock:
            concluidos += 1
            print(f"[{concluidos}/{total}] {cand['nome']} ({cand['uf']}) — {len(itens)} item(ns)")
        return itens

    resultados = []
    with ThreadPoolExecutor(max_workers=PARALELISMO) as ex:
        for itens in ex.map(tarefa, CANDIDATOS):
            resultados.extend(itens)

    resultados.sort(key=lambda n: (n.uf, n.candidato, n.data_publicacao, -n.relevancia))
    return resultados


def salvar_brutas(resultados: list, pasta_saida: Path):
    pasta_saida.mkdir(parents=True, exist_ok=True)

    agrupado = {}
    for n in resultados:
        agrupado.setdefault(n.uf, {}).setdefault(n.candidato, []).append({
            "data": n.data_publicacao,
            "titulo": n.titulo,
            "fonte": n.fonte,
            "link": n.link,
            "relevancia": n.relevancia,
        })

    with open(pasta_saida / "agendas_brutas.json", "w", encoding="utf-8") as f:
        json.dump(agrupado, f, ensure_ascii=False, indent=2)

    with open(pasta_saida / "agendas_por_dia.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UF", "Estado", "Candidato", "Partido", "Data", "Título", "Fonte", "Relevância", "Link"])
        for n in resultados:
            writer.writerow([n.uf, n.estado, n.candidato, n.partido, n.data_publicacao,
                              n.titulo, n.fonte, n.relevancia, n.link])

    with open(pasta_saida / "resumo.txt", "w", encoding="utf-8") as f:
        f.write("Resumo de itens encontrados por estado / candidato\n")
        f.write("=" * 55 + "\n\n")
        for uf, candidatos in agrupado.items():
            f.write(f"{uf}\n")
            for nome, itens in candidatos.items():
                f.write(f"  - {nome}: {len(itens)} item(ns)\n")
            f.write("\n")

    print(f"\nSalvo em: {pasta_saida.resolve()}")
    print(" - agendas_brutas.json")
    print(" - agendas_por_dia.csv")
    print(" - resumo.txt")


# ==================== ETAPA 2 — RASCUNHO CURADO (ex-montar_agenda_curada.py) ====================

def formatar_item(titulo: str) -> str:
    t = titulo.strip()
    if t and t[0].islower():
        t = t[0].upper() + t[1:]
    if t and not t.rstrip().endswith((".", "!", "?")):
        t = t.rstrip() + "."
    return t


def montar_agenda_curada(resultados: list, inicio: date, fim: date) -> dict:
    dias_datas = []
    d = inicio
    while d <= fim:
        dias_datas.append(d)
        d += timedelta(days=1)
    dias_rotulos = [f"{d.day:02d}/{MESES_ABREV_NUM[d.month]}" for d in dias_datas]

    por_uf_candidato = {}
    for n in resultados:
        por_uf_candidato.setdefault(n.uf, {}).setdefault(n.candidato, []).append(n)

    por_uf = {}
    for c in CANDIDATOS:
        por_uf.setdefault(c["uf"], {"estado": c["estado"], "nomes": []})
        por_uf[c["uf"]]["nomes"].append(c["nome"])

    estados = {}
    for uf, info in por_uf.items():
        bloco_estado = {}
        for idx, nome in enumerate(info["nomes"][:2]):
            chave = f"c{idx + 1}"
            noticias = por_uf_candidato.get(uf, {}).get(nome, [])
            por_dia = {}
            for n in noticias:
                por_dia.setdefault(n.data_publicacao, []).append(n)

            agenda, fontes = [], []
            for dt in dias_datas:
                iso = dt.isoformat()
                itens_dia = sorted(por_dia.get(iso, []), key=lambda n: -n.relevancia)[:TOP_N_POR_DIA]
                if itens_dia:
                    agenda.append([formatar_item(n.titulo) for n in itens_dia])
                    fontes_dia = sorted({n.fonte.strip() for n in itens_dia if n.fonte})
                    fontes.append(", ".join(fontes_dia) if fontes_dia else "")
                else:
                    agenda.append(["Sem agenda divulgada."])
                    fontes.append("")

            bloco_estado[chave] = {
                "nome": nome,
                "agenda": agenda,
                "fontes": fontes,
                "analise": "",
            }
        estados[info["estado"]] = bloco_estado

    return {"dias": dias_rotulos, "estados": estados}


# ==================== ETAPA 3 — CONFERÊNCIA (ex-conferir_recorte.py) ====================

def sa(s):
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn").lower()


def nomes_proprios(texto):
    sem_inicio = re.sub(r"(^|[.!?;:]\s+)([A-ZÁÂÃÀÉÊÍÓÔÕÚÇ])", lambda m: m.group(1) + m.group(2).lower(), texto)
    padrao = r"\b[A-ZÁÂÃÀÉÊÍÓÔÕÚÇ][\wÀ-ÿ]+(?:\s+(?:d[aeo]s?|e)\s+[A-ZÁÂÃÀÉÊÍÓÔÕÚÇ][\wÀ-ÿ]+|\s+[A-ZÁÂÃÀÉÊÍÓÔÕÚÇ][\wÀ-ÿ]+)*"
    achados = re.findall(padrao, sem_inicio)
    out = []
    for a in achados:
        a = a.strip()
        if len(a) < 4 or sa(a) in STOP:
            continue
        out.append(a)
    return out


def datas_citadas(texto):
    achados = []
    for m in re.finditer(r"\b(\d{1,2})\s+de\s+([A-Za-zçÇ]+)", texto):
        mes = MESES.get(sa(m.group(2)))
        if mes:
            achados.append((int(m.group(1)), mes, m.group(0)))
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})\b", texto):
        achados.append((int(m.group(1)), int(m.group(2)), m.group(0)))
    return achados


def janela(dias_rotulos):
    pares = []
    for r in dias_rotulos:
        m = re.match(r"\s*(\d{1,2})\s*/\s*([A-Za-z]{3})", r)
        if m and m.group(2).upper() in MESES_ABREV:
            pares.append((int(m.group(1)), MESES_ABREV[m.group(2).upper()]))
    return pares


def conferir(dados):
    erros, avisos = [], []
    dias_rot = dados.get("dias") or []
    pares_janela = janela(dias_rot)
    estados = dados.get("estados") or {}

    if not dias_rot:
        erros.append("[estrutura] JSON sem a chave 'dias'.")
    if not estados:
        erros.append("[estrutura] JSON sem a chave 'estados'.")

    variantes_ausencia = set()

    for estado, bloco in estados.items():
        grades = {}
        for chave in ("c1", "c2"):
            cand = bloco.get(chave)
            if not cand:
                erros.append(f"[estrutura] {estado}: falta '{chave}'.")
                continue
            nome = cand.get("nome") or chave
            agenda = cand.get("agenda") or []
            fontes = cand.get("fontes") or []

            if dias_rot and len(agenda) != len(dias_rot):
                erros.append(f"[estrutura] {estado}/{nome}: {len(agenda)} dias na agenda "
                             f"mas {len(dias_rot)} rótulos em 'dias'.")

            preenchidos = []
            for i, itens in enumerate(agenda):
                itens = itens or []
                rotulo = dias_rot[i] if i < len(dias_rot) else f"dia{i+1}"
                reais = [t for t in itens
                         if not any(f in sa(t) for f in FRASES_AUSENCIA)]
                for t in itens:
                    for f in FRASES_AUSENCIA:
                        if f in sa(t):
                            variantes_ausencia.add(t.strip())
                if reais:
                    preenchidos.append(i)
                    tem_fonte = i < len(fontes) and fontes[i]
                    if not tem_fonte:
                        erros.append(f"[fonte] {estado}/{nome} {rotulo}: "
                                     f"{len(reais)} item(ns) sem fonte registrada.")
                for t in itens:
                    if len(t) > LIMITE_ITEM:
                        avisos.append(f"[tamanho] {estado}/{nome} {rotulo}: item com "
                                      f"{len(t)} caracteres, pode transbordar.")
                    if t and t[0].islower():
                        avisos.append(f"[estilo] {estado}/{nome} {rotulo}: item começa "
                                      f"em minúscula — {t[:40]!r}")
                    if t and not t.rstrip().endswith((".", "!", "?")):
                        avisos.append(f"[estilo] {estado}/{nome} {rotulo}: item sem ponto "
                                      f"final — {t[:40]!r}")
            grades[chave] = {"nome": nome, "preenchidos": preenchidos,
                             "agenda": agenda, "analise": cand.get("analise") or ""}

        for chave, g in grades.items():
            analise = g["analise"]
            if not analise:
                continue
            if len(analise) > LIMITE_ANALISE:
                avisos.append(f"[tamanho] {estado}/{g['nome']}: análise com {len(analise)} "
                              f"caracteres (limite confortável {LIMITE_ANALISE}).")

            texto_grade = sa(" ".join(" ".join(x or []) for x in g["agenda"]))

            def ausente_da_grade(nome):
                if sa(nome) in texto_grade:
                    return False
                palavras = [w for w in re.split(r"\s+|,", nome)
                            if len(w) > 3 and sa(w) not in STOP]
                if not palavras:
                    return False
                return all(sa(w) not in texto_grade for w in palavras)

            ausentes = [n for n in nomes_proprios(analise) if ausente_da_grade(n)]
            ausentes = [n for n in ausentes
                        if sa(n) not in sa(g["nome"]) and sa(n) not in sa(estado)]
            vistos, unicos = set(), []
            for n in ausentes:
                if sa(n) not in vistos:
                    vistos.add(sa(n)); unicos.append(n)
            ausentes = unicos

            if not g["preenchidos"] and len(ausentes) >= 3:
                erros.append(
                    f"[contradição] {estado}/{g['nome']}: a grade está inteiramente "
                    f"vazia, mas a análise afirma atividade — cita "
                    f"{', '.join(ausentes[:5])}. Ou os eventos sobem para a grade "
                    f"com fonte, ou a análise precisa ser reescrita.")
            elif ausentes:
                avisos.append(f"[análise] {estado}/{g['nome']}: cita "
                              f"{', '.join(ausentes[:6])} — não aparece(m) na grade "
                              f"deste candidato.")

            if pares_janela:
                for dia, mes, txt in datas_citadas(analise):
                    if (dia, mes) not in pares_janela:
                        erros.append(f"[fora do recorte] {estado}/{g['nome']}: análise cita "
                                     f"'{txt}', fora da janela "
                                     f"{dias_rot[0]}–{dias_rot[-1]}.")

        if len(grades) == 2:
            c1, c2 = grades.get("c1"), grades.get("c2")
            if c1 and c2:
                for a, b in ((c1, c2), (c2, c1)):
                    vazios = [i for i in range(len(a["agenda"])) if i not in a["preenchidos"]]
                    corrida, maior = 0, 0
                    for i in range(len(a["agenda"])):
                        corrida = corrida + 1 if i in vazios else 0
                        maior = max(maior, corrida)
                    if maior >= 3 and len(b["preenchidos"]) >= 3:
                        avisos.append(
                            f"[cobertura] {estado}/{a['nome']}: {maior} dias vazios "
                            f"seguidos enquanto {b['nome']} tem {len(b['preenchidos'])} "
                            f"dias com agenda — provável falha de fonte, não de campanha.")

    if len(variantes_ausencia) > 1:
        avisos.append("[padronização] mais de uma formulação para ausência de agenda: "
                      + " | ".join(sorted(variantes_ausencia)[:6]))

    return erros, avisos


# ==================== ORQUESTRAÇÃO ====================

def nome_pasta_saida(inicio: date, fim: date) -> str:
    def rot(d):
        return f"{d.day:02d}{MESES_ABREV_NUM[d.month].lower()}"
    if inicio.month == fim.month:
        return f"saida_{inicio.day:02d}_{rot(fim)}"
    return f"saida_{rot(inicio)}_{rot(fim)}"


def imprimir_relatorio(erros, avisos, so_erros=False):
    if erros:
        print(f"\nERROS ({len(erros)})")
        for e in erros:
            print("  ✗ " + e)
    if avisos and not so_erros:
        print(f"\nAVISOS ({len(avisos)})")
        for a in avisos:
            print("  ! " + a)
    if not erros and not avisos:
        print("\nNada a apontar.")
    elif not erros:
        print(f"\nNenhum erro bloqueante. {len(avisos)} aviso(s) para olhar.")


def main():
    global TOP_N_POR_DIA, PARALELISMO, PAUSA_SEG

    p = argparse.ArgumentParser(description="Busca -> rascunho curado -> conferência, tudo em um passo.")
    p.add_argument("--inicio", default=INICIO, help="Data inicial (YYYY-MM-DD)")
    p.add_argument("--fim", default=FIM, help="Data final (YYYY-MM-DD)")
    p.add_argument("--saida", default=PASTA_SAIDA, help="Pasta de saída (padrão: gerado a partir das datas)")
    p.add_argument("--top", type=int, default=TOP_N_POR_DIA, help="Notícias por candidato/dia no rascunho")
    p.add_argument("--paralelismo", type=int, default=PARALELISMO, help="Buscas de candidatos em paralelo")
    p.add_argument("--pausa", type=float, default=PAUSA_SEG, help="Pausa (s) entre queries de um mesmo candidato")
    p.add_argument("--so-erros", action="store_true", help="Omite os avisos na conferência final")
    p.add_argument("--so-conferir", nargs="?", const="__auto__",
                    default=("__auto__" if SO_CONFERIR else None), metavar="ARQUIVO",
                    help="Pula a busca e só roda a conferência num agenda_curada.json já existente "
                         "(o que você editou à mão). Sem caminho, usa o da pasta de saída do recorte "
                         "(--saida, ou a gerada a partir de --inicio/--fim). Padrão vem de SO_CONFERIR "
                         "no topo do arquivo.")
    args = p.parse_args()

    inicio = date.fromisoformat(args.inicio)
    fim = date.fromisoformat(args.fim)
    pasta_saida = Path(args.saida) if args.saida else Path(__file__).parent / nome_pasta_saida(inicio, fim)

    if args.so_conferir is not None:
        caminho_curado = (Path(args.so_conferir) if args.so_conferir != "__auto__"
                          else pasta_saida / "agenda_curada.json")
        print(f"Conferindo {caminho_curado} (sem rodar a busca de novo)...")
        dados_curados = json.loads(caminho_curado.read_text(encoding="utf-8"))
        erros, avisos = conferir(dados_curados)
        imprimir_relatorio(erros, avisos, args.so_erros)
        sys.exit(1 if erros else 0)

    TOP_N_POR_DIA, PARALELISMO, PAUSA_SEG = args.top, args.paralelismo, args.pausa

    print(f"1/3 — Buscando notícias de {inicio.isoformat()} até {fim.isoformat()} para "
          f"{len(CANDIDATOS)} candidatos em {len(set(c['uf'] for c in CANDIDATOS))} estados "
          f"({PARALELISMO} em paralelo)...\n")
    resultados = buscar_tudo(inicio, fim)
    print(f"\nTotal de itens relevantes encontrados no período: {len(resultados)}")
    salvar_brutas(resultados, pasta_saida)

    print("\n2/3 — Montando rascunho curado (top notícias por candidato/dia)...")
    dados_curados = montar_agenda_curada(resultados, inicio, fim)
    caminho_curado = pasta_saida / "agenda_curada.json"
    caminho_curado.write_text(json.dumps(dados_curados, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Rascunho salvo em: {caminho_curado}")
    print("ATENÇÃO: pré-preenchido automaticamente por relevância — revise antes de usar no PPTX/PDF.")

    print("\n3/3 — Conferindo o rascunho...")
    erros, avisos = conferir(dados_curados)
    imprimir_relatorio(erros, avisos, args.so_erros)
    sys.exit(1 if erros else 0)


if __name__ == "__main__":
    main()
