#!/usr/bin/env python3
"""Coleta catálogo multiclasse e dados de mercado para o Radar.

Catálogo de descoberta: diretório público CotaLab (ações, FIIs, ETFs e BDRs).
Fundamentos disponíveis: Fundamentus para ações e FIIs.
Histórico de preços/proventos: Yahoo Finance via yfinance, sujeito a disponibilidade.
A lista do CotaLab é uma fonte secundária, não uma declaração oficial da B3.
"""
from __future__ import annotations
import concurrent.futures as cf
import datetime as dt
import json, math, re, time
from io import StringIO
from pathlib import Path
import pandas as pd
import requests
import yfinance as yf

OUT=Path('dados.json'); CATALOGO=Path('catalogo_ativos.json')
HEADERS={'User-Agent':'Mozilla/5.0 (compatible; RadarDeDividendos/2.0)'}
TIMEOUT=45; HISTORICO_ANOS=3; BATCH_COTACOES=50; MAX_PAGINAS=60
SESSION=requests.Session(); SESSION.headers.update(HEADERS)


def numero(v):
    if v is None: return None
    if isinstance(v,(int,float)) and not pd.isna(v):
        n=float(v); return n if math.isfinite(n) else None
    s=str(v).strip().replace('%','').replace('R$','').replace(' ','')
    if not s or s.lower() in {'nan','none','-','--','n/d'}: return None
    if ',' in s: s=s.replace('.','').replace(',','.')
    try:
        n=float(s); return n if math.isfinite(n) else None
    except (TypeError,ValueError): return None


def ticker_from(text):
    s=str(text).strip().upper()
    # Em algumas tabelas o HTML concatena ticker e nome sem espaço (ex.: AAPL34Apple Inc.).
    m=re.match(r'^([A-Z]{4}\d{1,2}|\d{2}[A-Z]{2}\d{2})(?=$|[^A-Z0-9]|[A-Z])',s)
    if m:return m.group(1)
    m=re.match(r'^([A-Z0-9]{4,6})$',s)
    return m.group(1) if m else None


def parse_catalog_table(table):
    df=table.copy(); df.columns=[str(c).strip().lower() for c in df.columns]
    out=[]
    for _,row in df.iterrows():
        vals=[str(x).strip() for x in row.tolist() if x is not None and str(x).strip() not in {'','nan','None'}]
        if not vals: continue
        ticker=None; ticker_cell=''
        for v in vals:
            t=ticker_from(v)
            if t: ticker=t; ticker_cell=v; break
        if not ticker: continue
        joined=' | '.join(vals).lower()
        cat=None
        for needle,canon in [('acoes','acao'),('ações','acao'),('fiis','fii'),('etfs','etf'),('bdrs','bdr')]:
            if needle in joined: cat=canon; break
        if not cat: continue
        nome=''
        if ticker_cell.upper()!=ticker:
            nome=re.sub(r'^'+re.escape(ticker)+r'\s*','',ticker_cell,flags=re.I).strip(' -–|')
        if not nome:
            for v in vals:
                if v!=ticker_cell and not any(x in v.lower() for x in ['acoes','ações','fiis','etfs','bdrs']):
                    if not re.fullmatch(r'[A-Z]{2,4}',v.strip()): nome=v; break
        setor=vals[-1] if len(vals)>2 else ''
        if setor.lower() in {'acoes','ações','fiis','etfs','bdrs'}: setor=''
        out.append({'t':ticker,'nome':nome or ticker,'cat':cat,'setor':setor,'fonte_catalogo':'CotaLab'})
    return out


def carregar_catalogo():
    """Busca as páginas do diretório, com fallback para o catálogo anterior válido."""
    catalogo=[]; total_pag=1
    for pagina in range(1,MAX_PAGINAS+1):
        url='https://www.cotalab.com.br/ativos' if pagina==1 else f'https://www.cotalab.com.br/ativos/pagina/{pagina}'
        r=SESSION.get(url,timeout=TIMEOUT)
        if r.status_code==404 and pagina>1: break
        r.raise_for_status()
        texto=r.text
        m=re.search(r'Página\s*\d+\s*de\s*(\d+)',texto,re.I)
        if m: total_pag=min(MAX_PAGINAS,int(m.group(1)))
        try: tabelas=pd.read_html(StringIO(texto))
        except ValueError: tabelas=[]
        candidatos=[]
        for tab in tabelas:
            cols=' '.join(str(c).lower() for c in tab.columns)
            # Seleciona tabela com colunas/tickers e categorias; evita tabelas de navegação.
            if any(k in cols for k in ['ticker','categoria','nome']) or len(tab)>=10:
                candidatos=parse_catalog_table(tab)
                if len(candidatos)>=10: break
        catalogo.extend(candidatos)
        print(f'[CATÁLOGO] página {pagina}/{total_pag}: {len(candidatos)} ativos',flush=True)
        if pagina>=total_pag: break
        time.sleep(0.2)
    unicos={}
    for a in catalogo: unicos[a['t']]=a
    resultado=sorted(unicos.values(),key=lambda a:a['t'])
    if len(resultado)<100:
        if CATALOGO.exists():
            antigo=json.loads(CATALOGO.read_text(encoding='utf-8'))
            if len(antigo.get('ativos',[]))>len(resultado):
                print('[AVISO] Catálogo atual incompleto; preservando catálogo anterior.',flush=True)
                return antigo['ativos']
        raise RuntimeError(f'Catálogo incompleto ({len(resultado)} ativos); não substituir dados existentes.')
    CATALOGO.write_text(json.dumps({'gerado':dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z'),'fonte':'CotaLab — diretório público; validar contra cadastro oficial B3','total':len(resultado),'ativos':resultado},ensure_ascii=False,allow_nan=False),encoding='utf-8')
    return resultado


def col_by(df,needles):
    for c in df.columns:
        s=str(c).lower()
        if any(n in s for n in needles): return c
    return None


def carregar_fundamentos(url,cat):
    """Lê tabelas públicas de fundamentos sem filtrar empresas por qualidade."""
    try:
        r=SESSION.get(url,timeout=TIMEOUT); r.raise_for_status()
        tabs=pd.read_html(StringIO(r.text),decimal=',',thousands='.')
        if not tabs: return {}
        df=max(tabs,key=lambda t:len(t))
        ct=col_by(df,['papel','ticker'])
        if ct is None:return {}
        mapping={
          'p': ['cota','cotação','cotacao'], 'pl':['p/l'], 'pvp':['p/vp'],
          'dy':['div. yield','div yield','dividend yield','div.'], 'ml':['mrg. l','margem líquida','margem liquida'],
          'roe':['roe'], 'liq':['liq.2','liquidez'], 'dbp':['dív/bruta','div/bruta','dívida/bruta','divida/bruta'],
          'cr':['cres. 5a','crescimento'], 'ffoy':['ffo yield'],
        }
        out={}
        for _,row in df.iterrows():
            t=str(row[ct]).strip().upper()
            if not re.fullmatch(r'[A-Z0-9]{4,6}',t): continue
            a={'t':t,'cat':cat}
            for k,needles in mapping.items():
                c=col_by(df,needles)
                if c is not None:a[k]=numero(row[c])
            if a.get('p') is not None and a['p']<=0:a['p']=None
            out[t]=a
        return out
    except Exception as e:
        print(f'[AVISO] Fundamentos {cat} indisponíveis: {e}',flush=True); return {}


def batch_precos(ativos):
    tickers=[a['t']+'.SA' for a in ativos]
    saida={}
    for ini in range(0,len(tickers),BATCH_COTACOES):
        grupo=tickers[ini:ini+BATCH_COTACOES]
        try:
            hist=yf.download(tickers=grupo,period=f'{HISTORICO_ANOS}y',interval='1d',auto_adjust=True,group_by='ticker',threads=True,progress=False,timeout=TIMEOUT)
            if hist is None or hist.empty: continue
            for a,ticker in zip(ativos[ini:ini+BATCH_COTACOES],grupo):
                try:
                    if isinstance(hist.columns,pd.MultiIndex):
                        if ticker in hist.columns.get_level_values(0): one=hist[ticker]
                        elif ticker in hist.columns.get_level_values(1): one=hist.xs(ticker,axis=1,level=1)
                        else: continue
                    else: one=hist
                    close=one['Close'].dropna() if 'Close' in one else pd.Series(dtype=float)
                    if close.empty: continue
                    first=float(close.iloc[0]); last=float(close.iloc[-1])
                    if not math.isfinite(last) or last<=0: continue
                    a['p']=round(last,6);a['desempenho_3a']=round((last/first-1)*100,2) if first>0 else None
                    one_year=close[close.index>=pd.Timestamp.now(tz=None)-pd.DateOffset(years=1)]
                    a['desempenho_1a']=round((last/float(one_year.iloc[0])-1)*100,2) if len(one_year)>1 and float(one_year.iloc[0])>0 else None
                    # Mínima observada no histórico disponível dos últimos 52 semanas.
                    # Com auto_adjust=True, preço e mínima ficam na mesma base ajustada.
                    a['lo']=round(float(one_year.min()),6) if not one_year.empty else None
                    a['periodo_min_52s_inicio']=str(one_year.index[0].date()) if not one_year.empty else None
                    a['data_cotacao']=str(close.index[-1].date())
                except Exception: continue
        except Exception as e: print(f'[AVISO] Lote de cotações {ini+1}-{min(ini+len(grupo),len(tickers))}: {e}',flush=True)
        if (ini//BATCH_COTACOES+1)%5==0: print(f'[COTAÇÕES] {min(ini+len(grupo),len(tickers))}/{len(tickers)} tickers processados',flush=True)
    return saida


def dividendos_ticker(a,agora):
    if a['cat'] not in {'acao','fii'}: return a
    try:
        s=yf.Ticker(a['t']+'.SA').dividends
        if s is None or s.empty:
            a.setdefault('dpa',None);a.setdefault('ap',0);a.setdefault('div',[]);a['dados_proventos']='sem histórico';return a
        idx=pd.DatetimeIndex(s.index);idx=idx.tz_localize(None) if idx.tz is not None else idx;s.index=idx
        inicio=agora-pd.DateOffset(years=HISTORICO_ANOS); rec=s[s.index>=inicio]
        anos=len(set(int(y) for y in rec.index.year))
        a['ap']=min(HISTORICO_ANOS,anos)
        a['dpa']=round(float(rec.sum())/anos,4) if anos else None
        a['div']=[{'v':float(v),'d':d.strftime('%Y-%m-%d')} for d,v in s.items() if math.isfinite(float(v)) and d>=agora-pd.DateOffset(months=18)]
        a['dados_proventos']='ok' if anos else 'sem histórico'
    except Exception as e:
        a.setdefault('dpa',None);a.setdefault('ap',0);a.setdefault('div',[]);a['dados_proventos']='indisponível';a['erro_proventos']=str(e)[:160]
    return a


def main():
    agora=pd.Timestamp.now().tz_localize(None)
    print('[1/5] Descobrindo catálogo multiclasse...',flush=True)
    catalogo=carregar_catalogo()
    print(f'[CATÁLOGO] {len(catalogo)} instrumentos únicos.',flush=True)
    print('[2/5] Carregando fundamentos de ações e FIIs...',flush=True)
    fund={}
    fund.update(carregar_fundamentos('https://www.fundamentus.com.br/resultado.php','acao'))
    fund.update(carregar_fundamentos('https://www.fundamentus.com.br/fii_resultado.php','fii'))
    for a in catalogo:
        f=fund.get(a['t'],{})
        # Mantém a categoria do catálogo; fundamentos ausentes ficam explicitamente nulos.
        a.update({k:v for k,v in f.items() if k not in {'t','cat'}})
        a.setdefault('p',None);a.setdefault('pl',None);a.setdefault('pvp',None);a.setdefault('dy',None)
        a.setdefault('roe',None);a.setdefault('ml',None);a.setdefault('liq',None);a.setdefault('dpa',None);a.setdefault('ap',0);a.setdefault('div',[])
        a['dados_fundamentos']='ok' if f else 'indisponível'
    print('[3/5] Obtendo histórico de preços em lotes de 50 tickers...',flush=True)
    batch_precos(catalogo)
    print('[4/5] Buscando proventos para ações e FIIs (pode demorar)...',flush=True)
    alvo=[a for a in catalogo if a['cat'] in {'acao','fii'}]
    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        futs=[pool.submit(dividendos_ticker,a,agora) for a in alvo]
        for i,f in enumerate(cf.as_completed(futs),1):
            try:f.result()
            except Exception as e: print(f'[AVISO] Proventos: {e}',flush=True)
            if i%100==0:print(f'[PROVENTOS] {i}/{len(alvo)} ativos',flush=True)
    # Preserva campos e evita que a falta de fundamentos transforme um ativo em falso sinal de compra.
    for a in catalogo:
        a['dados_completos']=bool(a.get('p') is not None and (a.get('dados_fundamentos')=='ok' or a['cat'] in {'etf','bdr'}))
    gerado=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
    doc={'gerado':gerado,'fonte':['CotaLab (catálogo secundário)','Fundamentus (fundamentos quando disponíveis)','Yahoo Finance/yfinance (cotações e proventos quando disponíveis)'],'historico_anos':HISTORICO_ANOS,'total':len(catalogo),'resumo_categorias':{c:sum(1 for a in catalogo if a['cat']==c) for c in ['acao','fii','etf','bdr']},'ativos':catalogo,'acoes':[a for a in catalogo if a['cat']=='acao']}
    temp=OUT.with_suffix('.json.tmp');temp.write_text(json.dumps(doc,ensure_ascii=False,allow_nan=False),encoding='utf-8');json.loads(temp.read_text(encoding='utf-8'));temp.replace(OUT)
    print(f'[5/5] Concluído: {len(catalogo)} ativos; resumo {doc["resumo_categorias"]}.',flush=True)

if __name__=='__main__':
    try: main()
    except Exception as e: print(f'[ERRO] Coleta interrompida; dados.json anterior preservado: {e}',flush=True); raise
