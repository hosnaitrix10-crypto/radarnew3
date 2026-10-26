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
    """Extrai ticker do início da célula, inclusive quando o nome vem colado.

    O CotaLab apresenta tickers como AALR3, A1AP34, 03BK11 e 3PRE11
    concatenados diretamente com o nome da empresa. Não exigir espaço/separador.
    """
    s=str(text).strip().upper()
    # Formatos comuns B3: letras + dígitos, mistos com prefixo numérico,
    # e tickers iniciados por letra-dígito (ex.: A1AP34). A ordem importa.
    patterns=(
        r'^([A-Z]{4}\d{1,2})',       # PETR4, ISAE4, AALR3, ABCB10
        r'^([A-Z]\d[A-Z]{2}\d{2})', # A1AP34, B1AM34
        r'^(\d{2}[A-Z]{2}\d{2})',  # 03BK11
        r'^([A-Z]{3}\d{2})',         # tickers especiais de 5 caracteres
        r'^(\d[A-Z]{3}\d{2})',    # 3PRE11
        r'^([A-Z]{2,4}\d{2})',       # outros formatos com sufixo numérico
    )
    for pattern in patterns:
        m=re.match(pattern,s)
        if m:
            return m.group(1)
    # Em fontes que trazem o ticker numa célula separada.
    if re.fullmatch(r'[A-Z0-9]{4,6}',s):
        return s
    return None


def parse_catalog_table(table):
    df=table.copy()
    df.columns=[str(c).strip().lower() for c in df.columns]
    out=[]
    # Identifica colunas pela estrutura real da tabela; as células de ticker e nome
    # podem estar concatenadas (ex.: A1AP34Apple Inc.).
    ticker_col=next((c for c in df.columns if 'ticker' in c or 'papel' in c),df.columns[0] if len(df.columns) else None)
    name_col=next((c for c in df.columns if 'nome' in c or 'empresa' in c),None)
    cat_col=next((c for c in df.columns if 'categoria' in c or 'tipo' in c),None)
    sector_col=next((c for c in df.columns if 'setor' in c),None)
    category_map={
        'acao':'acao','acoes':'acao','ação':'acao','ações':'acao',
        'fii':'fii','fiis':'fii','fundo imobiliario':'fii','fundos imobiliarios':'fii',
        'etf':'etf','etfs':'etf','fundo de indice':'etf','fundos de indice':'etf',
        'bdr':'bdr','bdrs':'bdr','brazilian depositary receipt':'bdr',
    }
    def category_for(value):
        v=re.sub(r'\s+',' ',str(value or '').strip().lower())
        if v in category_map: return category_map[v]
        # Aceita rótulos mais longos, sem confundir setor/nome com categoria.
        for label,cat in [('acoes','acao'),('ações','acao'),('ação','acao'),('acao','acao'),
                          ('fiis','fii'),('fii','fii'),('etfs','etf'),('etf','etf'),
                          ('bdrs','bdr'),('bdr','bdr')]:
            if v==label or v.startswith(label+' '): return cat
        return None
    for _,row in df.iterrows():
        vals=[str(x).strip() for x in row.tolist() if x is not None and str(x).strip() not in {'','nan','None'}]
        if not vals: continue
        ticker_cell=str(row[ticker_col]).strip() if ticker_col is not None and pd.notna(row[ticker_col]) else ''
        ticker=ticker_from(ticker_cell)
        # Não procure ticker nas demais células: isso pode interpretar rótulos
        # como 'acoes'/'etfs' como se fossem símbolos quando a célula principal
        # tiver formato inesperado. Falha fechada e deixa a validação sinalizar.
        if not ticker: continue
        cat=category_for(row[cat_col]) if cat_col is not None else None
        if not cat:
            # Fallback para HTML sem cabeçalhos consistentes: só categorias exatas.
            for v in vals:
                cat=category_for(v)
                if cat: break
        if not cat: continue
        nome=''
        if name_col is not None and pd.notna(row[name_col]):
            nome=str(row[name_col]).strip()
        if not nome and ticker_cell.upper()!=ticker:
            nome=re.sub(r'^'+re.escape(ticker)+r'\s*','',ticker_cell,flags=re.I).strip(' -–|')
        if not nome:
            for v in vals:
                if v!=ticker_cell and not category_for(v) and not re.fullmatch(r'[A-Z]{2,4}',v.strip()):
                    nome=v; break
        setor=str(row[sector_col]).strip() if sector_col is not None and pd.notna(row[sector_col]) else ''
        if setor in {'—','-','nan','None'}: setor=''
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
        # A paginação pode estar no rodapé textual, nos links ou no resumo
        # 'Mostrando 1–80 de 1941 ativos'. Tenta todas as formas conhecidas;
        # sem isso o coletor acha só a primeira página (80 linhas) e aborta.
        paginas_encontradas=[]
        m=re.search(r'Página\s*\d+\s*de\s*(\d+)',texto,re.I)
        if m: paginas_encontradas.append(int(m.group(1)))
        paginas_encontradas += [int(x) for x in re.findall(r'/ativos/pagina/(\d+)',texto,re.I)]
        m_total=re.search(r'Mostrando\s+\d+\s*[–-]\s*\d+\s+de\s+([\d.,]+)\s+ativos',texto,re.I)
        if m_total:
            total_ativos=int(re.sub(r'[^0-9]','',m_total.group(1)))
            paginas_encontradas.append(math.ceil(total_ativos/80))
        if paginas_encontradas:
            total_pag=min(MAX_PAGINAS,max(total_pag,max(paginas_encontradas)))
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
        unicos_pagina={a['t'] for a in candidatos}
        print(f'[CATÁLOGO] página {pagina}/{total_pag}: {len(candidatos)} linhas candidatas; '
              f'{len(unicos_pagina)} tickers extraídos',flush=True)
        if pagina>=total_pag: break
        time.sleep(0.2)
    unicos={}
    for a in catalogo: unicos[a['t']]=a
    resultado=sorted(unicos.values(),key=lambda a:a['t'])
    # A fonte pode mudar a paginação/estrutura e devolver uma página com muitos
    # registros, mas poucos tickers únicos. Não trate esse resultado parcial como
    # catálogo novo. Primeiro, tenta os snapshots locais conhecidos.
    if len(resultado)<100:
        candidatos_antigos=[]
        if CATALOGO.exists():
            try:
                antigo=json.loads(CATALOGO.read_text(encoding='utf-8'))
                candidatos_antigos=antigo.get('ativos',[]) if isinstance(antigo,dict) else []
            except (OSError,ValueError,TypeError):
                candidatos_antigos=[]
        # Se o snapshot do catálogo não existir (comum em checkout novo), usa
        # o universo já publicado em dados.json como fallback, sem sobrescrevê-lo
        # nem fingir que a descoberta atual foi completa.
        if not candidatos_antigos and OUT.exists():
            try:
                anterior=json.loads(OUT.read_text(encoding='utf-8'))
                ativos_anteriores=anterior.get('ativos',[]) if isinstance(anterior,dict) else []
                if not ativos_anteriores and isinstance(anterior,dict):
                    ativos_anteriores=anterior.get('acoes',[])
                candidatos_antigos=ativos_anteriores
            except (OSError,ValueError,TypeError):
                candidatos_antigos=[]
        if len(candidatos_antigos)>len(resultado):
            print(f'[AVISO] Catálogo atual incompleto ({len(resultado)} tickers únicos); '
                  f'preservando catálogo anterior ({len(candidatos_antigos)} ativos).',flush=True)
            return candidatos_antigos
        raise RuntimeError(f'Catálogo incompleto ({len(resultado)} ativos únicos; '
                           f'{len(catalogo)} linhas identificadas); catálogo anterior não disponível ou não é maior. '
                           'Não substituir dados existentes.')
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
