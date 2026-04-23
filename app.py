from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import time
import json

app = Flask(__name__)
CORS(app)

# ============================================================
#  CACHE SIMPLES EM MEMÓRIA
#  Guarda resultados por 5 minutos para não bater no banco
#  toda vez que alguém acessa a página
# ============================================================
_cache = {}
CACHE_TTL = 28800  # 8 horas em segundos

def cache_get(key):
    if key in _cache:
        valor, timestamp = _cache[key]
        if time.time() - timestamp < CACHE_TTL:
            return valor
    return None

def cache_set(key, valor):
    _cache[key] = (valor, time.time())

def cache_clear():
    _cache.clear()

# ============================================================
#  CONEXÃO COM SUPABASE
# ============================================================
def get_conn():
    return psycopg2.connect(
        host     = os.environ.get('DB_HOST'),
        port     = int(os.environ.get('DB_PORT', 6543)),
        database = os.environ.get('DB_NAME', 'postgres'),
        user     = os.environ.get('DB_USER'),
        password = os.environ.get('DB_PASS'),
        sslmode  = 'require',
        connect_timeout = 10,
    )

def consultar(sql, params=()):
    from datetime import date as _date, datetime as _dt
    conn   = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    resultado = cursor.fetchall()
    cursor.close()
    conn.close()
    # Converte date/datetime para string ISO — resolve data_inconsistencia sumindo ao recarregar
    def _cvt(v):
        if isinstance(v, (_dt, _date)): return v.isoformat()
        return v
    return [{k: _cvt(v) for k, v in dict(row).items()} for row in resultado]

# ============================================================
#  MONTA FILTROS
# ============================================================
def montar_filtros(args):
    condicoes = []
    params    = []

    anos = args.getlist('ano')
    if anos:
        placeholders = ','.join(['%s'] * len(anos))
        condicoes.append(f"ano IN ({placeholders})")
        params.extend([int(a) for a in anos])

    meses = args.getlist('mes')
    if meses:
        placeholders = ','.join(['%s'] * len(meses))
        condicoes.append(f"mes IN ({placeholders})")
        params.extend([int(m) for m in meses])

    unidade = args.get('unidade')
    if unidade:
        condicoes.append("unidade = %s")
        params.append(unidade)

    uf = args.get('uf')
    if uf:
        condicoes.append("uf = %s")
        params.append(uf)

    tipo = args.get('tipo')
    if tipo:
        condicoes.append("tipo_operacao = %s")
        params.append(tipo)

    marca = args.get('marca')
    if marca:
        condicoes.append("marca = %s")
        params.append(marca)

    # Filtra por nome do vendedor — agrupa todos os códigos automaticamente
    vendedores = args.getlist('vendedor')
    if vendedores:
        placeholders = ','.join(['%s'] * len(vendedores))
        condicoes.append(f"vendedor IN ({placeholders})")
        params.extend(vendedores)

    where = ("WHERE " + " AND ".join(condicoes)) if condicoes else ""
    return where, params

def cache_key(rota, args):
    return rota + '?' + '&'.join(f'{k}={v}' for k, v in sorted(args.items()))

# ============================================================
#  ROTAS
# ============================================================
@app.route('/')
def home():
    return jsonify({"status": "online", "mensagem": "API Horus funcionando!"})

@app.route('/api/filtros')
def filtros():
    key = 'filtros'
    cached = cache_get(key)
    if cached: return jsonify(cached)

    anos       = consultar("SELECT DISTINCT ano FROM faturamento WHERE ano IS NOT NULL AND ano > 0 ORDER BY ano DESC")
    meses      = consultar("SELECT DISTINCT mes FROM faturamento WHERE mes IS NOT NULL AND mes > 0 ORDER BY mes")
    unidades   = consultar("SELECT DISTINCT unidade FROM faturamento WHERE unidade IS NOT NULL ORDER BY unidade")
    ufs        = consultar("SELECT DISTINCT uf FROM faturamento WHERE uf IS NOT NULL AND uf != '' ORDER BY uf")
    marcas     = consultar("SELECT DISTINCT marca FROM faturamento WHERE marca IS NOT NULL ORDER BY marca")
    tipos      = consultar("SELECT DISTINCT tipo_operacao FROM faturamento WHERE tipo_operacao IS NOT NULL ORDER BY tipo_operacao")
    vendedores = consultar("""
        SELECT DISTINCT vendedor
        FROM faturamento
        WHERE vendedor IS NOT NULL
        ORDER BY vendedor
    """)

    resultado = {
        "anos":       [r['ano'] for r in anos],
        "meses":      [r['mes'] for r in meses],
        "unidades":   [r['unidade'] for r in unidades],
        "ufs":        [r['uf'] for r in ufs],
        "marcas":     [r['marca'] for r in marcas],
        "tipos":      [r['tipo_operacao'] for r in tipos],
        "vendedores": [r['vendedor'] for r in vendedores],
    }
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/kpis')
def kpis():
    key = cache_key('kpis', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    resultado = consultar(f"""
        SELECT
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS bonificacoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda' THEN valor_nf ELSE 0 END) /
                  NULLIF(COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END), 0) AS NUMERIC), 2) AS ticket_medio,
            COUNT(DISTINCT cliente)                              AS total_clientes,
            COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END) AS qtd_vendas
        FROM faturamento {where}
    """, params)
    r = resultado[0] if resultado else {}
    cache_set(key, r)
    return jsonify(r)

@app.route('/api/faturamento-mensal')
def faturamento_mensal():
    key = cache_key('faturamento-mensal', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    resultado = consultar(f"""
        SELECT ano, mes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS bonificacoes
        FROM faturamento {where}
        {'AND' if where else 'WHERE'} mes > 0
        GROUP BY ano, mes ORDER BY ano, mes
    """, params)
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/top-vendedores')
def top_vendedores():
    key = cache_key('top-vendedores', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT vendedor,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente)                   AS clientes,
            COUNT(DISTINCT unidade)                   AS unidades,
            COUNT(*)                                  AS qtd_vendas
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
        GROUP BY vendedor ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/faturamento-por-marca')
def faturamento_por_marca():
    key = cache_key('faturamento-por-marca', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 15))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT marca,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente)                   AS clientes
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
        GROUP BY marca ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/faturamento-por-regiao')
def faturamento_por_regiao():
    key = cache_key('faturamento-por-regiao', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT regiao,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente)                   AS clientes
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
          AND regiao IS NOT NULL AND regiao != ''
        GROUP BY regiao ORDER BY faturamento DESC
    """, params)
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/faturamento-por-unidade')
def faturamento_por_unidade():
    key = cache_key('faturamento-por-unidade', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT unidade,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS bonificacoes,
            COUNT(DISTINCT cliente) AS clientes
        FROM faturamento {where} {and_or} unidade IS NOT NULL
        GROUP BY unidade ORDER BY faturamento DESC
    """, params)
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/top-produtos')
def top_produtos():
    key = cache_key('top-produtos', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT produto, marca,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2)  AS faturamento,
            ROUND(CAST(SUM(quantidade) AS NUMERIC), 0) AS quantidade
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
        GROUP BY produto, marca ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/faturamento-por-uf')
def faturamento_por_uf():
    key = cache_key('faturamento-por-uf', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT uf,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente)                   AS clientes
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
          AND uf IS NOT NULL AND uf != ''
        GROUP BY uf ORDER BY faturamento DESC
    """, params)
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/carteira-vendedor')
def carteira_vendedor():
    key = cache_key('carteira-vendedor', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    cod_vendedor = request.args.get('cod_vendedor', '')
    if cod_vendedor:
        resultado = consultar("""
            SELECT * FROM carteira WHERE cod_vendedor = %s ORDER BY cliente
        """, [cod_vendedor])
    else:
        resultado = consultar("""
            SELECT cod_vendedor, COUNT(*) as total_clientes
            FROM carteira GROUP BY cod_vendedor ORDER BY total_clientes DESC
        """)
    cache_set(key, resultado)
    return jsonify(resultado)

@app.route('/api/resumo-carteira')
def resumo_carteira():
    """
    Retorna total de clientes em carteira e margem média.
    Se filtrar por vendedor (nome), busca o cod_vendedor correspondente
    e retorna a carteira daquele vendedor.
    """
    key = cache_key('resumo-carteira', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    vendedor = request.args.get('vendedor', '')
    where, params = montar_filtros(request.args)
    and_or = 'AND' if where else 'WHERE'

    # Margem média do período filtrado
    margem = consultar(f"""
        SELECT ROUND(CAST(AVG(margem) AS NUMERIC), 2) AS margem_media
        FROM faturamento {where}
        {and_or} tipo_operacao = 'Venda'
        AND margem IS NOT NULL AND margem != 0
    """, params)

    margem_media = margem[0]['margem_media'] if margem else 0

    # Total em carteira
    if vendedor:
        # Busca cod_vendedor pelo nome do vendedor no faturamento
        cods = consultar("""
            SELECT DISTINCT cod_vendedor FROM faturamento
            WHERE vendedor = %s AND cod_vendedor IS NOT NULL AND cod_vendedor != ''
        """, [vendedor])

        if cods:
            cod_list = [c['cod_vendedor'] for c in cods]
            placeholders = ','.join(['%s'] * len(cod_list))
            carteira = consultar(f"""
                SELECT COUNT(*) as total FROM carteira
                WHERE cod_vendedor IN ({placeholders})
            """, cod_list)
        else:
            carteira = [{'total': 0}]
    else:
        carteira = consultar("SELECT COUNT(*) as total FROM carteira")

    total_carteira = carteira[0]['total'] if carteira else 0

    resultado = {
        'total_carteira': total_carteira,
        'margem_media':   float(margem_media) if margem_media else 0,
    }
    cache_set(key, resultado)
    return jsonify(resultado)


@app.route('/api/top-clientes')
def top_clientes():
    key = cache_key('top-clientes', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)

    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT cliente,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(*) AS qtd_vendas
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
        GROUP BY cliente ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)


@app.route('/api/faturamento-por-cidade')
def faturamento_por_cidade():
    key = cache_key('faturamento-por-cidade', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)
    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 15))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT cidade, uf,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente) AS clientes
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
          AND cidade IS NOT NULL AND cidade != ''
        GROUP BY cidade, uf ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)


@app.route('/api/buscar-produtos')
def buscar_produtos():
    termo = request.args.get('q', '').strip()
    if not termo or len(termo) < 2:
        return jsonify([])
    resultado = consultar("""
        SELECT DISTINCT produto, cod_produto, marca
        FROM faturamento
        WHERE (LOWER(produto) LIKE LOWER(%s) OR LOWER(cod_produto) LIKE LOWER(%s))
          AND produto IS NOT NULL AND produto != ''
        ORDER BY produto
        LIMIT 30
    """, [f'%{termo}%', f'%{termo}%'])
    return jsonify(resultado)

@app.route('/api/top-produtos-filtrado')
def top_produtos_filtrado():
    key = cache_key('top-produtos-filtrado', dict(request.args))
    cached = cache_get(key)
    if cached: return jsonify(cached)
    where, params = montar_filtros(request.args)
    produtos = request.args.getlist('produtos')
    and_or = 'AND' if where else 'WHERE'
    if produtos:
        placeholders = ','.join(['%s'] * len(produtos))
        resultado = consultar(f"""
            SELECT produto, marca,
                ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2)  AS faturamento,
                ROUND(CAST(SUM(quantidade) AS NUMERIC), 0) AS quantidade,
                COUNT(DISTINCT cliente) AS clientes
            FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
              AND produto IN ({placeholders})
            GROUP BY produto, marca ORDER BY faturamento DESC
        """, params + produtos)
    else:
        limite = int(request.args.get('limite', 20))
        resultado = consultar(f"""
            SELECT produto, marca,
                ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2)  AS faturamento,
                ROUND(CAST(SUM(quantidade) AS NUMERIC), 0) AS quantidade,
                COUNT(DISTINCT cliente) AS clientes
            FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
            GROUP BY produto, marca ORDER BY faturamento DESC LIMIT %s
        """, params + [limite])
    cache_set(key, resultado)
    return jsonify(resultado)


@app.route('/api/vendedores-por-produto')
def vendedores_por_produto():
    produtos = request.args.getlist('produtos')
    if not produtos:
        return jsonify([])
    placeholders = ','.join(['%s'] * len(produtos))
    resultado = consultar(f"""
        SELECT DISTINCT vendedor
        FROM faturamento
        WHERE produto IN ({placeholders})
          AND vendedor IS NOT NULL AND vendedor != ''
          AND tipo_operacao = 'Venda'
        ORDER BY vendedor
    """, produtos)
    return jsonify([r['vendedor'] for r in resultado])


# ============================================================
#  SHELF LIFE
# ============================================================
from datetime import date as _date

@app.route('/api/shelflife/upload', methods=['POST'])
def shelflife_upload():
    data     = request.get_json()
    semana   = data.get('semana')
    unidade  = data.get('unidade')
    produtos = data.get('produtos', [])
    if not produtos:
        return jsonify({'erro': 'Nenhum produto enviado'}), 400

    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM shelflife WHERE semana = %s AND unidade = %s", [semana, unidade])

    hoje = _date.today()
    inseridos = 0
    for p in produtos:
        validade = p.get('validade','')
        try:
            from datetime import datetime
            val_date = datetime.strptime(str(validade)[:10], '%Y-%m-%d').date()
            dias = (val_date - hoje).days
        except:
            dias = 999

        if dias <= 30:   status = 'CRITICO'
        elif dias <= 60: status = 'ATENCAO'
        else:            status = 'OK'

        nome  = str(p.get('produto',''))
        is_sl = nome.upper().startswith('SL') or nome.upper().startswith('SL.')

        cursor.execute("""
            INSERT INTO shelflife (
                semana, unidade, cod_produto, cod_sl, produto, marca,
                quantidade_log, validade, dias_vencimento,
                vence_em, status_logistica, status, is_sl
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            semana, unidade,
            p.get('cod_produto'), p.get('cod_sl'),
            nome, p.get('marca'),
            p.get('quantidade'), validade[:10] if validade and len(str(validade))>=10 else None,
            dias, p.get('vence_em'), p.get('status_logistica'),
            status, is_sl
        ])
        inseridos += 1

    conn.commit(); cursor.close(); conn.close()
    return jsonify({'inseridos': inseridos, 'semana': semana, 'unidade': unidade})

@app.route('/api/shelflife/listar')
def shelflife_listar():
    semana  = request.args.get('semana')
    unidade = request.args.get('unidade')
    status  = request.args.get('status')
    where = []; params = []
    if semana:
        # Normaliza: aceita tanto "2026-04-20" quanto "Mon, 20 Apr 2026 00:00:00 GMT"
        where.append("semana::date = %s::date"); params.append(semana[:10] if len(str(semana)) >= 10 else semana)
    else:
        where.append("semana = (SELECT MAX(semana) FROM shelflife)")
    if unidade:
        where.append("unidade = %s"); params.append(unidade)
    if status == 'SL':
        where.append("is_sl = TRUE")
    elif status:
        where.append("status = %s AND is_sl = FALSE"); params.append(status)
    where_str = "WHERE " + " AND ".join(where) if where else ""
    resultado = consultar(f"SELECT * FROM shelflife {where_str} ORDER BY dias_vencimento ASC", params)
    return jsonify(resultado)

@app.route('/api/shelflife/semanas')
def shelflife_semanas():
    resultado = consultar("""
        SELECT DISTINCT TO_CHAR(semana, 'YYYY-MM-DD') as semana, unidade, COUNT(*) as total
        FROM shelflife GROUP BY semana, unidade ORDER BY semana DESC
    """)
    return jsonify(resultado)

@app.route('/api/shelflife/atualizar', methods=['POST'])
def shelflife_atualizar():
    data    = request.get_json()
    id_prod = data.get('id')
    conn    = get_conn()
    cursor  = conn.cursor()

    # Busca valores ANTERIORES para comparar
    cursor.execute("""
        SELECT semana, unidade, cod_produto, produto, quantidade_log,
               quantidade_atual, venda_3meses, venda_mes,
               data_inconsistencia, obs_logistica, obs_gerais, acao, vendedor
        FROM shelflife WHERE id = %s
    """, [id_prod])
    row = cursor.fetchone()

    # Atualiza o registro
    cursor.execute("""
        UPDATE shelflife SET
            quantidade_atual    = %s,
            venda_3meses        = %s,
            venda_mes           = %s,
            data_inconsistencia = %s,
            obs_logistica       = %s,
            obs_gerais          = %s,
            acao                = %s,
            vendedor            = %s,
            resolvido           = %s,
            updated_at          = NOW()
        WHERE id = %s
    """, [
        data.get('quantidade_atual'),
        data.get('venda_3meses'),
        data.get('venda_mes'),
        data.get('data_inconsistencia') or None,
        data.get('obs_logistica'),
        data.get('obs_gerais'),
        data.get('acao'),
        data.get('vendedor'),
        data.get('resolvido', False),
        id_prod
    ])

    # Gera log de alteracoes comparando anterior vs novo
    if row:
        campos = [
            ('quantidade_atual',    row[5],  data.get('quantidade_atual'),    'Qtde Atual'),
            ('venda_3meses',        row[6],  data.get('venda_3meses'),        'Venda 3 Meses'),
            ('venda_mes',           row[7],  data.get('venda_mes'),           'Venda Mensal'),
            ('data_inconsistencia', str(row[8]) if row[8] else '', data.get('data_inconsistencia') or '', 'Data Inconsistencia'),
            ('obs_logistica',       row[9],  data.get('obs_logistica'),       'Obs. Logistica'),
            ('obs_gerais',          row[10], data.get('obs_gerais'),          'Obs. Gerais'),
            ('acao',                row[11], data.get('acao'),                'Acao'),
            ('vendedor',            row[12], data.get('vendedor'),            'Vendedor'),
        ]

        alteracoes = []
        for campo, antes, depois, label in campos:
            antes_str  = str(antes or '').strip()
            depois_str = str(depois or '').strip()
            if antes_str != depois_str:
                alteracoes.append(f"{label}: [{antes_str or '—'}] → [{depois_str or '—'}]")

        # Registra no historico se houve qualquer alteracao
        if alteracoes:
            cursor.execute("""
                INSERT INTO shelflife_historico (
                    shelflife_id, semana, unidade, cod_produto, produto,
                    quantidade_log, quantidade_atual, venda_3meses, venda_mes,
                    acao, vendedor, obs_logistica, obs_gerais,
                    data_inconsistencia, usuario
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [
                id_prod, row[0], row[1], row[2], row[3],
                row[4],
                data.get('quantidade_atual'),
                data.get('venda_3meses'),
                data.get('venda_mes'),
                data.get('acao'),
                data.get('vendedor'),
                '|'.join(alteracoes),  # Salva as alteracoes em obs_logistica
                data.get('obs_gerais'),
                data.get('data_inconsistencia') or None,
                data.get('usuario', 'admin')
            ])

    conn.commit(); cursor.close(); conn.close()
    return jsonify({'ok': True, 'alteracoes': alteracoes if row else []})


@app.route('/api/shelflife/excluir', methods=['POST'])
def shelflife_excluir():
    data    = request.get_json()
    semana  = data.get('semana')
    unidade = data.get('unidade')
    conn    = get_conn()
    cursor  = conn.cursor()
    if unidade:
        cursor.execute("DELETE FROM shelflife WHERE semana = %s AND unidade = %s", [semana, unidade])
    else:
        cursor.execute("DELETE FROM shelflife WHERE semana = %s", [semana])
    deleted = cursor.rowcount
    conn.commit(); cursor.close(); conn.close()
    return jsonify({'excluidos': deleted})

@app.route('/api/pivot-clientes')
def pivot_clientes_novo():
    """
    Tabela pivot de clientes:
    - Sem produto: todos da carteira (LEFT JOIN com faturamento)
    - Com produto: só quem comprou (INNER JOIN)
    """
    vendedores = request.args.getlist('vendedor')
    produtos   = request.args.getlist('produtos')
    anos       = request.args.getlist('ano')

    conn   = get_conn()
    cursor = conn.cursor()

    if produtos:
        # COM produto — só quem comprou
        prod_ph = ','.join(['%s'] * len(produtos))
        vend_filter = ''
        params = list(produtos)

        if vendedores:
            vend_ph = ','.join(['%s'] * len(vendedores))
            vend_filter = f'AND f.vendedor IN ({vend_ph})'
            params += vendedores

        ano_filter = ''
        if anos:
            ano_ph = ','.join(['%s'] * len(anos))
            ano_filter = f'AND f.ano IN ({ano_ph})'
            params += [int(a) for a in anos]

        cursor.execute(f"""
            SELECT
                f.cliente, f.cod_cliente, f.vendedor,
                f.ano, f.mes,
                ROUND(SUM(CASE WHEN f.tipo_operacao='Venda' THEN f.valor_nf ELSE 0 END)::NUMERIC,2) AS faturamento,
                ROUND(SUM(CASE WHEN f.tipo_operacao='Devolucao' THEN f.valor_nf ELSE 0 END)::NUMERIC,2) AS devolucoes
            FROM faturamento f
            WHERE f.produto IN ({prod_ph})
              AND f.tipo_operacao IN ('Venda','Devolucao')
              AND f.mes > 0
              {vend_filter}
              {ano_filter}
            GROUP BY f.cliente, f.cod_cliente, f.vendedor, f.ano, f.mes
            ORDER BY f.cliente, f.ano, f.mes
        """, params)

    else:
        # SEM produto — todos da carteira + compras via LEFT JOIN
        # Filtra por NOME do vendedor via o JOIN com faturamento (carteira tem cod_vendedor, nao nome)
        vend_filter = ''
        params = []

        if vendedores:
            vend_ph = ','.join(['%s'] * len(vendedores))
            # Busca os cod_vendedor correspondentes aos nomes selecionados
            vend_filter = f'''WHERE c.cod_vendedor IN (
                SELECT DISTINCT cod_vendedor FROM faturamento
                WHERE vendedor IN ({vend_ph}) AND cod_vendedor IS NOT NULL AND cod_vendedor != ''
            )'''
            params += vendedores

        ano_filter = ''
        ano_params = []
        if anos:
            ano_ph = ','.join(['%s'] * len(anos))
            ano_filter = f'AND f.ano IN ({ano_ph})'
            ano_params = [int(a) for a in anos]

        cursor.execute(f"""
            SELECT
                c.cliente, c.cod_cliente,
                v.vendedor,
                f.ano, f.mes,
                ROUND(COALESCE(SUM(CASE WHEN f.tipo_operacao='Venda' THEN f.valor_nf ELSE 0 END),0)::NUMERIC,2) AS faturamento,
                ROUND(COALESCE(SUM(CASE WHEN f.tipo_operacao='Devolucao' THEN f.valor_nf ELSE 0 END),0)::NUMERIC,2) AS devolucoes
            FROM carteira c
            LEFT JOIN (
                SELECT DISTINCT cod_vendedor, vendedor FROM faturamento
            ) v ON v.cod_vendedor = c.cod_vendedor
            LEFT JOIN faturamento f
                ON f.cod_cliente = c.cod_cliente
                AND f.tipo_operacao IN ('Venda','Devolucao')
                AND f.mes > 0
                {ano_filter}
            {vend_filter}
            GROUP BY c.cliente, c.cod_cliente, v.vendedor, f.ano, f.mes
            ORDER BY c.cliente, f.ano, f.mes
        """, params + ano_params)

    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    resultado = [dict(zip(cols, row)) for row in rows]
    cursor.close()
    conn.close()
    return jsonify(resultado)

@app.route('/api/shelflife/historico')
def shelflife_historico():
    shelflife_id = request.args.get('shelflife_id')
    cod_produto  = request.args.get('cod_produto')
    where = []; params = []
    if shelflife_id:
        where.append("shelflife_id = %s"); params.append(shelflife_id)
    if cod_produto:
        where.append("cod_produto = %s"); params.append(cod_produto)
    where_str = "WHERE " + " AND ".join(where) if where else ""
    resultado = consultar(f"""
        SELECT * FROM shelflife_historico {where_str}
        ORDER BY created_at DESC LIMIT 50
    """, params)
    return jsonify(resultado)

# Limpa cache (útil após atualizar dados)
@app.route('/api/cache/clear', methods=['POST'])
def limpar_cache():
    cache_clear()
    return jsonify({"status": "cache limpo!"})

# Ping — mantém o servidor acordado
@app.route('/ping')
def ping():
    return jsonify({"status": "pong", "uptime": "ok"})

if __name__ == '__main__':
    print("🚀 API Horus iniciando...")
    app.run(debug=False, host='0.0.0.0', port=5000)
