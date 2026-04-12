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
    conn   = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    resultado = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(row) for row in resultado]

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

    vendedor = args.get('vendedor')
    if vendedor:
        condicoes.append("vendedor = %s")
        params.append(vendedor)

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
    vendedores = consultar("SELECT DISTINCT vendedor FROM faturamento WHERE vendedor IS NOT NULL ORDER BY vendedor")

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


@app.route('/api/pivot-clientes')
def pivot_clientes():
    """
    Retorna dados para tabela pivot:
    Linhas = Clientes
    Colunas = Mês/Ano
    Valores = Faturamento
    """
    where, params = montar_filtros(request.args)
    produtos = request.args.getlist('produtos')
    and_or = 'AND' if where else 'WHERE'

    extra = ''
    extra_params = []
    if produtos:
        placeholders = ','.join(['%s'] * len(produtos))
        extra = f'{and_or} produto IN ({placeholders})'
        extra_params = produtos
        and_or2 = 'AND'
    else:
        and_or2 = and_or

    resultado = consultar(f"""
        SELECT
            cliente,
            cod_cliente,
            vendedor,
            ano,
            mes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolução' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes
        FROM faturamento {where} {extra}
        {and_or2} tipo_operacao IN ('Venda', 'Devolução')
        AND mes > 0
        GROUP BY cliente, cod_cliente, vendedor, ano, mes
        ORDER BY cliente, ano, mes
    """, params + extra_params)
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
        where.append("semana = %s"); params.append(semana)
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
        SELECT DISTINCT semana, unidade, COUNT(*) as total
        FROM shelflife GROUP BY semana, unidade ORDER BY semana DESC
    """)
    return jsonify(resultado)

@app.route('/api/shelflife/atualizar', methods=['POST'])
def shelflife_atualizar():
    data    = request.get_json()
    id_prod = data.get('id')
    conn    = get_conn()
    cursor  = conn.cursor()
    cursor.execute("""
        UPDATE shelflife SET
            quantidade_atual    = %s,
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
        data.get('data_inconsistencia') or None,
        data.get('obs_logistica'),
        data.get('obs_gerais'),
        data.get('acao'),
        data.get('vendedor'),
        data.get('resolvido', False),
        id_prod
    ])
    conn.commit(); cursor.close(); conn.close()
    return jsonify({'ok': True})

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
