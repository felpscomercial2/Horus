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
