from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os

app = Flask(__name__)
CORS(app)

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
        sslmode  = 'require'
    )

def consultar(sql, params=()):
    conn   = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    resultado = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(row) for row in resultado]

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

@app.route('/')
def home():
    return jsonify({"status": "online", "mensagem": "API Horus funcionando!"})

@app.route('/debug')
def debug():
    return jsonify({
        "DB_HOST": os.environ.get('DB_HOST', 'NAO ENCONTRADO'),
        "DB_PORT": os.environ.get('DB_PORT', 'NAO ENCONTRADO'),
        "DB_NAME": os.environ.get('DB_NAME', 'NAO ENCONTRADO'),
        "DB_USER": os.environ.get('DB_USER', 'NAO ENCONTRADO'),
        "DB_PASS": "****" if os.environ.get('DB_PASS') else 'NAO ENCONTRADO',
    })

@app.route('/api/filtros')
def filtros():
    anos       = consultar("SELECT DISTINCT ano FROM faturamento WHERE ano IS NOT NULL ORDER BY ano DESC")
    meses      = consultar("SELECT DISTINCT mes FROM faturamento WHERE mes IS NOT NULL ORDER BY mes")
    unidades   = consultar("SELECT DISTINCT unidade FROM faturamento WHERE unidade IS NOT NULL ORDER BY unidade")
    ufs        = consultar("SELECT DISTINCT uf FROM faturamento WHERE uf IS NOT NULL AND uf != '' ORDER BY uf")
    marcas     = consultar("SELECT DISTINCT marca FROM faturamento WHERE marca IS NOT NULL ORDER BY marca")
    tipos      = consultar("SELECT DISTINCT tipo_operacao FROM faturamento WHERE tipo_operacao IS NOT NULL ORDER BY tipo_operacao")
    vendedores = consultar("SELECT DISTINCT vendedor FROM faturamento WHERE vendedor IS NOT NULL ORDER BY vendedor")
    return jsonify({
        "anos":       [r['ano'] for r in anos],
        "meses":      [r['mes'] for r in meses],
        "unidades":   [r['unidade'] for r in unidades],
        "ufs":        [r['uf'] for r in ufs],
        "marcas":     [r['marca'] for r in marcas],
        "tipos":      [r['tipo_operacao'] for r in tipos],
        "vendedores": [r['vendedor'] for r in vendedores],
    })

@app.route('/api/kpis')
def kpis():
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
    return jsonify(resultado[0] if resultado else {})

@app.route('/api/faturamento-mensal')
def faturamento_mensal():
    where, params = montar_filtros(request.args)
    resultado = consultar(f"""
        SELECT ano, mes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS bonificacoes
        FROM faturamento {where}
        GROUP BY ano, mes ORDER BY ano, mes
    """, params)
    return jsonify(resultado)

@app.route('/api/top-vendedores')
def top_vendedores():
    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))
    and_or = 'AND' if where else 'WHERE'
    resultado = consultar(f"""
        SELECT vendedor,
            ROUND(CAST(SUM(valor_nf) AS NUMERIC), 2) AS faturamento,
            COUNT(DISTINCT cliente)                   AS clientes,
            COUNT(*)                                  AS qtd_vendas
        FROM faturamento {where} {and_or} tipo_operacao = 'Venda'
        GROUP BY vendedor ORDER BY faturamento DESC LIMIT %s
    """, params + [limite])
    return jsonify(resultado)

@app.route('/api/faturamento-por-marca')
def faturamento_por_marca():
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
    return jsonify(resultado)

@app.route('/api/faturamento-por-regiao')
def faturamento_por_regiao():
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
    return jsonify(resultado)

@app.route('/api/faturamento-por-unidade')
def faturamento_por_unidade():
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
    return jsonify(resultado)

@app.route('/api/top-produtos')
def top_produtos():
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
    return jsonify(resultado)

@app.route('/api/faturamento-por-uf')
def faturamento_por_uf():
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
    return jsonify(resultado)

if __name__ == '__main__':
    print("🚀 API Horus iniciando...")
    app.run(debug=True, host='0.0.0.0', port=5000)
