from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

# ============================================================
#  CONFIGURAÇÕES
# ============================================================
app = Flask(__name__)
CORS(app)  # Permite o frontend (Vercel) acessar a API

BANCO_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'faturamento_real.db')


# ============================================================
#  FUNÇÃO AUXILIAR — conecta ao banco e executa uma consulta
#  Retorna uma lista de dicionários (fácil de converter em JSON)
# ============================================================
def consultar(sql, params=()):
    conn = sqlite3.connect(BANCO_DADOS)
    conn.row_factory = sqlite3.Row  # Permite acessar colunas pelo nome
    cursor = conn.cursor()
    cursor.execute(sql, params)
    resultado = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return resultado


# ============================================================
#  FUNÇÃO AUXILIAR — monta o WHERE dinamicamente com filtros
#  Filtros aceitos via query string:
#    ?ano=2026&mes=1&unidade=Reforpan&uf=PR&tipo=Venda
# ============================================================
def montar_filtros(args):
    condicoes = []
    params    = []

    # Filtro de ANO — pode ser múltiplo: ?ano=2025&ano=2026
    anos = args.getlist('ano')
    if anos:
        placeholders = ','.join(['?' for _ in anos])
        condicoes.append(f"ano IN ({placeholders})")
        params.extend([int(a) for a in anos])

    # Filtro de MÊS — pode ser múltiplo: ?mes=1&mes=2
    meses = args.getlist('mes')
    if meses:
        placeholders = ','.join(['?' for _ in meses])
        condicoes.append(f"mes IN ({placeholders})")
        params.extend([int(m) for m in meses])

    # Filtro de UNIDADE
    unidade = args.get('unidade')
    if unidade:
        condicoes.append("unidade = ?")
        params.append(unidade)

    # Filtro de UF
    uf = args.get('uf')
    if uf:
        condicoes.append("uf = ?")
        params.append(uf)

    # Filtro de TIPO DE OPERAÇÃO (Venda / Devolução / Bonificação)
    tipo = args.get('tipo')
    if tipo:
        condicoes.append("tipo_operacao = ?")
        params.append(tipo)

    # Filtro de MARCA
    marca = args.get('marca')
    if marca:
        condicoes.append("marca = ?")
        params.append(marca)

    # Filtro de VENDEDOR
    vendedor = args.get('vendedor')
    if vendedor:
        condicoes.append("vendedor = ?")
        params.append(vendedor)

    where = ("WHERE " + " AND ".join(condicoes)) if condicoes else ""
    return where, params


# ============================================================
#  ROTA: Teste — verifica se a API está funcionando
#  GET /
# ============================================================
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "mensagem": "API Horus funcionando!",
        "rotas": [
            "/api/filtros",
            "/api/kpis",
            "/api/faturamento-mensal",
            "/api/top-vendedores",
            "/api/faturamento-por-marca",
            "/api/faturamento-por-regiao",
            "/api/faturamento-por-unidade",
            "/api/top-produtos",
            "/api/faturamento-por-uf",
        ]
    })


# ============================================================
#  ROTA: Filtros disponíveis — popula os dropdowns do dashboard
#  GET /api/filtros
# ============================================================
@app.route('/api/filtros')
def filtros():
    anos      = consultar("SELECT DISTINCT ano FROM faturamento WHERE ano IS NOT NULL ORDER BY ano DESC")
    meses     = consultar("SELECT DISTINCT mes FROM faturamento WHERE mes IS NOT NULL ORDER BY mes")
    unidades  = consultar("SELECT DISTINCT unidade FROM faturamento WHERE unidade IS NOT NULL ORDER BY unidade")
    ufs       = consultar("SELECT DISTINCT uf FROM faturamento WHERE uf IS NOT NULL AND uf != '' ORDER BY uf")
    marcas    = consultar("SELECT DISTINCT marca FROM faturamento WHERE marca IS NOT NULL ORDER BY marca")
    tipos     = consultar("SELECT DISTINCT tipo_operacao FROM faturamento WHERE tipo_operacao IS NOT NULL ORDER BY tipo_operacao")
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


# ============================================================
#  ROTA: KPIs — cards do topo do dashboard
#  GET /api/kpis?ano=2026&unidade=Reforpan
#  Retorna: faturamento total, devoluções, bonificações, 
#           ticket médio, total de clientes, qtd vendas
# ============================================================
@app.route('/api/kpis')
def kpis():
    where, params = montar_filtros(request.args)

    resultado = consultar(f"""
        SELECT
            ROUND(SUM(CASE WHEN tipo_operacao = 'Venda'        THEN valor_nf ELSE 0 END), 2) AS faturamento,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Devolução'    THEN valor_nf ELSE 0 END), 2) AS devolucoes,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Bonificação'  THEN valor_nf ELSE 0 END), 2) AS bonificacoes,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Venda'        THEN valor_nf ELSE 0 END) /
                  NULLIF(COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END), 0), 2)         AS ticket_medio,
            COUNT(DISTINCT cliente)                                                           AS total_clientes,
            COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END)                              AS qtd_vendas
        FROM faturamento
        {where}
    """, params)

    return jsonify(resultado[0] if resultado else {})


# ============================================================
#  ROTA: Faturamento mensal — gráfico de linha/coluna
#  GET /api/faturamento-mensal?ano=2026&unidade=Reforpan
# ============================================================
@app.route('/api/faturamento-mensal')
def faturamento_mensal():
    where, params = montar_filtros(request.args)

    resultado = consultar(f"""
        SELECT
            ano,
            mes,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END), 2) AS faturamento,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END), 2) AS devolucoes,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END), 2) AS bonificacoes
        FROM faturamento
        {where}
        GROUP BY ano, mes
        ORDER BY ano, mes
    """, params)

    return jsonify(resultado)


# ============================================================
#  ROTA: Top vendedores — ranking
#  GET /api/top-vendedores?ano=2026&limite=10
# ============================================================
@app.route('/api/top-vendedores')
def top_vendedores():
    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))

    resultado = consultar(f"""
        SELECT
            vendedor,
            ROUND(SUM(valor_nf), 2)      AS faturamento,
            COUNT(DISTINCT cliente)       AS clientes,
            COUNT(*)                      AS qtd_vendas
        FROM faturamento
        {where}
        {'AND' if where else 'WHERE'} tipo_operacao = 'Venda'
        GROUP BY vendedor
        ORDER BY faturamento DESC
        LIMIT ?
    """, params + [limite])

    return jsonify(resultado)


# ============================================================
#  ROTA: Faturamento por marca
#  GET /api/faturamento-por-marca?ano=2026
# ============================================================
@app.route('/api/faturamento-por-marca')
def faturamento_por_marca():
    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 15))

    resultado = consultar(f"""
        SELECT
            marca,
            ROUND(SUM(valor_nf), 2) AS faturamento,
            COUNT(DISTINCT cliente)  AS clientes
        FROM faturamento
        {where}
        {'AND' if where else 'WHERE'} tipo_operacao = 'Venda'
        GROUP BY marca
        ORDER BY faturamento DESC
        LIMIT ?
    """, params + [limite])

    return jsonify(resultado)


# ============================================================
#  ROTA: Faturamento por região
#  GET /api/faturamento-por-regiao?ano=2026
# ============================================================
@app.route('/api/faturamento-por-regiao')
def faturamento_por_regiao():
    where, params = montar_filtros(request.args)

    resultado = consultar(f"""
        SELECT
            regiao,
            ROUND(SUM(valor_nf), 2) AS faturamento,
            COUNT(DISTINCT cliente)  AS clientes
        FROM faturamento
        {where}
        {'AND' if where else 'WHERE'} tipo_operacao = 'Venda'
          AND regiao IS NOT NULL
          AND regiao != ''
        GROUP BY regiao
        ORDER BY faturamento DESC
    """, params)

    return jsonify(resultado)


# ============================================================
#  ROTA: Faturamento por unidade
#  GET /api/faturamento-por-unidade?ano=2026
# ============================================================
@app.route('/api/faturamento-por-unidade')
def faturamento_por_unidade():
    where, params = montar_filtros(request.args)

    resultado = consultar(f"""
        SELECT
            unidade,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Venda'       THEN valor_nf ELSE 0 END), 2) AS faturamento,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Devolução'   THEN valor_nf ELSE 0 END), 2) AS devolucoes,
            ROUND(SUM(CASE WHEN tipo_operacao = 'Bonificação' THEN valor_nf ELSE 0 END), 2) AS bonificacoes,
            COUNT(DISTINCT cliente) AS clientes
        FROM faturamento
        {where}
        WHERE unidade IS NOT NULL
        GROUP BY unidade
        ORDER BY faturamento DESC
    """, params)

    return jsonify(resultado)


# ============================================================
#  ROTA: Top produtos
#  GET /api/top-produtos?ano=2026&marca=Salware&limite=10
# ============================================================
@app.route('/api/top-produtos')
def top_produtos():
    where, params = montar_filtros(request.args)
    limite = int(request.args.get('limite', 10))

    resultado = consultar(f"""
        SELECT
            produto,
            marca,
            ROUND(SUM(valor_nf), 2)  AS faturamento,
            ROUND(SUM(quantidade), 0) AS quantidade
        FROM faturamento
        {where}
        {'AND' if where else 'WHERE'} tipo_operacao = 'Venda'
        GROUP BY produto, marca
        ORDER BY faturamento DESC
        LIMIT ?
    """, params + [limite])

    return jsonify(resultado)


# ============================================================
#  ROTA: Faturamento por UF — para mapa
#  GET /api/faturamento-por-uf?ano=2026
# ============================================================
@app.route('/api/faturamento-por-uf')
def faturamento_por_uf():
    where, params = montar_filtros(request.args)

    resultado = consultar(f"""
        SELECT
            uf,
            ROUND(SUM(valor_nf), 2) AS faturamento,
            COUNT(DISTINCT cliente)  AS clientes
        FROM faturamento
        {where}
        {'AND' if where else 'WHERE'} tipo_operacao = 'Venda'
          AND uf IS NOT NULL AND uf != ''
        GROUP BY uf
        ORDER BY faturamento DESC
    """, params)

    return jsonify(resultado)


# ============================================================
#  INICIA O SERVIDOR
# ============================================================
if __name__ == '__main__':
    print("🚀 API Horus iniciando...")
    print(f"📦 Banco de dados: {BANCO_DADOS}")
    print("🌐 Acesse: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)