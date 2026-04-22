from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import time
import json
from datetime import date as _date, datetime

app = Flask(__name__)
CORS(app)

# Serializa objetos date/datetime do Python para string ISO (evita erro ao retornar datas do PostgreSQL)
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, _date)):
            return obj.isoformat()
        return super().default(obj)

app.json_encoder = DateEncoder

# ============================================================
#  CACHE SIMPLES EM MEMÓRIA
#  Guarda resultados por 8 horas para não bater no banco
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
#  NORMALIZA DATA — converte qualquer formato de semana
#  para YYYY-MM-DD, que é o que o banco espera.
#  Aceita:
#    "Mon, 20 Apr 2026 00:00:00 GMT"  (JS Date.toString / toUTCString)
#    "2026-04-20T00:00:00.000Z"       (JS toISOString)
#    "2026-04-20"                     (já correto)
# ============================================================
def normalizar_semana(semana_str):
    if not semana_str:
        return None
    s = str(semana_str).strip()
    # Se começa com YYYY- (4 digitos + hifen), pega os primeiros 10 chars
    if len(s) >= 10 and s[4] == '-':
        return s[:10]
    # Fallback para formato GMT: "Mon, 20 Apr 2026 00:00:00 GMT"
    from datetime import datetime
    try:
        return datetime.strptime(s, '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d')
    except Exception:
        pass
    try:
        return datetime.strptime(s, '%a, %d %b %Y %H:%M:%S').strftime('%Y-%m-%d')
    except Exception:
        pass
    return s[:10] if len(s) >= 10 else s

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
                                    ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Devolucao'   THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS devolucoes,
                                                ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Bonificacao' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS bonificacoes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda' THEN valor_nf ELSE 0 END) /
                  NULLIF(COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END), 0) AS NUMERIC), 2) AS ticket_medio,
            COUNT(DISTINCT cliente)                              AS total_clientes,
            COUNT(CASE WHEN tipo_operacao = 'Venda' THEN 1 END) AS qtd_vendas
        FROM faturamento {where}
    """, params)
    r = resultado[0] if resultado else {}
    cache_set(key, r)
    return jsonify(r)

@app.route('/api/shelflife/atualizar', methods=['POST'])
def shelflife_atualizar():
    data    = request.get_json()
    id_prod = data.get('id')
    conn    = get_conn()
    cursor  = conn.cursor()

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

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'ok': True})
