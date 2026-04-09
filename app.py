from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import time

app = Flask(__name__)
CORS(app)

# Configurações de Banco de Dados via Variáveis de Ambiente
def get_conn():
    return psycopg2.connect(
        host     = os.environ.get('DB_HOST'),
        port     = int(os.environ.get('DB_PORT', 6543)),
        database = os.environ.get('DB_NAME', 'postgres'),
        user     = os.environ.get('DB_USER'),
        password = os.environ.get('DB_PASS'),
        sslmode  = 'require'
    )

@app.route('/api/faturamento', methods=['GET'])
def get_faturamento():
    # Lógica de filtros conforme o seu original
    vendedor = request.args.get('vendedor')
    params = []
    where = ""
    
    if vendedor:
        where = "WHERE vendedor = %s"
        params.append(vendedor)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    query = f"""
        SELECT 
            cliente, cod_cliente, vendedor, ano, mes,
            ROUND(CAST(SUM(CASE WHEN tipo_operacao = 'Venda' THEN valor_nf ELSE 0 END) AS NUMERIC), 2) AS faturamento
        FROM faturamento
        {where}
        GROUP BY cliente, cod_cliente, vendedor, ano, mes
        ORDER BY cliente, ano, mes
    """
    
    cur.execute(query, params)
    resultado = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(resultado)

if __name__ == '__main__':
    app.run(debug=True)
