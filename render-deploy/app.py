from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile

app = Flask(__name__)
CORS(app)

# Configuración
GEMINI_API_KEY = "AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k"
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

# Configurar credenciales al iniciar
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

genai.configure(api_key=GEMINI_API_KEY)
bq_client = bigquery.Client(project=PROJECT_ID)

@app.route('/')
def home():
    return {"status": "Backend funcionando", "endpoints": ["/api/query", "/api/test"]}

@app.route('/api/test', methods=['POST'])
def test():
    try:
        data = request.get_json()
        return jsonify({
            "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
            "chart": {"labels": ["Test1", "Test2"], "values": [10, 20]}
        })
    except Exception as e:
        return jsonify({"text": f"Error: {str(e)}", "chart": None}), 500

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query'].lower()
        
        # Consulta inteligente
        if any(word in user_query for word in ['total', 'count', 'cuántos']):
            sql = f"SELECT COUNT(*) as total FROM `{TABLE_ID}`"
        elif any(word in user_query for word in ['últimos', 'recientes']):
            sql = f"SELECT * FROM `{TABLE_ID}` ORDER BY timestamp DESC LIMIT 10"
        else:
            sql = f"SELECT * FROM `{TABLE_ID}` LIMIT 10"
        
        results = bq_client.query(sql).to_dataframe()
        
        # Gemini
        model = genai.GenerativeModel('gemini-pro')
        data_summary = results.head().to_string() if len(results) > 0 else "No hay datos"
        
        prompt = f"Usuario pregunta: {user_query}\nDatos: {data_summary}\nResponde en español, claro y conciso."
        response = model.generate_content(prompt)
        
        # Gráfico
        chart_data = None
        if len(results) > 0:
            numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
            if len(numeric_cols) > 0:
                chart_data = {
                    "labels": [f"Registro {i+1}" for i in range(min(10, len(results)))],
                    "values": results[numeric_cols[0]].head(10).tolist()
                }
        
        return jsonify({
            "text": response.text,
            "chart": chart_data
        })
        
    except Exception as e:
        return jsonify({"text": f"Error: {str(e)}", "chart": None}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)